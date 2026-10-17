"""Pruebas TCP locales, sin GUI ni persistencia de registros recibidos."""

import hashlib
import io
import socket
from queue import Queue
from threading import Thread
import unittest
from unittest.mock import Mock, patch

import pandas as pd

from cliente import RUTA_DATASET, leer_dataset
from servidor_prueba import procesar_conexion
from transmision import (ErrorTransmision, MAX_LINEA, codificar, enviar_datos,
                         probar_conexion, recibir, ResultadoEnvio)
from interfaz_cliente import InterfazCliente


class ServidorUnaConexion:
    """Servidor efímero que expone el conteo o error observado por el receptor."""

    def __init__(self, procesar=procesar_conexion):
        self.servidor = socket.socket()
        self.servidor.bind(("127.0.0.1", 0))
        self.servidor.listen(1)
        self.servidor.settimeout(10)
        self.puerto = self.servidor.getsockname()[1]
        self.resultado = None
        self.error = None

        def ejecutar():
            try:
                with self.servidor:
                    conexion, _ = self.servidor.accept()
                    with conexion:
                        self.resultado = procesar(conexion)
            except Exception as error:
                self.error = error

        self.hilo = Thread(target=ejecutar, daemon=True)
        self.hilo.start()

    def esperar(self):
        self.hilo.join(60)
        if self.hilo.is_alive():
            raise AssertionError("El servidor no terminó dentro del tiempo esperado")


class PruebasTCP(unittest.TestCase):
    def test_01_muestra_y_dataset_completo(self):
        huella = hashlib.sha256(RUTA_DATASET.read_bytes()).hexdigest()
        datos = leer_dataset(RUTA_DATASET)
        for cantidad in (50, 569_250):
            with self.subTest(registros=cantidad):
                servidor = ServidorUnaConexion()
                progreso = []
                resultado = enviar_datos(datos.head(cantidad), "127.0.0.1", servidor.puerto,
                                         lambda enviados, total: progreso.append(enviados))
                servidor.esperar()
                self.assertIsNone(servidor.error)
                self.assertEqual((resultado.enviados, resultado.confirmados, servidor.resultado),
                                 (cantidad, cantidad, cantidad))
                self.assertEqual(progreso[-1], cantidad)
                print(f"\nTCP: enviados={resultado.enviados}, confirmados={resultado.confirmados}, recibidos={servidor.resultado}", flush=True)
        self.assertEqual(hashlib.sha256(RUTA_DATASET.read_bytes()).hexdigest(), huella)

    def test_02_fragmentos_utf8_y_controles(self):
        servidor = ServidorUnaConexion()
        mensajes = [
            {"tipo": "inicio", "version": 1, "columnas": ["tipo"], "registros": 1},
            {"tipo": "registro", "datos": ["fin: río Ñuble\nsegunda línea"]},
            {"tipo": "fin", "registros": 1},
        ]
        with socket.create_connection(("127.0.0.1", servidor.puerto), timeout=5) as conexion:
            for mensaje in mensajes:
                for byte in codificar(mensaje):
                    conexion.sendall(bytes([byte]))
            with conexion.makefile("rb") as archivo:
                self.assertEqual(recibir(archivo), {"tipo": "confirmacion", "registros": 1})
        servidor.esperar()
        self.assertIsNone(servidor.error)
        self.assertEqual(servidor.resultado, 1)

    def test_03_seleccion_nulos_y_ping(self):
        servidor = ServidorUnaConexion()
        probar_conexion("127.0.0.1", servidor.puerto)
        servidor.esperar()
        self.assertIsNone(servidor.error)
        def inspeccionar(conexion):
            with conexion.makefile("rb") as archivo:
                self.assertEqual(recibir(archivo)["columnas"], ["caudal"])
                self.assertEqual(recibir(archivo)["datos"], [None])
                self.assertEqual(recibir(archivo)["tipo"], "fin")
                conexion.sendall(codificar({"tipo": "confirmacion", "registros": 1}))
        datos = pd.DataFrame({"fecha": ["2015"], "caudal": [float("nan")]})
        servidor = ServidorUnaConexion(inspeccionar)
        enviar_datos(datos.loc[:, ["caudal"]], "127.0.0.1", servidor.puerto)
        servidor.esperar()
        self.assertIsNone(servidor.error)

    def test_04_confirmacion_incorrecta_y_desconexion(self):
        def respuesta_incorrecta(conexion):
            with conexion.makefile("rb") as archivo:
                while recibir(archivo)["tipo"] != "fin":
                    pass
                conexion.sendall(codificar({"tipo": "confirmacion", "registros": 999}))
        for manejador in (respuesta_incorrecta, lambda conexion: None):
            servidor = ServidorUnaConexion(manejador)
            with self.assertRaises(ErrorTransmision) as contexto:
                enviar_datos(pd.DataFrame({"a": [1]}), "127.0.0.1", servidor.puerto)
            self.assertEqual(contexto.exception.confirmados, 0)
            servidor.esperar()

    def test_05_limites_y_seleccion_vacia(self):
        for contenido in (b'{"tipo":"fin"}', b'x' * (MAX_LINEA + 1), b'NaN\n', b'{}\xff\n'):
            with self.assertRaises((ValueError, UnicodeError)):
                recibir(io.BytesIO(contenido))
        with self.assertRaises(ValueError):
            codificar({"datos": "x" * MAX_LINEA})
        with self.assertRaises(ErrorTransmision):
            enviar_datos(pd.DataFrame(index=range(3)), "127.0.0.1", 5000)

    def test_06_conteo_incorrecto_y_timeout(self):
        servidor = ServidorUnaConexion()
        with socket.create_connection(("127.0.0.1", servidor.puerto), timeout=5) as conexion:
            conexion.sendall(codificar({"tipo": "inicio", "version": 1, "columnas": ["a"], "registros": 1})
                               + codificar({"tipo": "fin", "registros": 0}))
        servidor.esperar()
        self.assertIsInstance(servidor.error, ValueError)
        def no_confirmar(conexion):
            with conexion.makefile("rb") as archivo:
                while recibir(archivo)["tipo"] != "fin":
                    pass
                conexion.settimeout(3)
                conexion.recv(1)
        servidor = ServidorUnaConexion(no_confirmar)
        with self.assertRaises(ErrorTransmision) as contexto:
            enviar_datos(pd.DataFrame({"a": [1]}), "127.0.0.1", servidor.puerto, timeout=0.2)
        self.assertEqual(contexto.exception.enviados, 1)
        self.assertEqual(contexto.exception.confirmados, 0)
        servidor.esperar()


class PruebasIntegracionInterfaz(unittest.TestCase):
    """Ejercitar acciones y cola de la GUI sin abrir una ventana de escritorio."""

    def test_envio_y_confirmacion_por_cola(self):
        app = object.__new__(InterfazCliente)
        app.cargando = False
        app.operando_red = False
        app.datos_originales = pd.DataFrame({"fecha": ["2015", "2016"], "caudal": [1, 2]})
        app.seleccion_columnas = {"fecha": Mock(), "caudal": Mock()}
        app.seleccion_columnas["fecha"].get.return_value = False
        app.seleccion_columnas["caudal"].get.return_value = True
        for nombre in ("boton", "boton_enviar", "boton_probar", "estado", "progreso_red", "host", "puerto"):
            setattr(app, nombre, Mock())
        app.host.get.return_value = "127.0.0.1"
        app.puerto.get.return_value = "5000"
        app.barra_progreso = {}
        app.eventos_red = Queue()
        def enviar(copia, host, puerto, progreso):
            self.assertEqual(list(copia.columns), ["caudal"])
            self.assertEqual(len(copia), 2)
            progreso(2, 2)
            return ResultadoEnvio(2, 2)
        # Ejecutar el trabajador de forma determinista sin tocar controles en él.
        with patch("interfaz_cliente.enviar_datos", side_effect=enviar), patch("interfaz_cliente.Thread") as hilo:
            app.iniciar_red(True)
            self.assertTrue(app.operando_red)
            hilo.call_args.kwargs["target"]()
        app.revisar_eventos_red()
        self.assertFalse(app.operando_red)
        self.assertEqual(app.barra_progreso["value"], 100)
        self.assertIn("Confirmados: 2", app.progreso_red.set.call_args.args[0])
        self.assertEqual(app.datos_originales.shape, (2, 2))
        app.seleccion_columnas["caudal"].get.return_value = False
        app.actualizar_botones_red()
        app.boton_enviar.state.assert_called_with(["disabled"])
        with patch("interfaz_cliente.Thread") as hilo:
            app.iniciar_red(True)
            hilo.assert_not_called()
        app.operando_red = True
        app.eventos_red.put(("error", ErrorTransmision("sin confirmación", 2)))
        app.revisar_eventos_red()
        self.assertFalse(app.operando_red)
        self.assertIn("Confirmados: 0", app.progreso_red.set.call_args.args[0])


if __name__ == "__main__":
    unittest.main(verbosity=2)
