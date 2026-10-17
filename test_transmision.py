"""Pruebas TCP locales, sin GUI ni persistencia de registros recibidos."""

import hashlib
import json
import time
import importlib.util
from pathlib import Path
import socket
from queue import Queue
from threading import Thread
import unittest
from unittest.mock import Mock, patch

import pandas as pd

from cliente import RUTA_DATASET, leer_dataset
from servidor_prueba import procesar_conexion
from transmision import (ErrorTransmision, MAX_RESPUESTA, enviar_datos,
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
    def test_dataset_y_muestra_con_servidor_grupo4(self):
        ruta = Path(r"C:\Users\msoto\Desktop\bigdata_evaluacion2\servidor_prueba.py")
        if not ruta.is_file():
            self.skipTest("Servidor de referencia Grupo 4 no disponible")
        spec = importlib.util.spec_from_file_location("grupo4_referencia", ruta)
        modulo = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(modulo)
        huella = hashlib.sha256(RUTA_DATASET.read_bytes()).hexdigest()
        datos = leer_dataset(RUTA_DATASET)
        for cantidad in (50, len(datos)):
            def atender(conexion):
                return modulo.atender(conexion, registrar=lambda mensaje: None)
            servidor = ServidorUnaConexion(atender)
            progreso = []
            resultado = enviar_datos(datos.head(cantidad), "127.0.0.1", servidor.puerto,
                                     lambda n, total: progreso.append(n))
            servidor.esperar()
            self.assertIsNone(servidor.error)
            self.assertEqual(resultado, ResultadoEnvio(cantidad, cantidad))
            self.assertEqual(servidor.resultado["filas"], cantidad)
            self.assertEqual(progreso[-1], cantidad)
            print(f"TCP Grupo 4: {cantidad} filas confirmadas", flush=True)
        self.assertEqual(hashlib.sha256(RUTA_DATASET.read_bytes()).hexdigest(), huella)

    def test_seleccion_nulos_unicode_y_lotes(self):
        def inspeccionar(conexion):
            datos = bytearray()
            while bloque := conexion.recv(17):
                datos.extend(bloque)
            self.assertTrue(datos.endswith(b"]<EOF>"))
            registros = json.loads(datos[:-5])
            self.assertEqual(registros, [{"caudal": None, "texto": "r?o <EOF>\n?uble"}] * 3000)
            for byte in b'{"ok":true,"filas":3000}':
                conexion.sendall(bytes([byte]))
        servidor = ServidorUnaConexion(inspeccionar)
        datos = pd.DataFrame({"caudal": [float("nan")] * 3000, "texto": ["r?o <EOF>\n?uble"] * 3000})
        progreso = []
        enviar_datos(datos, "127.0.0.1", servidor.puerto, lambda n, total: progreso.append(n))
        servidor.esperar()
        self.assertIsNone(servidor.error)
        self.assertGreater(len(progreso), 1)

    def test_prueba_tcp_no_envia_bytes(self):
        servidor = ServidorUnaConexion(lambda conexion: conexion.recv(100))
        probar_conexion("127.0.0.1", servidor.puerto)
        servidor.esperar()
        self.assertEqual(servidor.resultado, b"")

    def test_confirmaciones_invalidas(self):
        respuestas = [b'', b'{}', b'{"ok":false,"filas":0,"detalle":"ocupado"}',
                      b'{"ok":true,"filas":999}', b'{"ok":true,"filas":true}',
                      b'{"ok":1,"filas":1}', b'[]', b'NaN', b'{}x', b'\xff', b'x' * (MAX_RESPUESTA + 1)]
        for respuesta in respuestas:
            with self.subTest(respuesta=respuesta[:100]):
                def atender(conexion):
                    while conexion.recv(4096):
                        pass
                    conexion.sendall(respuesta)
                servidor = ServidorUnaConexion(atender)
                with self.assertRaises(ErrorTransmision) as error:
                    enviar_datos(pd.DataFrame({"a": [1]}), "127.0.0.1", servidor.puerto)
                self.assertEqual(error.exception.confirmados, 0)
                self.assertEqual(error.exception.enviados, 1)
                servidor.esperar()

    def test_timeout_y_seleccion_invalida(self):
        def esperar(conexion):
            while conexion.recv(4096):
                pass
            time.sleep(0.3)
        servidor = ServidorUnaConexion(esperar)
        with self.assertRaises(ErrorTransmision):
            enviar_datos(pd.DataFrame({"a": [1]}), "127.0.0.1", servidor.puerto, timeout=0.05)
        servidor.esperar()
        for datos in (pd.DataFrame(index=range(3)), pd.DataFrame([[1, 2]], columns=["a", "a"])):
            with self.assertRaises(ErrorTransmision):
                enviar_datos(datos, "127.0.0.1", 12345)

    def test_receptor_local(self):
        servidor = ServidorUnaConexion()
        self.assertEqual(enviar_datos(pd.DataFrame({"a": [1]}), "127.0.0.1", servidor.puerto), ResultadoEnvio(1, 1))
        servidor.esperar()
        self.assertIsNone(servidor.error)


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
        app.puerto.get.return_value = "12345"
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
