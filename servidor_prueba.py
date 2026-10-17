"""Servidor local secuencial de pruebas; cuenta filas y no guarda el dataset."""

import argparse
import socket

from transmision import TIMEOUT, VERSION, codificar, recibir, validar_destino


def procesar_conexion(conexion: socket.socket) -> int:
    """Validar la secuencia JSON Lines y confirmar solo un envío completo."""
    conexion.settimeout(TIMEOUT)
    with conexion.makefile("rb") as archivo:
        inicio = recibir(archivo)
        if inicio == {"tipo": "ping", "version": VERSION}:
            conexion.sendall(codificar({"tipo": "pong", "version": VERSION}))
            return 0
        columnas = inicio.get("columnas")
        total = inicio.get("registros")
        if (inicio.get("tipo") != "inicio" or inicio.get("version") != VERSION or
                not isinstance(columnas, list) or not columnas or
                not all(isinstance(c, str) for c in columnas) or
                len(set(columnas)) != len(columnas) or type(total) is not int or total < 0):
            raise ValueError("Cabecera de inicio inválida.")
        recibidos = 0
        while True:
            mensaje = recibir(archivo)
            if mensaje.get("tipo") == "fin":
                if type(mensaje.get("registros")) is not int or mensaje["registros"] != recibidos or recibidos != total:
                    raise ValueError(f"Conteos inconsistentes: esperados={total}, recibidos={recibidos}.")
                conexion.sendall(codificar({"tipo": "confirmacion", "registros": recibidos}))
                return recibidos
            valores = mensaje.get("datos")
            if (mensaje.get("tipo") != "registro" or not isinstance(valores, list) or
                    len(valores) != len(columnas) or recibidos >= total or
                    any(isinstance(v, (list, dict)) for v in valores)):
                raise ValueError("Registro inválido o mensaje fuera de secuencia.")
            recibidos += 1


def servir(host: str = "127.0.0.1", puerto: int = 5000) -> None:
    """Atender conexiones sucesivas hasta Ctrl+C, sin persistir datos."""
    validar_destino(host, puerto)
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as servidor:
        servidor.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        servidor.bind((host, puerto))
        servidor.listen(5)
        print(f"Servidor de prueba escuchando en {host}:{puerto}", flush=True)
        while True:
            conexion, direccion = servidor.accept()
            with conexion:
                try:
                    recibidos = procesar_conexion(conexion)
                    print(f"{direccion}: operación completa; registros recibidos={recibidos}", flush=True)
                except (OSError, ValueError, ConnectionError, UnicodeError) as error:
                    print(f"{direccion}: error: {error}", flush=True)
                    try:
                        conexion.sendall(codificar({"tipo": "error", "mensaje": str(error)[:1000]}))
                    except OSError:
                        pass


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--puerto", type=int, default=5000)
    opciones = parser.parse_args()
    try:
        servir(opciones.host, opciones.puerto)
    except KeyboardInterrupt:
        print("\nServidor detenido.")
