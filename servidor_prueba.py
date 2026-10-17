"""Servidor local secuencial de pruebas; cuenta filas y no guarda el dataset."""

import argparse
import socket

import json

from transmision import TIMEOUT, PUERTO, MARCA_FIN, validar_destino

MAX_BYTES = 512 * 1024 * 1024


def procesar_conexion(conexion: socket.socket) -> int:
    """Receptor local del contrato Grupo 4; sin persistencia ni dashboard."""
    conexion.settimeout(TIMEOUT)
    datos = bytearray()
    while True:
        bloque = conexion.recv(1024 * 1024)
        if not bloque:
            break
        datos.extend(bloque)
        if len(datos) > MAX_BYTES:
            raise ValueError("Transferencia superior a 512 MiB.")
    if not datos.endswith(MARCA_FIN):
        raise ValueError("Falta el sufijo <EOF>.")
    del datos[-len(MARCA_FIN):]
    def rechazar(valor):
        raise ValueError(f"Constante JSON inv?lida: {valor}")
    registros = json.loads(datos.decode("utf-8"), parse_constant=rechazar)
    if not isinstance(registros, list) or not all(isinstance(r, dict) for r in registros):
        raise ValueError("Se requiere un arreglo JSON de objetos.")
    conexion.sendall(json.dumps({"ok": True, "filas": len(registros)}).encode("utf-8"))
    return len(registros)


def servir(host: str = "127.0.0.1", puerto: int = PUERTO) -> None:
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
                        conexion.sendall(json.dumps({"ok": False, "filas": 0, "detalle": str(error)[:1000]}).encode("utf-8"))
                    except OSError:
                        pass


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--puerto", type=int, default=PUERTO)
    opciones = parser.parse_args()
    try:
        servir(opciones.host, opciones.puerto)
    except KeyboardInterrupt:
        print("\nServidor detenido.")
