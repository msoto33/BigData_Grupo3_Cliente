"""Cliente TCP y codificación del protocolo provisional CAMELS-CL v1."""

import json
import math
import socket
from dataclasses import dataclass
from typing import Callable

import pandas as pd

VERSION = 1
TIMEOUT = 30
MAX_LINEA = 1_048_576  # Bytes, incluido el salto de línea.
TAMANO_LOTE = 65_536


def codificar(mensaje: dict) -> bytes:
    """Codificar una única línea JSON estricta, con un límite explícito."""
    linea = (json.dumps(mensaje, ensure_ascii=False, allow_nan=False,
                        separators=(",", ":")) + "\n").encode("utf-8")
    if len(linea) > MAX_LINEA:
        raise ValueError("Mensaje superior al límite de 1 MiB.")
    return linea


def recibir(archivo) -> dict:
    """Leer una línea completa; el búfer recompone fragmentos TCP."""
    linea = archivo.readline(MAX_LINEA + 1)
    if not linea:
        raise ConnectionError("Desconexión antes de completar el protocolo.")
    if len(linea) > MAX_LINEA or not linea.endswith(b"\n"):
        raise ValueError("Línea incompleta o superior al límite de 1 MiB.")
    def rechazar_constante(valor):
        raise ValueError(f"Constante JSON no válida: {valor}")
    mensaje = json.loads(linea.decode("utf-8"), parse_constant=rechazar_constante)
    if not isinstance(mensaje, dict):
        raise ValueError("Cada mensaje debe ser un objeto JSON.")
    return mensaje


def validar_destino(host: str, puerto: int) -> None:
    """Validar parámetros antes de abrir una conexión."""
    if not host.strip() or type(puerto) is not int or not 1 <= puerto <= 65535:
        raise ValueError("Indique una IP/host y un puerto entre 1 y 65535.")


def probar_conexion(host: str, puerto: int, timeout: float = TIMEOUT) -> None:
    """Comprobar TCP y compatibilidad del protocolo mediante ping/pong."""
    validar_destino(host, puerto)
    with socket.create_connection((host, puerto), timeout=timeout) as conexion:
        conexion.sendall(codificar({"tipo": "ping", "version": VERSION}))
        with conexion.makefile("rb") as archivo:
            respuesta = recibir(archivo)
        if respuesta != {"tipo": "pong", "version": VERSION}:
            raise ValueError("El servidor no confirmó el protocolo provisional.")


@dataclass(frozen=True)
class ResultadoEnvio:
    enviados: int
    confirmados: int


class ErrorTransmision(Exception):
    """Fallo que conserva el número de registros entregados al socket local."""

    def __init__(self, mensaje: str, enviados: int):
        super().__init__(mensaje)
        self.enviados = enviados
        self.confirmados = 0


def valor_json(valor):
    """Representar nulos/no finitos como null y conservar escalares del CSV."""
    if pd.isna(valor):
        return None
    if hasattr(valor, "item"):
        valor = valor.item()
    if isinstance(valor, float) and not math.isfinite(valor):
        return None
    return valor


def enviar_datos(datos: pd.DataFrame, host: str, puerto: int,
                 progreso: Callable[[int, int], None] | None = None,
                 timeout: float = TIMEOUT) -> ResultadoEnvio:
    """Transmitir por lotes acotados y aceptar éxito solo tras confirmar el total.

    El callback se ejecuta en el hilo llamante, nunca debe tocar Tkinter.
    'Enviados' significa sendall completado, no recepción remota confirmada.
    """
    enviados = 0
    try:
        validar_destino(host, puerto)
        columnas = list(datos.columns)
        if not columnas or not all(isinstance(c, str) for c in columnas) or len(set(columnas)) != len(columnas):
            raise ValueError("Se requiere al menos una columna con nombre único de texto.")
        total = len(datos)
        with socket.create_connection((host, puerto), timeout=timeout) as conexion:
            conexion.sendall(codificar({"tipo": "inicio", "version": VERSION,
                                       "columnas": columnas, "registros": total}))
            lote = bytearray()
            pendientes = 0
            for fila in datos.itertuples(index=False, name=None):
                linea = codificar({"tipo": "registro", "datos": [valor_json(v) for v in fila]})
                if lote and len(lote) + len(linea) > TAMANO_LOTE:
                    conexion.sendall(lote)
                    enviados += pendientes
                    if progreso:
                        progreso(enviados, total)
                    lote.clear()
                    pendientes = 0
                lote.extend(linea)
                pendientes += 1
            if lote:
                conexion.sendall(lote)
                enviados += pendientes
                if progreso:
                    progreso(enviados, total)
            conexion.sendall(codificar({"tipo": "fin", "registros": enviados}))
            with conexion.makefile("rb") as archivo:
                respuesta = recibir(archivo)
            if (respuesta.get("tipo") != "confirmacion" or
                    type(respuesta.get("registros")) is not int or
                    respuesta["registros"] != total):
                raise ValueError(f"Confirmación inválida del servidor: {respuesta}")
        return ResultadoEnvio(enviados, total)
    except Exception as error:
        raise ErrorTransmision(str(error), enviados) from error
