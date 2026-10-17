"""Cliente TCP y codificación del protocolo de ingesta del Grupo 4."""

import json
import math
import socket
from dataclasses import dataclass
from typing import Callable

import pandas as pd

PUERTO = 12345
TIMEOUT = 120
MAX_RESPUESTA = 65_536
TAMANO_LOTE = 65_536
MARCA_FIN = b"<EOF>"


def recibir(conexion) -> dict:
    """Recomponer la confirmaci?n UTF-8 hasta el cierre remoto, con l?mite."""
    datos = bytearray()
    while True:
        bloque = conexion.recv(min(4096, MAX_RESPUESTA + 1 - len(datos)))
        if not bloque:
            break
        datos.extend(bloque)
        if len(datos) > MAX_RESPUESTA:
            raise ValueError("Confirmaci?n superior al l?mite de 64 KiB.")
    if not datos:
        raise ConnectionError("El servidor cerr? sin confirmar la transferencia.")
    def rechazar_constante(valor):
        raise ValueError(f"Constante JSON no v?lida: {valor}")
    respuesta = json.loads(datos.decode("utf-8"), parse_constant=rechazar_constante)
    if not isinstance(respuesta, dict):
        raise ValueError("La confirmaci?n debe ser un objeto JSON.")
    return respuesta


def validar_destino(host: str, puerto: int) -> None:
    """Validar parámetros antes de abrir una conexión."""
    if not host.strip() or type(puerto) is not int or not 1 <= puerto <= 65535:
        raise ValueError("Indique una IP/host y un puerto entre 1 y 65535.")


def probar_conexion(host: str, puerto: int, timeout: float = TIMEOUT) -> None:
    """Comprobar solo acceso TCP, sin ping ni transferencia vac?a."""
    validar_destino(host, puerto)
    with socket.create_connection((host, puerto), timeout=timeout):
        pass


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
            conexion.sendall(b"[")
            lote = bytearray()
            pendientes = 0
            for fila in datos.itertuples(index=False, name=None):
                registro = dict(zip(columnas, (valor_json(v) for v in fila)))
                linea = (("," if enviados + pendientes else "") + json.dumps(
                    registro, ensure_ascii=False, allow_nan=False,
                    separators=(",", ":"))).encode("utf-8")
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
            conexion.sendall(b"]" + MARCA_FIN)
            conexion.shutdown(socket.SHUT_WR)
            respuesta = recibir(conexion)
            if respuesta.get("ok") is not True:
                raise ValueError(f"El servidor rechaz? el env?o: {respuesta.get('detalle', respuesta)}")
            if type(respuesta.get("filas")) is not int or respuesta["filas"] != total:
                raise ValueError(f"Confirmaci?n inv?lida del servidor: {respuesta}")
        return ResultadoEnvio(enviados, total)
    except Exception as error:
        raise ErrorTransmision(str(error), enviados) from error
