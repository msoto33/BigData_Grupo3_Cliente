"""Consulta y transmisión TCP de CAMELS-CL sin modificar el dataset original."""

from dataclasses import dataclass
from pathlib import Path
from queue import Empty, Queue
from threading import Thread
import tkinter as tk
from tkinter import filedialog, ttk

import pandas as pd

from cliente import RUTA_DATASET, leer_dataset
from transmision import ErrorTransmision, enviar_datos, probar_conexion, validar_destino


COLUMNAS_CAMELS = (
    "fecha", "estacion_id", "estacion_nombre", "latitud", "longitud",
    "zona_latitudinal", "area_cuenca_km2", "caudal_m3_s",
)
LIMITE_VISTA = 100


@dataclass
class ResultadoCarga:
    """Resultado de lectura transferido al hilo principal mediante una cola."""

    filas: int = 0
    columnas: tuple[str, ...] = ()
    registros: tuple[tuple[str, ...], ...] = ()
    error: str = ""
    datos: pd.DataFrame | None = None


def cargar_en_segundo_plano(ruta: Path, resultados: Queue) -> None:
    """Leer el archivo y enviar su resumen sin acceder a ningún control Tkinter."""
    try:
        datos = leer_dataset(ruta)
        if set(datos.columns) != set(COLUMNAS_CAMELS):
            raise ValueError(
                "El CSV debe contener las ocho columnas CAMELS-CL: "
                + ", ".join(COLUMNAS_CAMELS)
            )
        # Convertir únicamente la muestra; conservar también el DataFrame completo.
        muestra = datos.head(LIMITE_VISTA)
        registros = tuple(
            tuple("" if pd.isna(valor) else str(valor) for valor in fila)
            for fila in muestra.itertuples(index=False, name=None)
        )
        resultado = ResultadoCarga(len(datos), tuple(datos.columns), registros, datos=datos)
    except pd.errors.EmptyDataError:
        resultado = ResultadoCarga(error="El archivo está vacío o no contiene columnas.")
    except Exception as error:
        # Garantizar que también los errores inesperados lleguen al hilo principal.
        resultado = ResultadoCarga(error=f"{type(error).__name__}: {error}")
    resultados.put(resultado)


class InterfazCliente:
    """Ventana principal con selección, resumen y vista limitada del CSV."""

    def __init__(self, ventana: tk.Tk) -> None:
        self.ventana = ventana
        self.resultados = Queue()
        self.cargando = False
        self.operando_red = False
        self.eventos_red = Queue()
        self.datos_originales: pd.DataFrame | None = None
        self.seleccion_columnas: dict[str, tk.BooleanVar] = {}
        ventana.title("Big Data – Grupo 3 | Cliente")
        ventana.geometry("1150x600")
        ventana.minsize(700, 400)
        ventana.columnconfigure(0, weight=1)
        ventana.rowconfigure(0, weight=1)

        contenido = ttk.Frame(ventana, padding=12)
        contenido.grid(row=0, column=0, sticky="nsew")
        contenido.columnconfigure(0, weight=1)
        contenido.rowconfigure(7, weight=1)

        self.boton = ttk.Button(contenido, text="Seleccionar archivo CSV", command=self.seleccionar_archivo)
        self.boton.grid(row=0, column=0, sticky="w", pady=(0, 10))
        self.nombre = tk.StringVar(value="Archivo: sin seleccionar")
        self.ruta = tk.StringVar(value="Ruta: sin seleccionar")
        self.resumen = tk.StringVar(value="Registros: — | Columnas: —")
        self.estado = tk.StringVar(value="Seleccione un archivo CSV para comenzar.")
        ttk.Label(contenido, textvariable=self.nombre).grid(row=1, column=0, sticky="w")
        ttk.Label(contenido, textvariable=self.ruta, wraplength=1000).grid(row=2, column=0, sticky="w")
        ttk.Label(contenido, textvariable=self.resumen).grid(row=3, column=0, sticky="w", pady=8)
        ttk.Label(contenido, text="Vista previa: primeros 100 registros como máximo.").grid(row=4, column=0, sticky="w")

        seleccion = ttk.LabelFrame(contenido, text="Columnas disponibles para transmisión", padding=8)
        seleccion.grid(row=5, column=0, sticky="ew", pady=6)
        self.marco_columnas = ttk.Frame(seleccion)
        self.marco_columnas.pack(fill="x")
        acciones = ttk.Frame(seleccion)
        acciones.pack(fill="x", pady=(8, 0))
        ttk.Button(acciones, text="Seleccionar todas", command=lambda: self.seleccionar_todas(True)).pack(side="left")
        ttk.Button(acciones, text="Deseleccionar todas", command=lambda: self.seleccionar_todas(False)).pack(side="left", padx=8)
        self.contador = tk.StringVar(value="Columnas seleccionadas: 0 de 0")
        ttk.Label(acciones, textvariable=self.contador).pack(side="left")

        red = ttk.LabelFrame(contenido, text="Servidor TCP (protocolo provisional)", padding=8)
        red.grid(row=6, column=0, sticky="ew", pady=6)
        self.host = tk.StringVar(value="127.0.0.1")
        self.puerto = tk.StringVar(value="5000")
        ttk.Label(red, text="IP/host:").grid(row=0, column=0)
        ttk.Entry(red, textvariable=self.host, width=20).grid(row=0, column=1, padx=6)
        ttk.Label(red, text="Puerto:").grid(row=0, column=2)
        ttk.Entry(red, textvariable=self.puerto, width=8).grid(row=0, column=3, padx=6)
        self.boton_probar = ttk.Button(red, text="Probar conexión", command=lambda: self.iniciar_red(False))
        self.boton_probar.grid(row=0, column=4, padx=6)
        self.boton_enviar = ttk.Button(red, text="Enviar datos", command=lambda: self.iniciar_red(True))
        self.boton_enviar.grid(row=0, column=5, padx=6)
        self.progreso_red = tk.StringVar(value="Enviados: 0 | Confirmados: 0")
        ttk.Label(red, textvariable=self.progreso_red).grid(row=1, column=0, columnspan=6, sticky="w")
        self.barra_progreso = ttk.Progressbar(red, maximum=100)
        self.barra_progreso.grid(row=2, column=0, columnspan=6, sticky="ew")
        self.actualizar_botones_red()

        marco_tabla = ttk.Frame(contenido)
        marco_tabla.grid(row=7, column=0, sticky="nsew", pady=(6, 0))
        marco_tabla.columnconfigure(0, weight=1)
        marco_tabla.rowconfigure(0, weight=1)
        self.tabla = ttk.Treeview(marco_tabla, columns=COLUMNAS_CAMELS, show="headings")
        self.tabla.grid(row=0, column=0, sticky="nsew")
        vertical = ttk.Scrollbar(marco_tabla, orient="vertical", command=self.tabla.yview)
        horizontal = ttk.Scrollbar(marco_tabla, orient="horizontal", command=self.tabla.xview)
        vertical.grid(row=0, column=1, sticky="ns")
        horizontal.grid(row=1, column=0, sticky="ew")
        self.tabla.configure(yscrollcommand=vertical.set, xscrollcommand=horizontal.set)
        self.configurar_columnas(COLUMNAS_CAMELS)

        ttk.Label(ventana, textvariable=self.estado, relief="sunken", anchor="w", padding=6,
                  wraplength=1000).grid(row=1, column=0, sticky="ew")
        # Este sondeo se inicia y se renueva exclusivamente desde el hilo principal.
        ventana.after(100, self.revisar_resultados)

    def configurar_columnas(self, columnas: tuple[str, ...]) -> None:
        """Mantener nombres y orden originales en las cabeceras."""
        self.tabla.configure(columns=columnas)
        for columna in columnas:
            self.tabla.heading(columna, text=columna)
            self.tabla.column(columna, width=max(130, len(columna) * 10), minwidth=100)

    def crear_selector_columnas(self) -> None:
        """Crear casillas en el orden del CSV, seleccionadas inicialmente."""
        for control in self.marco_columnas.winfo_children():
            control.destroy()
        self.seleccion_columnas.clear()
        if self.datos_originales is not None:
            for indice, columna in enumerate(self.datos_originales.columns):
                variable = tk.BooleanVar(master=self.ventana, value=True)
                self.seleccion_columnas[columna] = variable
                ttk.Checkbutton(
                    self.marco_columnas, text=columna, variable=variable,
                    command=self.actualizar_vista_previa,
                ).grid(row=indice // 4, column=indice % 4, sticky="w", padx=6, pady=2)
        self.actualizar_vista_previa()

    def obtener_columnas_seleccionadas(self) -> list[str]:
        """Devolver los nombres seleccionados en su orden original."""
        if self.datos_originales is None:
            return []
        return [columna for columna in self.datos_originales.columns
                if self.seleccion_columnas[columna].get()]

    def obtener_datos_para_envio(self) -> pd.DataFrame:
        """Preparar una copia de todas las filas; bloquear selecciones vacías.

        Uso desde el hilo principal porque consulta variables Tkinter. El envío
        asíncrono captura la misma selección y copia los datos en el trabajador.
        """
        columnas = self.obtener_columnas_seleccionadas()
        if self.cargando or self.datos_originales is None or not columnas:
            mensaje = "Seleccione al menos una columna de un dataset cargado antes de preparar el envío."
            self.estado.set(mensaje)
            raise ValueError(mensaje)
        return self.datos_originales.loc[:, columnas].copy(deep=True)

    def seleccionar_todas(self, seleccionadas: bool) -> None:
        """Marcar o desmarcar todas las columnas con una sola actualización."""
        for variable in self.seleccion_columnas.values():
            variable.set(seleccionadas)
        self.actualizar_vista_previa()

    def actualizar_vista_previa(self) -> None:
        """Actualizar solo las primeras 100 filas sin alterar el DataFrame completo."""
        columnas = self.obtener_columnas_seleccionadas()
        self.actualizar_botones_red()
        self.contador.set(f"Columnas seleccionadas: {len(columnas)} de {len(self.seleccion_columnas)}")
        for elemento in self.tabla.get_children():
            self.tabla.delete(elemento)
        self.configurar_columnas(tuple(columnas))
        if not columnas:
            self.estado.set("Sin columnas seleccionadas. Envío bloqueado; seleccione al menos una columna.")
            return
        muestra = self.datos_originales.head(LIMITE_VISTA).loc[:, columnas]
        for fila in muestra.itertuples(index=False, name=None):
            valores = tuple("" if pd.isna(valor) else str(valor) for valor in fila)
            self.tabla.insert("", "end", values=valores)
        self.estado.set(f"Mostrando {len(muestra)} de {len(self.datos_originales):,} registros; {len(columnas)} columnas seleccionadas.")

    def seleccionar_archivo(self) -> None:
        """Solicitar un CSV y lanzar una única carga a la vez."""
        if self.cargando or self.operando_red:
            return
        seleccion = filedialog.askopenfilename(
            parent=self.ventana, title="Seleccionar dataset CAMELS-CL",
            initialdir=str(RUTA_DATASET.parent),
            filetypes=[("Archivos CSV", "*.csv")],
        )
        if not seleccion:
            return
        ruta = Path(seleccion)
        self.nombre.set(f"Archivo: {ruta.name}")
        self.ruta.set(f"Ruta: {ruta}")
        self.resumen.set("Registros: — | Columnas: —")
        self.datos_originales = None
        self.crear_selector_columnas()
        self.estado.set("Cargando archivo…")
        self.cargando = True
        self.actualizar_botones_red()
        self.boton.state(["disabled"])
        # Un hilo daemon permite cerrar la ventana incluso durante una lectura.
        Thread(target=cargar_en_segundo_plano, args=(ruta, self.resultados), daemon=True).start()

    def revisar_resultados(self) -> None:
        """Aplicar el resultado en el hilo principal sin bloquear la ventana."""
        try:
            resultado = self.resultados.get_nowait()
        except Empty:
            pass
        else:
            self.cargando = False
            self.actualizar_botones_red()
            self.boton.state(["!disabled"])
            if resultado.error:
                self.estado.set(f"Error de carga: {resultado.error}")
            else:
                self.datos_originales = resultado.datos
                self.resumen.set(f"Registros: {resultado.filas:,} | Columnas: {len(resultado.columnas)}")
                self.crear_selector_columnas()
        self.revisar_eventos_red()
        self.ventana.after(100, self.revisar_resultados)

    def actualizar_botones_red(self) -> None:
        """Bloquear operaciones simultáneas y envíos sin columnas."""
        ocupada = self.cargando or self.operando_red
        habilitado = not ocupada and bool(self.obtener_columnas_seleccionadas())
        self.boton_enviar.state(["!disabled" if habilitado else "disabled"])
        self.boton_probar.state(["disabled" if ocupada else "!disabled"])
        self.boton.state(["disabled" if ocupada else "!disabled"])

    def iniciar_red(self, transmitir: bool) -> None:
        """Capturar configuración y selección en el hilo principal."""
        if self.cargando or self.operando_red:
            return
        try:
            host, puerto = self.host.get().strip(), int(self.puerto.get())
            validar_destino(host, puerto)
            columnas = self.obtener_columnas_seleccionadas()
            if transmitir and (self.datos_originales is None or not columnas):
                raise ValueError("Seleccione al menos una columna antes de enviar.")
        except ValueError as error:
            self.estado.set(f"Error: {error}")
            return
        datos = self.datos_originales
        self.operando_red = True
        self.actualizar_botones_red()
        self.progreso_red.set("Enviados: 0 | Confirmados: 0")
        self.barra_progreso["value"] = 0
        self.estado.set("Enviando selección actual…" if transmitir else "Probando conexión…")

        def trabajador():
            # La selección queda fijada al pulsar Enviar. La copia costosa se hace
            # aquí; no se leen variables ni se actualizan controles de Tkinter.
            try:
                if transmitir:
                    copia = datos.loc[:, columnas].copy(deep=True)
                    resultado = enviar_datos(
                        copia, host, puerto,
                        progreso=lambda enviados, total: self.eventos_red.put(("progreso", (enviados, total))),
                    )
                    self.eventos_red.put(("fin", resultado))
                else:
                    probar_conexion(host, puerto)
                    self.eventos_red.put(("conexion", None))
            except Exception as error:
                self.eventos_red.put(("error", error))

        Thread(target=trabajador, daemon=True).start()

    def revisar_eventos_red(self) -> None:
        """Reflejar progreso y confirmaciones exclusivamente desde el hilo principal."""
        for _ in range(100):
            try:
                tipo, valor = self.eventos_red.get_nowait()
            except Empty:
                break
            if tipo == "progreso":
                enviados, total = valor
                self.progreso_red.set(f"Enviados: {enviados:,} de {total:,} | Confirmados: 0 (pendientes)")
                self.barra_progreso["value"] = 100 * enviados / max(total, 1)
            else:
                self.operando_red = False
                if tipo == "fin":
                    self.progreso_red.set(f"Enviados: {valor.enviados:,} | Confirmados: {valor.confirmados:,}")
                    self.barra_progreso["value"] = 100
                    self.estado.set("Transmisión completada y confirmada por el servidor.")
                elif tipo == "conexion":
                    self.estado.set("Conexión y protocolo comprobados correctamente.")
                else:
                    if isinstance(valor, ErrorTransmision):
                        self.progreso_red.set(f"Enviados: {valor.enviados:,} | Confirmados: 0 (sin confirmación válida)")
                    self.estado.set(f"Error de comunicación: {valor}")
                self.actualizar_botones_red()


def main() -> None:
    """Abrir la aplicación de escritorio."""
    ventana = tk.Tk()
    InterfazCliente(ventana)
    ventana.mainloop()


if __name__ == "__main__":
    main()
