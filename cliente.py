"""Lectura y validación inicial del dataset del Grupo 3 (Cliente)."""

import codecs
import csv
from pathlib import Path

import pandas as pd


RUTA_DATASET = (
    Path(__file__).resolve().parent
    / "datos"
    / "CAMELS_CL_caudales_2015_2019_569250.csv"
)
MINIMO_REGISTROS = 500_000


def detectar_formato(ruta: Path) -> tuple[str, str]:
    """Inferir codificación y separador con una muestra, sin dependencias extra.

    Las codificaciones sin marca no siempre pueden distinguirse con certeza.
    Se priorizan UTF-8, UTF-16/32 con BOM y codificaciones occidentales.
    """
    with ruta.open("rb") as archivo:
        muestra = archivo.read(65_536)

    if muestra.startswith((codecs.BOM_UTF32_LE, codecs.BOM_UTF32_BE)):
        candidatas = ("utf-32",)
    elif muestra.startswith((codecs.BOM_UTF16_LE, codecs.BOM_UTF16_BE)):
        candidatas = ("utf-16",)
    else:
        candidatas = ("utf-8-sig", "cp1252", "latin-1")

    for codificacion in candidatas:
        try:
            # Una muestra puede terminar en mitad de un carácter multibyte.
            decodificador = codecs.getincrementaldecoder(codificacion)()
            texto = decodificador.decode(muestra, final=False)
        except UnicodeError:
            continue
        try:
            dialecto = csv.Sniffer().sniff(texto, delimiters=",;\t|")
            return codificacion, dialecto.delimiter
        except csv.Error:
            continue

    raise ValueError("No se pudo detectar la codificación o el delimitador del CSV.")


def leer_dataset(ruta: Path) -> pd.DataFrame:
    """Leer el CSV y recurrir a detección de formato si es necesario."""
    if not ruta.is_file():
        raise FileNotFoundError(f"No se encontró el archivo: {ruta}")

    try:
        datos = pd.read_csv(ruta, encoding="utf-8-sig", sep=",")
        if len(datos.columns) > 1:
            print("Lectura: codificación UTF-8 y delimitador coma.")
            return datos
        # Otro delimitador puede producir una sola columna sin lanzar error.
    except (UnicodeError, pd.errors.ParserError):
        pass

    codificacion, delimitador = detectar_formato(ruta)
    print(f"Formato detectado: codificación {codificacion}, delimitador {delimitador!r}.")
    return pd.read_csv(ruta, encoding=codificacion, sep=delimitador)


def validar_dataset(datos: pd.DataFrame) -> bool:
    """Mostrar el diagnóstico y devolver si se cumple el mínimo de registros.

    Los duplicados se comparan usando todas las columnas; no se eliminan.
    Los nulos se interpretan según las reglas predeterminadas de pandas.
    """
    filas, columnas = datos.shape
    print(f"\nTotal de filas: {filas:,}")
    print(f"Total de columnas: {columnas}")
    print("\nColumnas y tipos de datos:")
    print(datos.dtypes.to_string())
    print("\nPrimeros cinco registros:")
    print(datos.head(5).to_string(index=False))

    nulos = datos.isna().sum()
    print("\nValores nulos por columna:")
    print(nulos.to_string())
    print(f"Total de valores nulos: {int(nulos.sum()):,}")
    print(f"Registros duplicados (posteriores a la primera aparición): {int(datos.duplicated().sum()):,}")

    cumple_minimo = filas >= MINIMO_REGISTROS
    estado = "CUMPLE" if cumple_minimo else "NO CUMPLE"
    print(f"\nValidación de al menos {MINIMO_REGISTROS:,} registros: {estado}.")
    return cumple_minimo


def main() -> int:
    """Ejecutar la validación; devolver 0 si cumple, 1 si falla la lectura o 2 si no cumple."""
    try:
        datos = leer_dataset(RUTA_DATASET)
        return 0 if validar_dataset(datos) else 2
    except FileNotFoundError as error:
        print(f"Error de archivo: {error}")
    except pd.errors.EmptyDataError:
        print("Error de lectura: el archivo está vacío o no contiene columnas.")
    except (OSError, UnicodeError, pd.errors.ParserError, ValueError) as error:
        print(f"Error de lectura del dataset: {error}")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
