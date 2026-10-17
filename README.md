# Grupo 3 — Cliente Big Data

El proyecto lee y valida el dataset con Python y pandas e incluye una interfaz
gráfica Tkinter para consultarlo y un cliente TCP con servidor local de pruebas.
No modifica el archivo original ni guarda los datos recibidos en el servidor.

## Primera versión funcional: cuatro etapas

| Etapa | Implementación | Archivo principal |
| --- | --- | --- |
| 1. Lectura y validación | Detección de formato, dimensiones, tipos, muestra, nulos, duplicados y mínimo de 500.000 registros. | `cliente.py` |
| 2. Interfaz gráfica | Selección del CSV, resumen, tabla de hasta 100 filas y lectura en segundo plano. | `interfaz_cliente.py` |
| 3. Selección de columnas | Casillas dinámicas, selección total o vacía, contador y conservación del DataFrame completo. | `interfaz_cliente.py` |
| 4. Comunicación TCP local | JSON Lines, envío progresivo, progreso, confirmación y servidor de pruebas independiente del Grupo 4. | `transmision.py`, `servidor_prueba.py` |

Requiere Python 3.10 o posterior con Tcl/Tk y la dependencia `pandas` declarada
en `requirements.txt`. Tkinter, sockets, JSON y las pruebas usan la biblioteca
estándar; no se instalan por separado con pip.

El dataset no se distribuye en este repositorio. Coloque su copia local en
`datos/CAMELS_CL_caudales_2015_2019_569250.csv`; no la agregue a Git.

## Interfaz gráfica

Desde la terminal de VS Code, en la carpeta del proyecto:

```powershell
python interfaz_cliente.py
```

Pulse **Seleccionar archivo CSV** y elija el dataset de la carpeta `datos`.
La ventana muestra el nombre, la ruta, la cantidad total de registros y columnas,
y una tabla con un máximo de 100 registros y desplazamiento horizontal y vertical.
Se requieren las ocho columnas CAMELS-CL con sus nombres originales; se conserva
su orden en el archivo seleccionado. Los valores nulos se muestran como celdas vacías.

La barra de estado informa la carga, su finalización o cualquier error. Mientras
se lee el archivo, el botón permanece deshabilitado para evitar cargas simultáneas.
La lectura completa se realiza en un hilo de trabajo y el hilo principal actualiza
los controles mediante una cola consultada con `after`. Se conserva el DataFrame
completo en memoria; la tabla sigue limitada a las primeras 100 filas.

### Selección de columnas (Etapa 3)

La sección **Columnas disponibles para transmisión** crea una casilla por columna
del CSV cargado, inicialmente marcada. Puede cambiar cada casilla o usar
**Seleccionar todas** y **Deseleccionar todas**. El contador y la vista previa
se actualizan con cada selección, conservando el orden original del archivo.
Sin columnas seleccionadas, la tabla queda vacía y la barra de estado informa
que el envío está bloqueado.

La clase `InterfazCliente` expone `obtener_columnas_seleccionadas()`, que devuelve
los nombres en orden original, y `obtener_datos_para_envio()`, que devuelve una
copia independiente con todas las filas y solo las columnas elegidas. Este último
método lanza `ValueError` si no hay dataset, hay una carga en curso o la selección
está vacía. Las futuras acciones de envío deberán usar este método y manejar el
error. Ambos métodos se llaman desde el hilo principal por consultar variables
de Tkinter. El envío TCP captura la misma selección en el hilo principal y hace
la copia del DataFrame en el trabajador para no bloquear la ventana.

Seleccionar columnas no modifica el DataFrame original ni el CSV. Al elegir otro
archivo se limpia la selección anterior y, tras una carga correcta, se seleccionan
todas las columnas del nuevo dataset.

## Comunicación TCP local (Etapa 4)

En una terminal de VS Code, iniciar el servidor:

```powershell
python servidor_prueba.py
```

En otra terminal, abrir la aplicación:

```powershell
python interfaz_cliente.py
```

Cargar el CSV, seleccionar columnas, mantener IP `127.0.0.1` y puerto `5000`,
pulsar **Probar conexión** y luego **Enviar datos**. La prueba usa ping/pong y
no envía filas. El envío transmite todas las filas con las columnas seleccionadas
al pulsar el botón; cambiar casillas durante la operación no cambia ese envío.
Se bloquean nuevas cargas y operaciones de red simultáneas. Sin columnas, el
botón de envío queda deshabilitado y también existe validación antes de conectar.

La barra muestra el progreso y separa **enviados** (registros de lotes cuyo
`sendall` terminó) de **confirmados** (conteo final validado por el servidor).
El progreso del 100 % enviado todavía puede estar esperando confirmación.
Ante desconexión o confirmación incorrecta no se declara éxito: confirmados queda
en cero, que significa ausencia de confirmación válida, no necesariamente que el
servidor no haya recibido datos. Un lote parcialmente enviado al fallar no se
incluye en el conteo local. No hay reintentos automáticos ni reanudación.

El servidor acepta conexiones sucesivas de forma secuencial hasta **Ctrl+C** y
solo cuenta y valida registros; no los persiste. Para otro destino local:

```powershell
python servidor_prueba.py --host 127.0.0.1 --puerto 5001
```

### Protocolo provisional v1

Se centraliza en `transmision.py` para adaptarlo al futuro contrato del Grupo 4.
**El protocolo es provisional y debe coordinarse con el Grupo 4 antes de integrar
ambos sistemas**: nombres y orden de campos, representación de valores, mensajes
de control, versión, límites y confirmaciones forman parte de ese acuerdo.
Cada conexión admite una prueba ping/pong o una transmisión. Todos los mensajes
son objetos JSON estrictos en UTF-8 seguidos por `\n` (JSON Lines). Cada línea
admite como máximo **1 MiB**, incluido el salto. El receptor usa lectura de líneas
con búfer y límite, por lo que admite fragmentación y agrupación TCP, incluso
cuando un carácter UTF-8 queda dividido entre paquetes. No se usa `recv` como
si cada llamada fuera un mensaje completo.

Ejemplo con dos columnas y una fila:

```jsonl
{"tipo":"inicio","version":1,"columnas":["fecha","caudal_m3_s"],"registros":1}
{"tipo":"registro","datos":["2015-01-01",0.644]}
{"tipo":"fin","registros":1}
```

El servidor devuelve:

```jsonl
{"tipo":"confirmacion","registros":1}
```

`datos` es una lista de valores en el orden de `columnas`. El campo exterior
`tipo` distingue controles y registros sin confundirse con nombres o valores del
dataset. Nulos y números no finitos se representan como `null`; el CSV CAMELS-CL
aporta cadenas y números. El servidor exige cabecera válida, versión compatible,
cantidad de valores correcta y coincidencia entre conteo inicial, final y recibido.
Ante un mensaje inválido intenta responder `{"tipo":"error","mensaje":"..."}`
y cierra la conexión sin confirmar. Cerrar sin `fin` es una transmisión incompleta.

La prueba de conexión envía `{"tipo":"ping","version":1}` y espera
`{"tipo":"pong","version":1}`. Los sockets tienen timeout de **30 segundos por
operación bloqueante**, no por transferencia completa. Las filas se serializan
progresivamente y se agrupan en lotes de aproximadamente **64 KiB**; una línea
individual puede superar el lote, hasta el límite de 1 MiB. No se construye una
cadena JSON del dataset completo. Esta herramienta está destinada a pruebas
locales; el protocolo actual no incorpora autenticación, cifrado ni persistencia.

### Comprobaciones reproducibles

```powershell
python -B -m unittest test_transmision -v
```

La primera prueba transmite **50 filas** y después **569.250 filas** del CSV real,
verificando en cada caso enviados = recibidos = confirmados. Se usan servidores
TCP locales con puertos efímeros; no es necesario abrir el servidor manualmente.
También se comprueban fragmentación byte a byte, UTF-8, selección de columnas,
nulos, ping/pong, confirmación incorrecta, desconexión, timeout, conteos inválidos,
límites de línea y selección vacía. Se verifica el hash del CSV antes y después.
Los datos permanecen locales; todos los archivos `*.csv` están excluidos mediante `.gitignore`.

### Prueba local con cinco columnas

El 17 de octubre de 2026 se verificó una transmisión TCP local completa con las
columnas `fecha`, `estacion_id`, `estacion_nombre`, `zona_latitudinal` y
`caudal_m3_s`, en ese orden, contra `servidor_prueba.procesar_conexion` mediante
un puerto local efímero. Se utilizó el cliente real `transmision.enviar_datos`.

| Columnas | Registros enviados | Registros recibidos | Registros confirmados |
| ---: | ---: | ---: | ---: |
| 5 | 569.250 | 569.250 | 569.250 |

Para repetirla desde la interfaz, inicie el servidor, cargue el dataset, marque
solo esas cinco columnas y pulse **Enviar datos**. La vista previa muestra hasta
100 filas, pero la transmisión incluye las 569.250. La suite automatizada anterior
prueba las ocho columnas; esta comprobación adicional verifica cinco columnas.

Tkinter forma parte de la distribución habitual de Python para Windows y no se
agregan dependencias. Si Python informa que falta `tkinter`, habilite el componente
Tcl/Tk en el instalador de Python. La validación por terminal sigue disponible
mediante el comando siguiente.

## Ejecución

Desde la terminal de VS Code, en la carpeta del proyecto:

```powershell
python cliente.py
```

Si pandas no está disponible en el entorno Python seleccionado:

```powershell
python -m pip install -r requirements.txt
```

La ruta `datos/CAMELS_CL_caudales_2015_2019_569250.csv` se resuelve respecto
a la ubicación de `cliente.py`, independientemente del directorio de ejecución.

El informe muestra dimensiones, columnas y tipos, cinco registros de ejemplo,
nulos por columna, duplicados y cumplimiento del mínimo de 500.000 filas.
Los duplicados se cuentan después de su primera aparición, considerando todas
las columnas. Los nulos y duplicados se informan sin eliminar registros.

La lectura intenta UTF-8 con separador coma y luego infiere el formato con una
muestra si falla o devuelve una sola columna. La detección admite coma, punto y
coma, tabulación y barra vertical; considera UTF-8, UTF-16/32 con BOM, CP1252 y
Latin-1. La inferencia de codificaciones sin BOM es heurística.

Códigos de salida: `0` si cumple el mínimo, `1` ante errores de lectura y `2`
si contiene menos de 500.000 registros.
