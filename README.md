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
| 4. Comunicación TCP local | Protocolo Grupo 4, lotes, progreso y confirmación JSON. | `transmision.py`, `servidor_prueba.py` |

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

## TCP compatible con Grupo 4

Inicie el servidor oficial y pulse **Iniciar escucha**:

```powershell
python C:\Users\msoto\Desktop\bigdata_evaluacion2\servidor_prueba.py
```

Abra `python interfaz_cliente.py`, cargue el CSV, seleccione columnas y use
`127.0.0.1:12345`. Para otro equipo use la IP y escucha correspondientes.
Pulse **Enviar datos**. Se conservan lectura, validaciones, vista previa,
selección y progreso; las operaciones de red siguen en segundo plano.

**Probar conexión** solo abre y cierra TCP, sin enviar bytes. No valida el
protocolo ni transmite un ping o un dataset vacío. El servidor oficial puede
registrarla como transferencia incompleta; su dataset anterior se conserva.

### Protocolo

Una transferencia es un solo arreglo JSON de objetos UTF-8 con sufijo `<EOF>`:

```text
[{"fecha":"2015-01-01","caudal_m3_s":0.644}]<EOF>
```

El cliente serializa fila por fila, agrupando lotes de aproximadamente 64 KiB,
sin construir el JSON completo en memoria. Un registro individual puede superar
ese tamaño. Las claves son las columnas seleccionadas; nulos y valores no
finitos se representan como `null`. `<EOF>` dentro de valores se conserva.
No se usan JSON Lines ni mensajes de inicio, fin, ping o pong.

Tras enviar el sufijo se ejecuta `shutdown(socket.SHUT_WR)` y se mantiene la
lectura abierta. El servidor espera ese cierre antes de responder y cerrar:

```json
{"ok":true,"filas":569250}
```

Solo hay éxito si `ok` es exactamente `true` y `filas` es un entero igual
al total enviado. Rechazos (`ok: false`, con `detalle`), respuestas vacías,
malformadas, conteos distintos, desconexiones o timeout producen un error.
Se recompone la respuesta hasta el cierre TCP, con un límite de 64 KiB.
El timeout del cliente es 120 segundos por operación bloqueante. El servidor
oficial tiene sus propios límites (512 MiB, 60 segundos sin avance y
600 segundos totales por defecto).

La barra distingue enviados (lotes cuyo `sendall` terminó) de confirmados
(conteo final validado). El 100 % enviado puede seguir esperando respuesta.
Un lote parcialmente enviado no cuenta; confirmados en cero significa ausencia
de confirmación válida. No hay reintentos ni reanudación automáticos.
El protocolo no incorpora autenticación ni cifrado.

El receptor local usa el mismo formato, sin persistencia ni dashboard:

```powershell
python servidor_prueba.py --host 127.0.0.1 --puerto 12345
```

### Pruebas

```powershell
python -B -m unittest discover -v
```

Se verifican lotes, nulos, Unicode, marcas dentro de valores, respuestas
fragmentadas e inválidas, timeout, selección vacía, prueba TCP sin bytes,
receptor local y acciones de GUI con mocks sin abrir ventanas.
La integración carga el servidor de referencia del Grupo 4 en memoria y
prueba su adaptador `atender` mediante puertos efímeros. Si el archivo de
referencia no existe, esa prueba se omite. No se modifican sus archivos.

El 17 de octubre de 2026 pasaron siete pruebas, incluyendo 50 filas y las
**569.250 filas del CSV enviadas, recibidas y confirmadas** por el Grupo 4.
Se comprobó el hash del dataset antes y después. La aceptación visual del
dashboard Tkinter queda para una verificación manual.

Tkinter requiere Tcl/Tk en la instalación de Python. No se agregan dependencias.
Los CSV permanecen excluidos de Git.

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
