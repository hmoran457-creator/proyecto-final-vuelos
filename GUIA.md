# Guía paso a paso: de cero a la presentación

Esta guía es para ti, no para el profesor (el profesor solo lee el `README.md`). Síguela en orden, con la
guía abierta en una pestaña y Databricks en otra. Cada paso tiene cuatro partes: **Qué vas a hacer**,
**Clics exactos**, **Qué deberías ver** y **Si falla**.

> **Estado honesto del proyecto (léelo una vez).** Los notebooks `01_bronce`, `02_plata` y `03_oro` se
> corrieron en una computadora con Spark local sobre 61,536 vuelos reales de julio de 2024 más errores
> sembrados a propósito, y detectaron todo. **Nada se ha corrido todavía en un workspace real de
> Databricks.** Tampoco se ha comprobado que Model Serving y Databricks Apps funcionen con modelos propios en
> Free Edition: eso sale de la documentación. Por eso el orden de esta guía pone primero lo más incierto
> (el endpoint y la app, que valen 55 de 100 puntos) y deja la limpieza de datos corriendo en paralelo.
> Es normal que algo falle. Para eso están las secciones "Si falla" y "Cómo pedir ayuda a otro modelo de IA".

> **Los menús de Databricks cambian de nombre seguido.** Los nombres de abajo (**Serverless**, **Run all**,
> **Add or upload data**, **Jobs & Pipelines**, **Serving**, etc.) vienen de la documentación oficial de
> noviembre de 2026. Si uno no aparece igual, busca uno parecido en la misma zona (la barra lateral
> izquierda, el botón **New** o **Create**, o el buscador de arriba) y sigue. La lógica de los pasos no cambia.

---

## Resumen de una pantalla

| Paso | Qué | Dónde | Tiempo estimado | Criterio de rúbrica que respalda |
|---|---|---|---|---|
| 0 | Cuenta Free Edition, repo en GitHub, Git folder en Databricks | Navegador, terminal | 15 a 30 min | Entregable (repo con README) |
| 1 | Crear esquema/volumen (`00`), descargar y subir los 12 zips | Databricks + laptop | 30 a 45 min | Base del 1 (datos) |
| 2 | Prueba de humo y publicar el endpoint de prueba | Databricks | 15 min tuyos + 10 a 20 min de espera | 3 (25 pts) |
| 3 | Bronce, plata y oro (mientras el endpoint se construye) | Databricks, 2.ª pestaña | 45 a 90 min | 1 (35 pts) |
| 4 | Desplegar la app Streamlit contra el endpoint de prueba y probar fallas | Databricks Apps | 30 a 45 min | 4 (30 pts) |
| 5 | Entrenar el modelo real y actualizar el endpoint | Databricks | 30 min tuyos + 10 a 25 min de espera | 2 (10 pts) y 3 |
| 6 | Job de Lakeflow con 3 tareas y horario | Databricks | 20 min | 1 (metodología por capas, orquestación) |
| 7 | Llenar el README, capturas, commit y push | Laptop | 60 a 90 min | Entregable |
| Final | Checklist, guion de demo, ensayo | Tu pantalla | 30 min | Presentación en vivo |

### Reparto sugerido

| Día | Meta | Pasos |
|---|---|---|
| **Miércoles 18 (hoy)** | Tener el endpoint de prueba y la app funcionando, y los datos cargados hasta oro | 0, 1, 2, 3 y, si el endpoint ya está **Ready**, 4 |
| **Jueves 19** | Modelo real publicado, Job creado, README casi completo | 4 (si faltó), 5, 6, 7 |
| **Viernes 20 (mañana)** | Cero cambios grandes. Checklist, ensayo y capturas de respaldo | Checklist del día de la presentación, ensayo con el guion |

Regla de oro: **si el miércoles en la noche el endpoint no se puede crear, pasa al Plan B ese mismo día**
(sección al final), no el jueves.

---

## Paso 0 · Cuenta, repositorio y Git folder (15 a 30 min)

**Qué vas a hacer.** Crear tu cuenta de Databricks Free Edition, subir estos archivos a un repositorio
público de GitHub y traerlos a Databricks como una carpeta Git.

**Clics exactos**

1. Crea tu cuenta de Databricks Free Edition (busca "Databricks Free Edition" y regístrate; no pide tarjeta).
   Al entrar verás tu workspace.
2. En GitHub: **New repository** > nombre `proyecto-final-vuelos` > **Public** > **Create repository**.
3. En tu laptop, dentro de la carpeta `proyecto-final-vuelos`, en una terminal:
   ```
   git init
   git add .
   git commit -m "Proyecto final: primera versión"
   git branch -M main
   git remote add origin https://github.com/<TU_USUARIO>/proyecto-final-vuelos.git
   git push -u origin main
   ```
4. En Databricks: barra lateral > **Workspace** > **Create** > **Git folder**.
5. En **Git repository URL** pega la liga de tu repo, en **Git provider** elige **GitHub** y pulsa **Create Git folder**.

**Qué deberías ver**
- [ ] En GitHub, el repo con `README.md`, `GUIA.md`, `notebooks/`, `app/`, etc.
- [ ] En Databricks, una carpeta con el nombre del repo y, dentro, `notebooks/` con archivos que tienen
  ícono de notebook (no de archivo de texto).

**Si falla**
- *Pide credenciales aunque el repo es público:* la documentación es inconsistente en esto. Prueba cerrar
  y reabrir el diálogo; si insiste, usa el plan manual de abajo.
- *Plan manual (sin Git folder):* en Databricks, **Workspace** > **Create** > **Folder** (llámala
  `proyecto-final-vuelos`). Dentro de ella usa **Import** (menú de los tres puntos de la carpeta, o el botón
  **Import** de Workspace) y sube uno por uno los `.py` de `notebooks/` (`_config.py`, `00_...`, `01_...`, etc.).
  Databricks los reconoce como notebooks porque su primera línea es `# Databricks notebook source`. Deben
  quedar **todos en la misma carpeta**, porque se llaman entre sí con `%run ./_config`. Para la app
  necesitarás también la carpeta `app/` (puedes importarla igual, archivo por archivo, a una carpeta `app`).
- *`git push` pide usuario y contraseña:* GitHub ya no acepta contraseña. Usa GitHub Desktop o el
  inicio de sesión del navegador que ofrece Git; si no, pide ayuda con el prompt de la última sección.

---

## Paso 1 · Preparar el entorno y subir los 12 zips (30 a 45 min)

**Qué vas a hacer.** Correr `00_preparar_entorno` para crear el esquema y el volumen, descargar los 12 zips
del BTS en tu laptop (el workspace probablemente no tiene salida a esos servidores) y subirlos al volumen.

### 1A. Correr el notebook 00

**Clics exactos**

1. Abre `notebooks/00_preparar_entorno` en tu Git folder.
2. Arriba a la derecha, en el selector de cómputo, elige **Serverless**.
3. Pulsa **Run all**.

**Qué deberías ver**
- La celda 1 imprime `Esquema : workspace.vuelos` y `Volumen : /Volumes/workspace/vuelos/crudo`. **Anota
  el nombre del catálogo que imprime** (casi seguro `workspace`).
- La celda 2 intenta bajar los datos. Lo más probable es que imprima `mes 01: FALLO ...` y el mensaje
  `La descarga directa no está permitida desde este workspace.` **Eso es lo esperado, no es un error tuyo.**
- La celda 3 termina con `FALTAN ARCHIVOS: sube los zips que faltan y vuelve a correr esta celda.`
  (todavía no hay zips).

**Si falla**
- *Error de permiso en `CREATE SCHEMA`:* abre `notebooks/_config.py` y cambia `ESQUEMA = "vuelos"` por
  `ESQUEMA = "default"`; guarda y vuelve a correr. El esquema `default` ya existe y no necesita permiso para crearse.
  Si lo cambias, hazlo **antes** de correr cualquier otro notebook, y recuerda que las rutas del volumen
  cambiarán a `/Volumes/<catálogo>/default/crudo`.
- *El catálogo no se llama `workspace`:* no pasa nada, el código lo detecta solo. Solo usa el nombre que
  imprimió al subir los archivos (siguiente paso).
- *No aparece el selector Serverless:* en Free Edition solo existe cómputo serverless; si no ves el
  selector, busca un botón **Connect** arriba a la derecha y elige **Serverless**.

### 1B. Descargar los 12 zips en tu laptop

**Opción 1 (un comando).** Abre **PowerShell** en la carpeta `datos` del proyecto y corre:

```
.\descargar_zips.ps1
```

Si dice que la ejecución de scripts está deshabilitada, corre antes (una vez, en esa misma ventana):
`Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass`. Para ver solo las ligas sin descargar:
`.\descargar_zips.ps1 -SoloListar`. Los archivos quedan en `datos\zips\` como `vuelos_2024_01.zip` ... `vuelos_2024_12.zip`.
Si el script se interrumpe, vuélvelo a correr: se salta los que ya bajaron.

**Opción 2 (a mano).** Abre cada liga en el navegador (cada zip pesa de 25 a 35 MB; el total es de unos 360 MB):

| Mes | Liga |
|---|---|
| 1 | https://transtats.bts.gov/PREZIP/On_Time_Reporting_Carrier_On_Time_Performance_1987_present_2024_1.zip |
| 2 | https://transtats.bts.gov/PREZIP/On_Time_Reporting_Carrier_On_Time_Performance_1987_present_2024_2.zip |
| 3 | https://transtats.bts.gov/PREZIP/On_Time_Reporting_Carrier_On_Time_Performance_1987_present_2024_3.zip |
| 4 | https://transtats.bts.gov/PREZIP/On_Time_Reporting_Carrier_On_Time_Performance_1987_present_2024_4.zip |
| 5 | https://transtats.bts.gov/PREZIP/On_Time_Reporting_Carrier_On_Time_Performance_1987_present_2024_5.zip |
| 6 | https://transtats.bts.gov/PREZIP/On_Time_Reporting_Carrier_On_Time_Performance_1987_present_2024_6.zip |
| 7 | https://transtats.bts.gov/PREZIP/On_Time_Reporting_Carrier_On_Time_Performance_1987_present_2024_7.zip |
| 8 | https://transtats.bts.gov/PREZIP/On_Time_Reporting_Carrier_On_Time_Performance_1987_present_2024_8.zip |
| 9 | https://transtats.bts.gov/PREZIP/On_Time_Reporting_Carrier_On_Time_Performance_1987_present_2024_9.zip |
| 10 | https://transtats.bts.gov/PREZIP/On_Time_Reporting_Carrier_On_Time_Performance_1987_present_2024_10.zip |
| 11 | https://transtats.bts.gov/PREZIP/On_Time_Reporting_Carrier_On_Time_Performance_1987_present_2024_11.zip |
| 12 | https://transtats.bts.gov/PREZIP/On_Time_Reporting_Carrier_On_Time_Performance_1987_present_2024_12.zip |

No descomprimas nada: se suben tal cual. Los nombres de archivo no importan; lo que importa es que sean 12 zips distintos.

- [ ] Tengo 12 archivos `.zip` en mi laptop, de 25 a 35 MB cada uno.

### 1C. Subir los zips al volumen

**Clics exactos**

1. En la barra lateral: **New** > **Add or upload data** > **Upload files to a volume**.
2. Arrastra los 12 zips al área **Files** (puedes soltarlos todos juntos; el límite es de 5 GB por archivo).
3. En **Destination volume** elige el catálogo `workspace` (o el que imprimió el notebook 00) > esquema
   `vuelos` > volumen `crudo` > carpeta `zips`.
4. Confirma la subida y espera a que termine.
5. Vuelve al notebook `00_preparar_entorno` y corre **solo la última celda** (la de la sección 3,
   "Verificación").

**Qué deberías ver**
- [ ] Una tabla con 12 filas, cada una con tamaño de 20 a 40 MB y `estado = ok`.
- [ ] El mensaje `Zips válidos en /Volumes/workspace/vuelos/crudo/zips: 12 de 12 esperados` y debajo
  `LISTO: están los 12 meses. Sigue con el notebook 01_bronce.`

**Si falla**
- *Dice menos de 12 pero más de 8:* con 8 o más meses ya se rebasan los 4 millones de registros. Puedes
  continuar, pero sube los que faltan cuando puedas.
- *`ZIP DAÑADO: vuelve a subirlo`:* ese archivo se cortó en la descarga o en la subida. Descárgalo otra vez y súbelo de nuevo
  (el zip dañado se sobrescribe si tiene el mismo nombre).
- *No encuentras la carpeta `zips` en el selector de destino:* el notebook 00 la crea al correr; si no
  aparece, corre otra vez la celda 1 del notebook 00 y recarga la página de subida.
- *La pantalla de subida no tiene la opción de volumen:* busca **Catalog** en la barra lateral, abre
  `workspace` > `vuelos` > `crudo`, abre la carpeta `zips` y usa el botón **Upload to this volume**.

---

## Paso 2 · Prueba de humo y endpoint de prueba (15 min tuyos + espera)

**Qué vas a hacer.** Antes de invertir horas en los datos, comprobar que Free Edition te deja publicar
un modelo propio. `prueba_de_humo` registra un modelo entrenado con datos **sintéticos** (inventados, solo
para probar la plomería) en Unity Catalog; `05_publicar_endpoint` crea el endpoint y lo prueba.

### 2A. Prueba de humo

**Clics exactos**: abre `notebooks/prueba_de_humo` > **Serverless** > **Run all**.

**Qué deberías ver**
- Una tabla con unos 5,000 vuelos sintéticos (inventados) y la línea `Vuelos sintéticos: 5,000 | rutas distintas: ...`.
- Al registrar, una línea `Librerías que instalará el endpoint: mlflow==..., scikit-learn==..., pandas==..., numpy==..., cloudpickle==...`.
  Si ahí aparece también `starlette<1`, es normal (ver "Si falla" de 2B).
- `Rechazo esperado para un vuelo inválido:` seguido de un mensaje que empieza con `Entrada inválida:` (el modelo rechaza mes 13).
- Al final: `OK: modelo de prueba registrado: <catálogo>.vuelos.modelo_retrasos versión <n> (alias campeon)` y la indicación de seguir con `05_publicar_endpoint`.

**Este modelo de prueba solo conoce 17 aeropuertos** (ATL, DFW, DEN, ORD, LAX, JFK, LAS, MCO, CLT, SEA, PHX, MIA, SFO, IAH, EWR, BOS, LGA) y las aerolíneas de `AEROLINEAS`.
Cualquier otro (por ejemplo HNL) lo rechaza la API a propósito. Es lo esperado con el modelo de prueba; el modelo real conoce todos los aeropuertos de los datos.

**Si falla**
- *Error de permisos al registrar el modelo o "schema not found":* repite el Paso 1A (el esquema debe existir).
- *Error de `pip` o de versiones de `mlflow`:* copia el error completo y usa la última sección de esta guía.
- *`ModuleNotFoundError` o `No module named 'mlflow....'` al registrar el modelo:* falta una parte de MLflow en el entorno serverless.
  Agrega **una celda nueva al principio** del notebook (arriba de todo, con **+ Code**) con este contenido:
  ```
  %pip install --upgrade "mlflow-skinny[databricks]"
  ```
  y, en otra celda justo debajo:
  ```
  dbutils.library.restartPython()
  ```
  Ejecuta esas dos celdas y después vuelve a correr el notebook completo (es el arreglo que da la documentación oficial de Unity Catalog para serverless).
- *Dice que el modelo "exige firma" (signature):* es un error del código, no tuyo; pásalo a la IA con el prompt de la última sección.
- *`RuntimeError: No se pudieron obtener credenciales para la API de Databricks`* (la lanza la función `credenciales()` de `_modelo.py`; en 2A no suele salir, pero sí en 2B): pega el mensaje completo, que trae dos causas (SDK y contexto del notebook), a la IA con el prompt de la última sección.

### 2B. Publicar el endpoint

**Clics exactos**: abre `notebooks/05_publicar_endpoint` > **Serverless** > **Run all**.

**Qué deberías ver**
- Primero la versión que va a publicar: `Alias  : campeon -> versión <n>`.
- Luego una línea `Acción: creado` (la primera vez). Las otras posibles son `actualizado` (había otra versión),
  `en curso` (ya se estaba actualizando a esa versión) y `sin cambios` (ya la sirve y está lista; solo repite las pruebas).
- Mientras construye, una línea cada 30 segundos como esta:
  `[03:30] ready=NOT_READY | config_update=IN_PROGRESS | versiones servidas=-`
  El campo `ready` pasa a `READY` y `config_update` a `NOT_UPDATING`; entonces imprime `Endpoint listo después de X minutos.`
- **La primera vez el endpoint tarda unos 10 minutos (hasta 20) en construirse.** El notebook espera como máximo **45 minutos**
  y, si se pasa, lanza `TimeoutError` ("Pasaron 45 minutos..."); el despliegue sigue en Databricks de todos modos.

**Recomendado: no dejes el notebook esperando (cuota de cómputo).** Un notebook que se queda consultando el estado mantiene ocupado el
cómputo serverless, y pasarte de la cuota diaria apaga el cómputo el resto del día. Una vez que el notebook imprimió `Acción: creado`
(o `actualizado`) y las primeras líneas `[mm:ss] ready=...`, el despliegue **continúa del lado de Databricks aunque cancelaste la celda**:

1. **Cancela la celda de espera** (botón de detener/interrumpir de la celda, o **Interrupt** arriba).
2. Pasa al siguiente paso (Paso 3). No necesitas tener este notebook abierto ni corriendo.
3. En la barra lateral, **Serving** > `vuelos-retrasos`: espera a que el estado diga **Ready** (revisa de vez en cuando; si falla, ahí están **Events** y **Logs**).
4. Cuando esté **Ready**, corre `05_publicar_endpoint` de nuevo (**Run all**). Imprime `Acción: sin cambios`, ejecuta las 9 pruebas y termina en
   alrededor de un minuto.

Si prefieres esperar con el notebook (más cómodo, pero gasta cómputo), también funciona: sigue hasta `Endpoint listo después de X minutos.`
Si se corta o cierras la pestaña, vuelve a correrlo: imprime `Acción: en curso` y solo continúa esperando.

**Ya con el endpoint listo** (en esa segunda corrida de `05`) deberías ver:
- Una línea `Acción: sin cambios` (o `creado`/`actualizado`/`en curso`, según el caso; ver arriba).
- Las pruebas en vivo: 9 líneas `[PASA ] ...` (2 vuelos válidos y 7 inválidos) y una tabla con la columna `resultado`.
- El esquema OpenAPI, de forma **opcional ("mejor esfuerzo")**. Si el workspace lo devuelve, se guarda **siempre** en el volumen:
  `/Volumes/<catálogo>/vuelos/crudo/docs/openapi.json` (`Guardado en el volumen: ...`); solo si el notebook corre dentro del Git folder,
  imprime además `Guardado en el repositorio: .../docs/openapi.json`, y si no, `No encontré la carpeta docs del repositorio...` (ver Paso 7).
  **Si el workspace no puede devolverlo**, imprime una línea que empieza con `AVISO: no se pudo obtener el esquema OpenAPI` y **continúa**
  hasta los `OK:` finales. No es un error: el repo ya trae una versión base, `docs/openapi.json`, escrita a mano a partir de la firma del modelo, que
  sigue siendo documentación válida del contrato. Para mostrarla como Swagger en la presentación, abre https://editor.swagger.io, pega ahí el contenido de `docs/openapi.json` (o usa **File** > **Import file**).
- Al final: `OK: endpoint vuelos-retrasos listo, sirviendo la versión <n>` y `OK: las 9 pruebas del endpoint pasaron`.
- En **Serving** > `vuelos-retrasos`, el estado es **Ready**.

- [ ] El endpoint `vuelos-retrasos` aparece como **Ready**.
- [ ] El notebook imprimió `OK: las 9 pruebas del endpoint pasaron`.

**Sobre abrir una segunda pestaña:** puedes tener Databricks en dos pestañas, pero dos notebooks corriendo a la vez **sí** consumen cómputo
(cada uno ocupa serverless mientras corre), así que no es gratis. Por eso el flujo recomendado es cancelar la espera de `05` y correr
`01_bronce`, `02_plata` y `03_oro` mientras el endpoint se construye solo en los servidores de Databricks. Si un notebook da un error de cómputo o de cuota, espera a que termine el otro.

**Si falla**
- *Se queda en **Not Ready** mucho tiempo o aparece `UPDATE_FAILED`:* el notebook se detiene con
  `El endpoint vuelos-retrasos no pudo desplegar la nueva configuración (UPDATE_FAILED)`. Ve a **Serving** > `vuelos-retrasos` y abre las
  pestañas **Events** y **Logs**. Lo más probable es un problema de dependencias (versión de `mlflow`, `scikit-learn`,
  `numpy` o `cloudpickle` que no existe o no coincide). Copia el log de construcción completo y úsalo con el
  prompt de la última sección.
- *El log de construcción menciona un conflicto de `starlette` o `fastapi`:* en `notebooks/_modelo.py`, en la función
  `requisitos_pip()`, quita la línea `requisitos.append("starlette<1")` (cámbiala por `pass`, porque está dentro de un `if` y borrarla sin más deja el `if` vacío y da error de sintaxis).
  Vuelve a correr `prueba_de_humo` (crea una versión nueva) y luego `05_publicar_endpoint`.
- *Al contrario: el log dice `'FastAPI' object has no attribute 'route'`:* falta ese tope. El modelo se registró **sin** `starlette<1`;
  revisa la línea `Librerías que instalará el endpoint:` de `prueba_de_humo` (debería incluirlo con tu versión de MLflow). Si no lo trae, agrega `starlette<1`
  a la lista `requisitos` de `requisitos_pip()`, y repite `prueba_de_humo` y `05_publicar_endpoint`.
- *`RuntimeError: No se pudieron obtener credenciales para la API de Databricks`:* pega el mensaje completo a la IA con el prompt de la última sección (no inventes un token ni lo pegues en el código).
- *`No se pudo crear el endpoint: HTTP 400/429 ... Parece un límite de Free Edition`:* es el límite de endpoints o de cuota. Borra endpoints que no uses en **Serving**; si no hay ninguno, Plan B.
- *`El endpoint tiene otra actualización en curso`:* solo espera, el notebook sigue esperando solo.
- *Error al crear: "límite de endpoints", "not supported", "feature not available" o similar:* Free Edition
  limita los endpoints. Pásate al **Plan B** (sección al final) sin perder más tiempo.
- *El notebook dice `FALLARON n de 9 pruebas del endpoint`:* la lista imprime el nombre de cada prueba, qué se esperaba y qué llegó. Pégala en la consulta a la IA. Una falla en las pruebas de entrada inválida es importante: es el
  criterio de "validación de entradas".
- *Una prueba da 503/504 la primera vez:* el endpoint estaba despertando. El notebook reintenta hasta 4 veces cada 15 segundos (`intento 1: 504 -> se reintenta en 15 s`); si aun así falla, vuelve a correr el notebook.

---

## Paso 3 · Bronce, plata y oro (45 a 90 min, mientras el endpoint se construye)

**Qué vas a hacer.** Cargar los datos a tablas Delta y limpiarlos. Esta es la parte del criterio 1 (35 puntos).

**Clics exactos** (puedes hacerlos en otra pestaña del navegador; un notebook a la vez, esperando a que cada uno termine; recuerda que cada notebook que corre consume cuota de cómputo):

1. `notebooks/01_bronce` > **Serverless** > **Run all**.
2. `notebooks/02_plata` > **Serverless** > **Run all**.
3. `notebooks/03_oro` > **Serverless** > **Run all**.

No corras el siguiente hasta que el anterior imprima su mensaje final.

**Qué deberías ver**

| Notebook | Mensaje final | Otras señales |
|---|---|---|
| `01_bronce` | `OK: la tabla bronce cumple el mínimo de 4,000,000 de registros.` | 12 zips cargados; **cerca de 7 millones de filas** (aprox.); `Columnas en bronce: 112 (109 de la fuente + 3 de trazabilidad)` |
| `02_plata` | `OK: capa plata escrita en <catálogo>.vuelos.plata_vuelos (... filas; ... % de bronce)` | La bitácora tiene **9 renglones**; plata conserva normalmente **más del 99 %** de bronce; "Valores no convertibles en total" debería ser 0 |
| `03_oro` | `OK: capa oro escrita. <catálogo>.vuelos.oro_vuelos_modelo tiene ... filas y ...% de retrasos.` | **Cerca de 7 millones de filas** (aprox.); entre **18 y 25 %** de vuelos retrasados (aprox.); cero nulos |

- [ ] Bronce, plata y oro terminaron con su mensaje `OK`.
- [ ] En **Catalog** > `workspace` > `vuelos` ven las tablas `bronce_vuelos`, `plata_vuelos`, `plata_bitacora_calidad`,
  `oro_vuelos_modelo` y los cuatro `oro_resumen_*`.

**Qué guardar para el README (hazlo ahora, después se pierde en el scroll)**
- [ ] En `01_bronce`: copia el número de filas de bronce y toma una **captura** de la tabla por archivo.
- [ ] En `02_plata`: la **última celda imprime la bitácora de calidad ya en formato de tabla Markdown**.
  Cópiala completa (de `| # | Paso |...` hasta el último renglón). Va pegada en el README.
- [ ] En `03_oro`: copia el porcentaje de retrasados y toma una captura de la tabla de balance de clases.

**Si falla**
- *`AssertionError: ... Hay N filas y el proyecto pide al menos 4,000,000` (bronce):* faltan meses.
  Revisa la verificación del notebook 00 y sube los zips que falten; luego vuelve a correr `01_bronce`
  (es incremental: solo carga los zips nuevos y no duplica).
- *Error de `CAST` o de "ANSI" (por ejemplo `CAST_INVALID_INPUT`, `DIVIDE_BY_ZERO`):* el cómputo serverless usa modo
  ANSI y no perdona conversiones inválidas. El código ya usa `try_cast` y `try_divide`; si aparece
  uno, copia el error y la celda a la IA. No lo "arregles" quitando la validación.
- *`input_file_name` o `_metadata` no soportado:* mismo trato: pásalo a la IA con la celda.
- *El notebook se queda mucho tiempo en una celda:* son 7 millones de filas, plata puede tardar varios minutos.
  Si pasan más de 30 min sin avance, deténlo y revisa que el cómputo no se haya apagado.
- *Aviso de cuota ("compute quota exceeded" o el cómputo se apaga):* ver la fila correspondiente en "Errores frecuentes".
  No sigas corriendo notebooks; reanuda mañana.
- *`AssertionError` en plata ("Quedaron nulos en columnas críticas", etc.):* hay un caso que las pruebas locales
  no vieron. Copia el mensaje y la celda a la IA; es información valiosa porque es un dato real inesperado.
- *`01_bronce` dice `AVISO: N mes(es) aparecen en más de un archivo`:* subiste el mismo mes dos veces con nombres
  distintos. Plata elimina esos duplicados por sí sola, pero lo limpio es borrar el zip repetido del volumen y empezar bronce de cero.

---

## Paso 4 · La aplicación Streamlit contra el endpoint de prueba (30 a 45 min)

**Qué vas a hacer.** Crear la app en Databricks Apps, darle permiso de consultar el endpoint y comprobar
que muestra predicciones y maneja errores. Vale 30 puntos (criterio 4). Usa el endpoint de prueba: cuando
en el Paso 5 lo actualices al modelo real, **la app sigue funcionando sin cambios**.

**Clics exactos**

1. Abre el selector de aplicaciones/menú de Databricks (el área **Compute** > **Apps**; la documentación describe
   ambas rutas) y entra a **Databricks Apps**.
2. Pulsa **+ Create app** > **Create a custom app**.
3. Nombre: `vuelos-app` (solo minúsculas, números y guiones; **no se puede renombrar después**).
4. En **App resources**: **+ Add resource** > **Model** (serving endpoint) > selecciona `vuelos-retrasos`.
   Permiso: **Can query**. En la clave del recurso (resource key) **cambia `model` por `serving-endpoint`**.
   Debe coincidir exactamente con el `valueFrom` de `app/app.yaml`.
5. Pulsa **Create app**. **Crear no despliega.**
6. Abre la app > **Deploy** > pestaña **From workspace** > en **Source code path** elige la carpeta `app` dentro
   de tu Git folder > **Deploy**.
7. Cuando el estado sea corriendo, abre la URL de la app.

**Qué deberías ver**
- [ ] La app abre (te pide iniciar sesión en tu workspace: las apps no pueden ser públicas).
- [ ] Con los valores por defecto (DL, ATL a LAX), al pulsar **Predecir retraso** aparece una probabilidad entre 0 y 1 y
  un veredicto de retraso (con el modelo de prueba el valor no tiene sentido real; solo importa que responda).
  Esa combinación sí está en los datos sintéticos de `prueba_de_humo` (DL entre las aerolíneas; ATL y LAX entre los 17 aeropuertos).
- [ ] Las 8 rutas de ejemplo de la app funcionan con el modelo de prueba. **Si escribes un aeropuerto fuera de esos 17
  (por ejemplo `HNL`), la API contesta 400 ("no es un aeropuerto conocido por el modelo") y la app muestra ese mensaje.**
  Es lo correcto, no un bug; con el modelo real (Paso 5) se aceptan todos los aeropuertos de los datos.
- [ ] Pruebas de falla: en la barra lateral de la app, el selector **Modo demostración de fallas** tiene cinco modos.
  Pruébalos todos y pulsa **Predecir retraso** en cada uno:
  - [ ] **Normal**: muestra la probabilidad y el veredicto.
  - [ ] **Tiempo de espera agotado**: aviso de que el endpoint tardó demasiado, sin predicción.
  - [ ] **Error HTTP: la API rechaza la entrada**: envía mes=13; la app muestra el mensaje real del HTTP 400.
  - [ ] **Endpoint inexistente (404)**: aviso de que el endpoint no existe. Ojo: si Databricks contesta 403 para un endpoint que no existe,
    la app mostrará en su lugar el aviso de permisos ("falta el permiso Can query"). Cualquiera de los dos es correcto: lo que importa es que se muestre un aviso y no un traceback.
  - [ ] **Respuesta inválida**: es **simulada** (la app reemplaza la respuesta por una con probabilidad 1.7); avisa que el servicio contestó algo inesperado y no muestra predicción.
  - [ ] Ningún error muestra un traceback de Python.
- [ ] La pestaña **Documentación de la API** abre y el botón **Obtener el esquema OpenAPI del endpoint** trae el esquema.
- [ ] Toma una captura de cada uno de estos casos (van al README y son tu respaldo el viernes).

**Si falla**
- *La app abre pero dice que falta la variable de entorno (`SERVING_ENDPOINT`):* la clave del recurso no
  coincide. En la página de la app > **App resources** > revisa que la clave sea exactamente `serving-endpoint`
  (editarla puede requerir quitar el recurso y agregarlo otra vez) y vuelve a pulsar **Deploy**.
- *403 o "sin permiso" al consultar:* el recurso no se agregó, o el permiso no es **Can query**. Corrígelo en
  **App resources** y redespliega. (La app consulta con **su propia identidad**, un service principal; tu
  usuario no cuenta.)
- *404:* el endpoint no se llama `vuelos-retrasos`. Revisa el nombre en **Serving**.
- *504, "tiempo agotado" o "el servicio está despertando" en la primera consulta:* arranque en frío del
  endpoint. Espera medio minuto y vuelve a pulsar; el cliente ya reintenta solo.
- *No se ve nada o la app no arranca:* abre la pestaña **Logs** de la app, o agrega `/logz` a la URL de la app.
  Copia el error al asistente de IA con el prompt de la última sección.
- *El estado es "Stopped" o "Crashed":* revisa **Logs**; si solo se detuvo, pulsa **Start**. Las apps se
  detienen solas 24 horas después de iniciarse o redesplegarse.

---

## Paso 5 · Entrenar el modelo real y actualizar el endpoint (día 2; 30 min + espera)

**Qué vas a hacer.** Entrenar con los 7 millones de vuelos reales (tablas oro), evaluar con F1 sobre días
que el modelo nunca vio y registrar la versión nueva con el alias `campeon`. Luego actualizar el endpoint.
Criterio 2 (10 puntos) y mejora el 3.

**Clics exactos**

1. `notebooks/04_entrenamiento` > **Serverless** > **Run all**.
2. Cuando termine, `notebooks/05_publicar_endpoint` > **Serverless** > **Run all** (de nuevo).

**Qué deberías ver**
- `04_entrenamiento` imprime por el camino: `Muestra de entrenamiento: ... filas | muestra de prueba: ... filas`,
  `OK: ningún día se repite entre ajuste, validación y prueba.`, una línea por modelo (`logistica: entrenado con ... filas ... | F1 validación = ...`),
  `Modelo elegido: <logistica o gradiente> con umbral ...`, la tabla de métricas de prueba y dos gráficas.
  Termina con `OK: modelo registrado: <catálogo>.vuelos.modelo_retrasos versión <n> (alias campeon)`
  y luego imprime un bloque en Markdown que empieza con `RESUMEN PARA EL README` (entre dos líneas de guiones).
  **Cópialo completo** (sin las líneas de guiones): trae las métricas y va pegado en el README (sección 2).
  Si ves `AVISO: un F1 tan alto sin datos del clima es sospechoso` hay una posible fuga de información; si ves `AVISO: el modelo no supera a la línea base`, no lo publiques sin revisar.
- `05_publicar_endpoint` lee el alias y la línea `Acción:` debe decir `actualizado` (de 10 a 25 minutos; verás las mismas líneas `[mm:ss] ready=... | config_update=... | versiones servidas=...`).
  Igual que en el Paso 2B, **no dejes el notebook esperando** (cuota de cómputo): en cuanto imprima `Acción: actualizado`, cancela la celda,
  espera a que **Serving** > `vuelos-retrasos` diga **Ready** con la versión nueva (mientras tanto el endpoint anterior sigue respondiendo; ocupa ese tiempo en el README), y corre `05` otra vez: imprime
  `Acción: sin cambios`, repite las 9 pruebas y guarda el OpenAPI (si el workspace lo permite; si imprime `AVISO: no se pudo obtener el esquema OpenAPI`, se queda el `docs/openapi.json` base del repo). Termina con `OK: endpoint vuelos-retrasos listo, sirviendo la versión <n>` (la `<n>` es mayor que la de la
  prueba de humo) y `OK: las 9 pruebas del endpoint pasaron`. Si dice `Acción: sin cambios`, el endpoint ya servía esa versión.
- [ ] La app, **sin tocarla**, ahora da predicciones del modelo real. Prueba una consulta conocida (por ejemplo
  ATL a LAX, 1,947 millas, 285 minutos) y toma captura.

**Si falla**
- *Error de memoria o el notebook se queda muy lento:* en la celda de configuración de `04_entrenamiento` baja las constantes
  `FILAS_ENTRENAMIENTO` (800,000), `FILAS_PRUEBA` (300,000) y `FILAS_LOGISTICA` (200,000), por ejemplo a la mitad cada una, y vuelve a correr.
  No cambies `FRACCION_VALIDACION` ni la separación por días. Si sigue fallando, copia el error a la IA con el prompt de la última sección.
  (Menos filas en el README es válido: el requisito de 4 millones es del pipeline de datos, no del entrenamiento; di cuántas usaste.)
- *El endpoint se queda en **Not Ready** tras actualizar:* el endpoint anterior sigue sirviendo mientras la actualización
  corre. Revisa **Events** y **Logs**; si aparece `UPDATE_FAILED`, el endpoint viejo sigue vivo (la app no se cae).
  Pega el log a la IA.
- *F1 muy bajo o parecido a 0:* es posible con clases desbalanceadas si el umbral quedó mal. El notebook elige el umbral
  que maximiza F1 en validación; si el número te parece raro, no lo maquilles: explica en el README lo que obtuviste.
- *Corriste `05` y sigue sirviendo la versión de prueba:* el alias `campeon` no apunta a la versión nueva. Revisa
  en **Catalog** > `workspace` > `vuelos` > `modelo_retrasos` que la versión nueva tenga el alias `campeon`. La primera línea de `05`
  (`Alias  : campeon -> versión <n>`) te dice qué versión va a publicar.
- *`05` falla con "Pasaron 45 minutos..." (`TimeoutError`):* la actualización puede seguir en Databricks; mira **Serving** > `vuelos-retrasos` y, cuando diga **Ready**, vuelve a correr `05`.

---

## Paso 6 · El Job de Lakeflow: bronce, plata y oro encadenados (20 min)

**Qué vas a hacer.** Convertir los tres notebooks de datos en un pipeline orquestado con horario. Es lo que
el profesor llama "pipelines o jobs".

**Clics exactos**

1. Barra lateral > **Jobs & Pipelines** > **Create** > **Job**.
2. Nombre del job: `vuelos-medallion`.
3. Primera tarea: nombre `bronce`, **Type** = **Notebook**, **Source** = **Workspace**, **Path** = el notebook
   `01_bronce` de tu carpeta > **Save task**. (El cómputo es serverless por defecto; déjalo así.)
4. **Add task** > **Notebook**: nombre `plata`, Path = `02_plata`, **Depends on** = `bronce` > **Save task**.
5. **Add task** > **Notebook**: nombre `oro`, Path = `03_oro`, **Depends on** = `plata` > **Save task**.
6. En el panel de detalles del job: **Schedules & Triggers** > **Add trigger** > **Trigger type** = **Scheduled**.
   Elige una frecuencia **baja: semanal** (por ejemplo, los lunes a las 3:00).
7. Pulsa **Run now** una vez.

**Qué deberías ver**
- [ ] El grafo del job con tres cajas encadenadas: `bronce` > `plata` > `oro`.
- [ ] Una ejecución (en la pestaña **Runs**) con las tres tareas en verde. Como bronce es incremental y los
  zips ya están cargados, esa tarea debe ser rápida; plata y oro vuelven a procesar todo (varios minutos).
- [ ] Captura del grafo del job y de la ejecución exitosa.

**Después de la demo, pausa el horario** (en el job: **Schedules & Triggers** > **Pause**). Cada corrida gasta
cuota de cómputo diaria y no tiene caso gastarla.

**Si falla**
- *Una tarea no encuentra `./_config` (error de `%run`):* el notebook se está ejecutando desde una ruta distinta a
  su carpeta. Verifica que el **Path** apunte al notebook dentro de la misma carpeta donde está `_config`.
- *La tarea falla con una cuota o un error de cómputo:* revisa la fila de cuota en "Errores frecuentes". Si no hay cuota hoy, déjalo para
  mañana; el job ya está configurado, y para la entrega basta con tener una ejecución exitosa en el historial.
- *No encuentras **Jobs & Pipelines**:* busca **Workflows** o **Jobs** en la barra lateral; el nombre cambió varias veces.

---

## Paso 7 · README, capturas, commit y push (60 a 90 min)

**Qué vas a hacer.** Completar el README (es lo único que el profesor revisa en el repositorio) y subir todo.

**Clics exactos**

1. En tu editor, abre `README.md` y busca todos los marcadores con el texto `<<`:
   - Windows PowerShell: `Select-String -Path README.md -Pattern "<<"`
   - Editor: usa buscar (Ctrl+F) con `<<`.
2. Reemplaza cada `<<PEGAR: ...>>` con el texto o la tabla que dice y cada `<<CAPTURA: ...>>` con una imagen
   (guarda las capturas en una carpeta `docs/capturas/` y enlázalas así: `![descripción](docs/capturas/nombre.png)`).
   Reemplaza `<<TU NOMBRE>>`.
3. Revisa `docs/openapi.json`. El repo ya trae una **versión base** escrita a mano a partir de la firma del modelo, válida como documentación del contrato.
   El notebook 05 la reemplaza por el esquema en vivo **solo si** el workspace puede devolverlo (si imprimió `AVISO: no se pudo obtener el esquema OpenAPI`,
   se queda la versión base y no hay nada que hacer). Cuando sí lo devolvió, lo guarda siempre en el volumen
   (`/Volumes/<catálogo>/vuelos/crudo/docs/openapi.json`) y **solo** lo escribe en `docs/openapi.json` del repositorio si corrió dentro del
   Git folder (imprime `Guardado en el repositorio: ...`). Casos:
   - Si lo guardó en el Git folder: en Databricks, abre el Git folder > menú de la rama > **Commit & Push** (marca `docs/openapi.json`), y luego `git pull` en tu laptop.
   - Si no (corriste los notebooks importados a mano, o imprimió `No encontré la carpeta docs del repositorio...`): en **Catalog** >
     `workspace` > `vuelos` > `crudo` > carpeta `docs`, abre `openapi.json` y descárgalo (**Download**); guárdalo en tu laptop como `docs/openapi.json` dentro del proyecto.
   Para mostrarlo como Swagger en la presentación, abre https://editor.swagger.io y pega el contenido de `docs/openapi.json`.
4. **Borra del README la sección completa "Pendientes antes de entregar"** (desde su encabezado `## Pendientes antes de entregar` hasta justo antes de `## Arquitectura`).
   Es tu lista de trabajo: sus renglones contienen `<<` y no debe llegar al profesor.
5. Verifica que ya no quedan marcadores (esta revisión solo puede salir vacía **después** del paso 4):
   `Select-String -Path README.md -Pattern "<<"` debe regresar **nada**.
6. Sube todo:
   ```
   git add .
   git commit -m "README completo con resultados y capturas"
   git push
   ```
7. Abre tu repo en GitHub y revisa que el README se vea bien, **en especial el diagrama Mermaid** y las tablas.

- [ ] Borré la sección "Pendientes antes de entregar" y no queda ningún `<<` en el README (ni `<<PEGAR`, `<<CAPTURA` ni `<<TU NOMBRE>>`).
- [ ] Los números del README son **los de tu corrida**, no los de esta guía.
- [ ] El repo es público y abre sin iniciar sesión.
- [ ] Mi nombre está en el README y el repo no contiene credenciales ni tokens (`git grep -i token` no muestra secretos).

**Si falla**
- *El diagrama Mermaid sale como texto:* revisa que el bloque empiece con tres comillas invertidas y la palabra `mermaid`, sin espacios.
- *`git push` rechazado:* haz `git pull --rebase` y vuelve a hacer `git push`.
- *Cambiaste cosas en Databricks (Git folder) y también en la laptop:* en el Git folder de Databricks usa el menú de la
  rama > **Pull**/**Commit & Push** y resuelve conflictos antes de subir. Si te enredas, la regla simple es: edita el README solo en la laptop.

---

## Checklist del día de la presentación (viernes 20)

Estos puntos salen de los límites verificados de Free Edition. Hazlos **en este orden**.

- [ ] **Reinicia la app esa mañana.** Las apps se detienen solas 24 horas después de su último inicio o despliegue.
  Si la desplegaste el miércoles o jueves, ya está apagada: abre la app > **Start** (o **Deploy** otra vez) y verifica que abra.
- [ ] **Calienta el endpoint dos veces.** Escala a cero tras 30 minutos sin uso; la primera consulta puede tardar de
  segundos a minutos o devolver 504. Manda una consulta de calentamiento desde la app (o **Serving** > `vuelos-retrasos` > **Query endpoint**)
  unos **10 minutos antes** de empezar, y **otra justo antes** de presentar. Mientras hablas, la app reintenta sola si hay un 504.
- [ ] **No vuelvas a correr el pipeline completo el viernes.** Pasarte de la cuota de cómputo apaga el cómputo el
  resto del día y no hay forma de remediarlo en vivo. Muestra las **tablas ya creadas** (Catalog) y el **Job ya ejecutado** (pestaña Runs).
- [ ] **Ten capturas de respaldo de cada pantalla** (tablas, bitácora, endpoint **Ready**, Swagger/OpenAPI, app con
  predicción, app con error, Job). Si algo no carga, enséñalas sin perder el ritmo.
- [ ] **Muestra la app desde tu propia pantalla.** No es pública: quien la abra debe iniciar sesión en tu workspace.
  Tampoco se la mandes al profesor como liga para que la abra solo.
- [ ] Sesión de Databricks iniciada, pestañas abiertas de antemano: README en GitHub, Catalog (tablas), Serving, la app, el Job.
- [ ] Pausaste el horario semanal del Job (o lo vas a hacer después de la demo).
- [ ] La liga del repo de GitHub está lista para entregar. El README se ve bien (diagrama y tablas).
- [ ] Zoom del navegador al 125 % para que se lea en el proyector.

---

## Guion de la demo (5 a 7 minutos)

Mapa por criterio de la rúbrica; los tiempos son una guía.

| Min | Qué muestras | Qué dices | Criterio |
|---|---|---|---|
| 0:00 a 0:45 | README: título y diagrama de arquitectura | "Predecimos si un vuelo llegará con 15 minutos o más de retraso usando solo datos conocidos antes del despegue. 7 millones de vuelos de 2024, arquitectura medallion, modelo publicado como endpoint, app que lo consulta." | Contexto, requisito de >4 M registros |
| 0:45 a 2:15 | Catalog: tablas bronce, plata (y bitácora), oro. Job ya ejecutado | "Bronce guarda todo crudo. Plata tipa, trata nulos según su significado, quita duplicados y valores imposibles; los retrasos extremos no se borran, se acotan, porque son la clase que queremos predecir. Cada decisión queda en la bitácora. Oro deja la tabla del modelo, separada por días completos para evitar fuga." Muestra el Job: "Tres tareas encadenadas con horario." | 1 (35 pts) |
| 2:15 a 3:00 | Resumen de `04_entrenamiento` (o la sección 2 del README) | "Es clasificación binaria con clases desbalanceadas, por eso la métrica principal es F1 de la clase retrasado y no la exactitud. Resultado: <tu F1 real>. La prueba se hizo con días que el modelo nunca vio." | 2 (10 pts) |
| 3:00 a 4:15 | **Serving** > `vuelos-retrasos` (Ready) y el OpenAPI/Swagger | "El modelo está registrado en Unity Catalog y servido en un endpoint REST. Tiene contrato documentado en OpenAPI y dos capas de validación: la firma de MLflow y reglas de negocio. Por ejemplo, mes 13 devuelve un error 400 con mensaje claro." Muestra la tabla de pruebas del notebook 05. | 3 (25 pts) |
| 4:15 a 6:15 | La app: una consulta normal, luego los modos "Error HTTP" y "Tiempo de espera agotado" (di que "Respuesta inválida" es simulada) | "La app no tiene el modelo: llama al endpoint por REST con su propia identidad. Si el endpoint tarda, reintenta con espera creciente; si la respuesta viene mal formada, avisa y no inventa una predicción." | 4 (30 pts) |
| 6:15 a 7:00 | Limitaciones y cierre | "Sin datos de clima, un solo año, límites de Free Edition, app no pública. Siguiente paso: agregar clima y reentrenar con más años." | Honestidad / preguntas |

- [ ] Ensáyalo completo una vez con cronómetro el jueves en la noche o el viernes temprano.
- [ ] Prepara una respuesta corta para "¿por qué no eliminaron los outliers?" y "¿por qué F1 y no accuracy?".

---

## Plan B: si Model Serving no está disponible

Úsalo **solo** si no se puede crear el endpoint en Free Edition (error de límite o "not supported" al crear
`vuelos-retrasos`, confirmado en **Events**/**Logs** o en el mensaje del notebook 05). Se publica el modelo como una API
**FastAPI** dentro de una segunda Databricks App. FastAPI trae Swagger automático en `/docs` (cumple "API
documentada") y el contrato es el mismo, así que la app Streamlit casi no cambia.

**Clics exactos**

1. Si tu catálogo **no** se llama `workspace`, edita `api_fastapi/app.yaml` y cambia el valor de `MODEL_URI`
   (`models:/<tu_catálogo>.vuelos.modelo_retrasos@campeon`); haz commit, push y **Pull** en el Git folder.
2. Crea una segunda app: **Databricks Apps** > **+ Create app** > **Create a custom app** > nombre `vuelos-api`.
   Esta app **no** usa el recurso del endpoint: carga el modelo directo desde Unity Catalog con su propia identidad.
   Por eso su service principal necesita leer el modelo: en **Catalog** > `workspace` > `vuelos` > `modelo_retrasos` > **Permissions**,
   dale `EXECUTE` (y `USE CATALOG` / `USE SCHEMA` si lo pide). Si el log dice "permission denied" al cargar el modelo, es esto.
3. **Create app** > **Deploy** > **From workspace** > **Source code path** = la carpeta `api_fastapi` de tu Git folder > **Deploy**.
4. Abre la URL de la app y entra a `/docs` (ahí está Swagger). Prueba `POST` con un ejemplo del contrato.
5. Dale a la app Streamlit permiso de usar la API: abre la app `vuelos-api` > **Permissions** (o **Share**) > agrega el
   **service principal** de `vuelos-app` (lo ves en la pestaña de detalles de `vuelos-app`, con un nombre parecido a `app-xxxx vuelos-app`) con
   permiso **Can use**.
6. En `app/app.yaml` quita el `#` de las dos últimas líneas (`- name: "URL_BASE_API"` y `value: ...`) y pon como valor la URL de
   `vuelos-api` (sin diagonal final). Haz commit y push, **Pull** en el Git folder y vuelve a pulsar **Deploy** en `vuelos-app`.
   La app mostrará en la barra lateral un aviso "Modo plan B". Si el recurso del endpoint no existe, puedes quitarlo de la app;
   `SERVING_ENDPOINT` cae por defecto a `vuelos-retrasos`.

**Qué deberías ver**: la app Streamlit mostrando predicciones, ahora a través de la API FastAPI. En el README,
cambia la sección 3 para describir esta ruta: "API REST con FastAPI en Databricks Apps, con Swagger en `/docs`" (sigue cumpliendo
el criterio: "API REST (FastAPI, Flask)... documentada y con validación de entradas").

**Si falla**
- *403 al llamar a `vuelos-api` desde la app:* faltó el permiso **Can use** al service principal correcto, o la app
  consulta sin el encabezado de autenticación. Pega el log de ambas apps a la IA.
- *Tienes ya 3 apps (el máximo):* borra una app que no uses antes de crear otra.
- *La API arranca pero no encuentra el modelo:* revisa que exista la versión con alias `campeon` (Paso 2 o 5).

---

## Errores frecuentes y cómo resolverlos

| Síntoma | Causa probable | Qué hacer |
|---|---|---|
| La descarga del notebook 00 falla (`mes 01: FALLO ...`) | Free Edition bloquea casi toda la salida a internet. **Es lo esperado.** | Descarga los zips en tu laptop (Paso 1B) y súbelos a mano (Paso 1C) |
| El catálogo no se llama `workspace` | Tu workspace usa otro nombre | No pasa nada: `_config` lo detecta solo. Usa el nombre que imprime el notebook 00 al subir archivos |
| `CREATE SCHEMA` con permiso denegado | Tu usuario no puede crear esquemas en ese catálogo | En `notebooks/_config.py` cambia `ESQUEMA = "vuelos"` por `"default"` y vuelve a correr desde el notebook 00 |
| `AssertionError: ... menos de 4,000,000` (bronce) | Faltan meses en el volumen | Corre la verificación del 00, sube los zips faltantes, vuelve a correr `01_bronce` (incremental) |
| Error de `CAST` / ANSI (`CAST_INVALID_INPUT`, `DIVIDE_BY_ZERO`) | Serverless usa modo ANSI: un texto inválido detiene el notebook | El código usa `try_cast` y `try_divide`. Si aparece uno, manda el error y la celda a la IA. No quites validaciones |
| Endpoint en **Not Ready** mucho tiempo o `UPDATE_FAILED` | Probable problema de dependencias (versiones de pip) al construir la imagen | **Serving** > `vuelos-retrasos` > **Events** y **Logs**; pega el log de construcción a la IA |
| Error al crear el endpoint: límite o "not supported" | Free Edition limita los endpoints, o no permite este tipo | Pasa al **Plan B** |
| 403 desde la app | El endpoint no está agregado como recurso, o el permiso no es **Can query** | App > **App resources**: agrega `vuelos-retrasos` con **Can query**; **Deploy** de nuevo |
| La app dice que falta la variable de entorno | La clave del recurso no es `serving-endpoint` (la de fábrica es `model`) | Corrige la clave para que coincida con `valueFrom` en `app/app.yaml`; **Deploy** |
| La app no abre o aparece como detenida | Las apps se detienen 24 h después del último inicio | Abre la app > **Start** o **Deploy** otra vez |
| 504 o tiempo agotado en la primera consulta | El endpoint estaba dormido (escala a cero tras 30 min) | Espera 30 a 60 s y reintenta; la app ya reintenta sola. Calienta antes de presentar |
| El cómputo se apaga o dice que excediste la cuota | La cuota diaria de Free Edition se agotó (no está publicado el número) | Los datos no se borran. Espera al día siguiente. No repitas cargas completas. Muestra tablas y capturas |
| `ModuleNotFoundError` o error de `pip` al correr un notebook | Versión de entorno distinta a la esperada | Copia el error al asistente de IA con el prompt de la sección siguiente |
| `%run ./_config` no encuentra el archivo | Los notebooks no están en la misma carpeta | Ponlos todos juntos (Git folder o Import en la misma carpeta) |

---

## Cómo pedir ayuda a otro modelo de IA

Cuando algo falle, no describas el error de memoria: pásalo completo. Usa esta plantilla (copia y pega,
llenando los huecos entre `[ ]`), y **adjunta el archivo `docs/CONTEXTO_TECNICO.md`** (tiene nombres, contrato de la API y límites verificados de Free Edition).

```
Estoy haciendo un proyecto final de Big Data en Databricks Free Edition (solo cómputo serverless, Unity Catalog).
Adjunto CONTEXTO_TECNICO.md con la arquitectura, los nombres y los límites verificados del entorno.
Soy analista de datos: sé Python y SQL, pero soy nuevo en Databricks. Explícame con pasos concretos.

Dónde falla:
- Notebook o archivo: [por ejemplo 02_plata]
- Celda: [número o el título de la sección]
- Paso de la guía: [por ejemplo Paso 3]

Error completo (pégalo entero, de la primera a la última línea, sin resumir):
[pega aquí el traceback o el log de la pestaña Events/Logs]

Qué esperaba que pasara:
[por ejemplo "debía imprimir OK: capa plata escrita"]

Qué ya intenté:
[por ejemplo "volví a correr la celda y falla igual"]

Reglas para tu respuesta: no cambies los nombres ni el contrato de la API del documento adjunto;
si propones cambiar código, dime el archivo, la celda y el texto exacto que reemplaza; dime cómo
comprobar que quedó resuelto; y si no estás seguro de un nombre de menú de Databricks, dilo.
```

Consejos:
- Pega el error **completo**: la línea importante suele estar al principio o al final del traceback.
- Si el error es del endpoint, pega el contenido de las pestañas **Events** y **Logs**, no solo la palabra "falló".
- Si el modelo te propone quitar una validación o un `try_cast` para "que ya corra", desconfía: eso rompe lo que se califica.
- Nunca pegues tokens, contraseñas ni claves en la conversación con la IA.
