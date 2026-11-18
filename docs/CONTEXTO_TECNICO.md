# Contexto técnico del proyecto

Este documento es la referencia única del proyecto: qué se construye, con qué nombres, qué contrato
tiene la API y qué límites del entorno ya se verificaron. Si pides ayuda a otra persona (o a un
asistente de IA) para resolver un error, compártele este archivo junto con el mensaje de error.

## 1. Qué es el proyecto

**"¿Se retrasará mi vuelo?"**: predice si un vuelo comercial doméstico de EE.UU. llegará con 15
minutos o más de retraso, usando solo datos que se conocen antes del despegue.

| Pieza | Decisión |
|---|---|
| Datos | BTS "Reporting Carrier On-Time Performance", 12 meses de 2024, ~7 millones de vuelos |
| Plataforma | Databricks **Free Edition** (solo cómputo serverless, Unity Catalog) |
| Procesamiento | PySpark en notebooks, arquitectura medallion (bronce, plata, oro) en tablas Delta |
| Orquestación | Un Job de Lakeflow con 3 tareas encadenadas (bronce, plata, oro) y horario |
| Tarea de ML | Clasificación binaria desbalanceada. Métrica principal: **F1 de la clase "retrasado"** |
| Modelo | Pipeline de scikit-learn (escalado + codificación + clasificador) envuelto en un `mlflow.pyfunc` que valida entradas |
| Publicación | Modelo registrado en Unity Catalog y servido en un endpoint de **Model Serving** (REST, CPU, escala a cero) |
| Aplicación | Streamlit en **Databricks Apps**; consume el endpoint por REST, con manejo de errores, tiempos de espera y respuestas inválidas |
| Plan B | Si Model Serving no estuviera disponible: API FastAPI (Swagger en `/docs`) como segunda Databricks App, con el mismo contrato |

## 2. Estructura del repositorio

```
proyecto-final-vuelos/
  README.md                     explicación del proyecto (lo que se califica)
  GUIA.md                       paso a paso para montarlo en Databricks
  docs/CONTEXTO_TECNICO.md      este archivo
  docs/openapi.json             esquema OpenAPI del endpoint (lo genera el notebook 05)
  datos/descargar_zips.ps1      descarga los 12 zips del BTS a tu computadora
  notebooks/
    _config.py                  nombres y constantes (lo cargan todos con %run)
    _modelo.py                  clase del modelo, pipeline y funciones del endpoint (se carga con %run)
    00_preparar_entorno.py      crea esquema y volumen; intenta descargar los datos
    01_bronce.py                ingesta cruda a Delta
    02_plata.py                 limpieza con bitácora de calidad
    03_oro.py                   tabla para el modelo y tablas de resumen
    04_entrenamiento.py         entrena, evalúa, registra el modelo en Unity Catalog
    05_publicar_endpoint.py     crea o actualiza el endpoint, lo prueba y guarda el OpenAPI
    prueba_de_humo.py           registra un modelo de prueba (datos sintéticos) para validar el endpoint el día 1
  app/                          aplicación Streamlit (Databricks Apps)
    app.py, cliente_endpoint.py, app.yaml, requirements.txt
  api_fastapi/                  plan B: API FastAPI con Swagger
    main.py, app.yaml, requirements.txt
  pruebas/                      pruebas automáticas (pytest) del cliente, la app y la API del plan B; ver pruebas/README.md
```

Los notebooks están en **formato fuente de Databricks**: archivos `.py` cuya primera línea es
`# Databricks notebook source`, con celdas separadas por `# COMMAND ----------` y Markdown en líneas
`# MAGIC %md`. Databricks los abre como notebooks.

## 3. Nombres (definidos en `notebooks/_config.py`)

| Constante | Valor |
|---|---|
| `CATALOGO` | el catálogo por defecto del workspace (`SELECT current_catalog()`); en Free Edition normalmente `workspace` |
| `ESQUEMA` | `vuelos` |
| Volumen | `/Volumes/<catalogo>/vuelos/crudo` con subcarpetas `zips/` y `csv/` |
| `T_BRONCE` | `<catalogo>.vuelos.bronce_vuelos` |
| `T_PLATA`, `T_BITACORA` | `<catalogo>.vuelos.plata_vuelos`, `<catalogo>.vuelos.plata_bitacora_calidad` |
| `T_ORO_MODELO` | `<catalogo>.vuelos.oro_vuelos_modelo` |
| `T_ORO_AEROLINEA`, `T_ORO_RUTA`, `T_ORO_HORA`, `T_ORO_MES` | tablas de resumen `oro_resumen_*` |
| `NOMBRE_MODELO` | `<catalogo>.vuelos.modelo_retrasos` |
| `ALIAS_MODELO` | `campeon` (la versión que se publica) |
| `NOMBRE_ENDPOINT` | `vuelos-retrasos` |
| `COLUMNAS_ENTERAS` | `mes`, `dia_semana`, `hora_salida_prog`, `hora_llegada_prog` |
| `COLUMNAS_DECIMALES` | `distancia_millas`, `duracion_prog_min` |
| `COLUMNAS_TEXTO` | `aerolinea`, `origen`, `destino` |
| `COLUMNAS_MODELO` | las 9 anteriores, en ese orden |
| `ETIQUETA` | `retrasado` (1 = llegó 15 min o más tarde) |

## 4. Datos de origen (verificado contra el archivo real de julio de 2024)

* Liga por mes: `https://transtats.bts.gov/PREZIP/On_Time_Reporting_Carrier_On_Time_Performance_1987_present_2024_<mes>.zip`
  (`<mes>` de 1 a 12, sin cero a la izquierda). Sin login. Cada zip pesa 25 a 35 MB y trae un CSV de ~285 MB.
* El CSV tiene 109 columnas con nombre más una columna vacía al final (cada línea termina en coma;
  Spark la nombra `_c109`). Unas 630 mil filas por mes.
* Todo llega como texto: horas en formato `HHMM` (`0700`), números con decimales (`1.00`, `636.00`),
  vacío cuando el evento no ocurrió.
* `ArrDel15` (la etiqueta) viene vacía exactamente en los vuelos cancelados o desviados.
* `DayOfWeek`: 1 = lunes ... 7 = domingo (igual que `date.isoweekday()` de Python).

## 5. Capas medallion (ya escritas y probadas localmente)

* **Bronce**: todo como `STRING` más `_archivo_zip`, `_ruta_origen`, `_fecha_ingesta`. Carga incremental
  (no recarga zips ya cargados). Exige al menos 4,000,000 de filas.
* **Plata**: 33 columnas tipadas con `try_cast`; nulos tratados por significado; duplicados exactos y
  por llave; 7 reglas de dominio para valores imposibles; retrasos extremos acotados (no eliminados).
  Cada paso se registra en `plata_bitacora_calidad` con filas antes/después y justificación.
* **Oro**: `oro_vuelos_modelo` (un renglón por vuelo operado, 9 variables + `retrasado` + `conjunto`),
  más 4 tablas de resumen. La columna `conjunto` vale `prueba` para 1 de cada 5 **días** (hash de la
  fecha) y `entrenamiento` para el resto: la separación es por días completos para evitar fuga.

Reglas de dominio de plata (la API del modelo aplica las mismas a cada consulta):

| Variable | Regla |
|---|---|
| `mes` | 1 a 12 |
| `dia_semana` | 1 a 7 |
| `hora_salida_prog`, `hora_llegada_prog` | 0 a 23 |
| `distancia_millas` | mayor que 0 y hasta 6,000 |
| `duracion_prog_min` | 15 a 900 |
| velocidad implícita (`distancia / (duracion / 60)`) | 40 a 700 mph |
| `origen`, `destino` | distintos entre sí |

## 6. Contrato de la API del modelo

**Petición**

```
POST https://<host-del-workspace>/serving-endpoints/vuelos-retrasos/invocations
Authorization: Bearer <token>
Content-Type: application/json

{
  "dataframe_split": {
    "columns": ["mes", "dia_semana", "hora_salida_prog", "hora_llegada_prog",
                "distancia_millas", "duracion_prog_min", "aerolinea", "origen", "destino"],
    "data": [[7, 5, 18, 21, 1947.0, 285.0, "DL", "ATL", "LAX"]]
  }
}
```

También acepta `{"dataframe_records": [{"mes": 7, ...}]}`. Máximo 1,000 filas por petición.

| Campo | Tipo en la firma | Validación |
|---|---|---|
| `mes`, `dia_semana`, `hora_salida_prog`, `hora_llegada_prog` | `long` (entero) | rangos de la tabla anterior |
| `distancia_millas`, `duracion_prog_min` | `double` | rangos de la tabla anterior y velocidad implícita |
| `aerolinea` | `string` | código presente en los datos de entrenamiento (por ejemplo `AA`, `DL`, `UA`, `WN`) |
| `origen`, `destino` | `string` | código de aeropuerto presente en los datos de entrenamiento; distintos entre sí |

Hay dos niveles de validación:
1. **Firma de MLflow** (la aplica el servidor antes de llamar al modelo): columnas faltantes o tipos
   incompatibles se rechazan.
2. **Reglas de negocio** (dentro de `ModeloRetrasos.predict`): rangos, categorías conocidas y coherencia.
   Los textos se normalizan (sin espacios, en mayúsculas) antes de validar. Si algo falla se lanza
   `MlflowException(..., error_code=INVALID_PARAMETER_VALUE)` con un mensaje como
   `Entrada inválida: fila 0: mes=13 fuera de rango (1-12)`.

**Respuesta correcta (HTTP 200)**

```json
{"predictions": [{"probabilidad_retraso": 0.3124, "retraso_predicho": 0, "umbral_decision": 0.41}]}
```

* `probabilidad_retraso`: número entre 0 y 1.
* `retraso_predicho`: 1 si `probabilidad_retraso >= umbral_decision`, si no 0.
* `umbral_decision`: el umbral que maximizó F1 en validación (constante por versión del modelo).

**Errores que el cliente debe manejar**

| Caso | Qué llega | Qué hace la app |
|---|---|---|
| Entrada inválida | HTTP 400 con JSON `{"error_code": ..., "message": ...}` | Muestra la parte legible del mensaje de la API (ver abajo); no reintenta |
| Sin permiso | HTTP 401 o 403 | Explica que falta el permiso "Can query"; no reintenta |
| Endpoint inexistente | HTTP 404 | Explica que el nombre del endpoint no existe; no reintenta |
| Saturación | HTTP 429 | Reintenta con espera creciente |
| Endpoint despertando o caído | HTTP 500, 502, 503 o 504 | Reintenta con espera creciente; si se agotan los intentos, avisa |
| Tiempo de espera | `requests.Timeout` | Reintenta; si se agota, avisa |
| Sin conexión | `requests.ConnectionError` | Reintenta; si se agota, avisa |
| Respuesta inválida | 200 pero no es JSON, falta `predictions`, o la probabilidad no es un número entre 0 y 1 | Avisa que el servicio contestó algo inesperado; no muestra predicción |

**Cómo llega un error 400 (observado con el servidor real de MLflow 3.8.1, en local)**

El servidor de MLflow **envuelve** el mensaje del modelo. Una regla de negocio violada llega así:

```json
{"error_code": "INVALID_PARAMETER_VALUE",
 "message": "Failed to predict data '<copia de los datos enviados>'. \nError: Entrada inválida: fila 0: mes=13 fuera de rango (1-12)"}
```

Por eso el cliente no muestra `message` tal cual. Regla para sacar el texto útil (la usan la app y el notebook 05):
si el mensaje contiene `Entrada inválida:`, se muestra desde ahí; si no, y empieza con `Failed to predict data`,
se muestra lo que sigue al último `Error: `; en cualquier otro caso se muestra completo.

| Petición | Código | `error_code` | Mensaje |
|---|---|---|---|
| Regla de negocio (mes 13, aerolínea o aeropuerto desconocido, origen = destino, velocidad imposible, valor faltante, más de 1,000 filas) | 400 | `INVALID_PARAMETER_VALUE` | envuelto; termina en `Entrada inválida: fila N: ...` (lista hasta 10 problemas) |
| Falta una columna | 400 | `INVALID_PARAMETER_VALUE` | envuelto; termina en `Model is missing inputs ['destino'].` |
| Tipo incompatible (`mes="julio"`, `mes=null`) | 400 | `BAD_REQUEST` | en inglés, de la firma: `Failed to convert column mes to type 'int64'` |
| JSON mal formado o clave desconocida | 400 | `BAD_REQUEST` | en inglés |

Conversiones que el servidor acepta sin error: enteros en las columnas `double`; `"7"` o `7.0` en una columna
entera; columnas de más (se ignoran); textos en minúsculas o con espacios (el modelo los normaliza).
Ojo: `mes=7.5` **se trunca a 7** antes de llegar al modelo. La app siempre manda enteros.

Falta confirmar en Databricks que Model Serving devuelve este mismo cuerpo (el notebook 05 imprime el cuerpo
real en su sección 4.1). El cliente también acepta un mensaje sin envolver.

## 7. Databricks Free Edition: límites verificados en la documentación (noviembre de 2026)

Fuente principal: https://docs.databricks.com/aws/en/getting-started/free-edition-limitations

* Solo cómputo **serverless**; no hay clústeres configurables. Sin R ni Scala.
* **Cuota**: si se excede, el cómputo se apaga por el resto del día (en casos extremos, del mes). Los
  datos no se borran. La documentación no publica el número. No repitas cargas completas sin necesidad.
* **Salida a internet** restringida a una lista corta de dominios que no está publicada. PyPI sí
  aparece reportado como alcanzable. Asume que `transtats.bts.gov` está bloqueado: los zips se suben a mano.
* **Model Serving**: "límites en el número de endpoints activos" (sin número publicado), sin GPU, sin
  throughput aprovisionado. La documentación no dice de forma explícita que los modelos propios en CPU
  estén permitidos; por eso el primer paso de la guía es una prueba de humo.
* **Databricks Apps**: máximo 3 apps; cada app se detiene sola 24 horas después de iniciarse o
  redesplegarse (se puede volver a iniciar). Las apps no pueden ser públicas: quien la abra debe iniciar sesión.
* **Jobs**: máximo 5 tareas concurrentes. **SQL warehouse**: uno, tamaño 2X-Small.

Serverless (https://docs.databricks.com/aws/en/compute/serverless/limitations):

* **Modo ANSI activo**: un `CAST` de texto inválido o una división entre cero detienen el notebook.
  Usar `try_cast` y `try_divide`.
* `input_file_name()` no funciona con Unity Catalog; usar `_metadata.file_path`, seleccionándola en el
  mismo `select` de la lectura.
* Sin RDD ni `spark.sparkContext`. `spark.conf.set` solo admite 6 propiedades.
* Volúmenes: sin escritura aleatoria ni en modo "append" (afecta escribir zips); por eso los zips se
  descomprimen en disco local y el CSV se copia al volumen con `shutil.copyfile`.
* Subida por la interfaz: hasta 5 GB por archivo.

Librerías preinstaladas en serverless según la versión de entorno (la de Free Edition no está documentada):

| Versión de entorno | mlflow-skinny | scikit-learn | databricks-sdk | Python |
|---|---|---|---|---|
| 6 | 3.12.0 | 1.7.2 | 0.122.0 | 3.12.3 |
| 5 | 3.8.1 | 1.6.1 | 0.67.0 | 3.12.3 |
| 4 | 2.22.0 | 1.6.1 | 0.49.0 | 3.12.3 |

Consecuencias para el código:

* Solo viene **`mlflow-skinny`**. Al registrar el modelo hay que pasar `pip_requirements` explícitos con
  `mlflow==<versión>` (no `mlflow-skinny`), más `scikit-learn`, `pandas`, `numpy` y `cloudpickle` con las
  versiones del notebook. Si no, la imagen del endpoint puede fallar al construirse.
* `requisitos_pip()` (en `_modelo.py`) agrega además `starlette<1` **solo** cuando el servidor del MLflow
  instalado es FastAPI y todavía usa `app.route(` (caso de MLflow 3.8.1): sin ese tope, `pip` instala
  Starlette 1.x y el servidor de MLflow no arranca. Si la construcción del endpoint fallara por un conflicto
  de `starlette`, borra esa línea de `requisitos_pip()` y vuelve a registrar el modelo. Al registrar, el
  notebook imprime la lista final ("Librerías que instalará el endpoint").
* Cargar el modelo con `mlflow.pyfunc.load_model` **dentro del mismo proceso** (plan B) es más estricto que
  el servidor HTTP: rechaza enteros en las columnas `double`. La API FastAPI convierte esas dos columnas a
  decimal antes de llamar a `predict`.
* `mlflow.pyfunc.log_model` usa `name=` en MLflow 3 y `artifact_path=` en MLflow 2: el código detecta
  cuál existe.
* El SDK de Databricks cambió la firma de `EndpointCoreConfigInput` entre versiones: para crear,
  actualizar y consultar el endpoint se usa la **API REST con `requests`** (independiente de la versión).
* Los modelos en Unity Catalog **exigen firma** (`signature`).

Model Serving:

* Crear: `POST /api/2.0/serving-endpoints` con `{"name", "config": {"served_entities": [{"entity_name", "entity_version", "workload_size": "Small", "scale_to_zero_enabled": true}]}}`.
* Actualizar: `PUT /api/2.0/serving-endpoints/<nombre>/config` con `{"served_entities": [...]}`.
* Estado: `GET /api/2.0/serving-endpoints/<nombre>` y revisar `state.ready` (`READY`) y `state.config_update` (`NOT_UPDATING`, `IN_PROGRESS`, `UPDATE_FAILED`).
* OpenAPI: `GET /api/2.0/serving-endpoints/<nombre>/openapi` (requiere firma y estado `READY`).
* Tarda unos 10 minutos en quedar listo la primera vez; una actualización, de 10 a 25 minutos.
* Con escala a cero se duerme tras 30 minutos sin uso. Despertar suele tomar 10 a 20 segundos, a veces
  minutos; la primera petición puede devolver 504. Por eso el cliente reintenta.
* Límites: 16 MB por petición, 597 segundos de ejecución.

Databricks Apps (https://docs.databricks.com/aws/en/dev-tools/databricks-apps/):

* Entorno: Python 3.11, con `streamlit` 1.38.0 y `databricks-sdk` 0.33.0 preinstalados (`requests` llega
  como dependencia del SDK). El código de la app debe funcionar con Streamlit 1.38.
* `app.yaml`: `command: ["streamlit", "run", "app.py"]` y variables en `env` (`name` + `value` o `valueFrom`).
* El endpoint se agrega como **recurso** de la app con permiso **Can query**. La clave del recurso por
  defecto es `model`; en este proyecto se usa `serving-endpoint` y `app.yaml` la lee con
  `valueFrom: "serving-endpoint"` hacia la variable `SERVING_ENDPOINT` (su valor es el nombre del endpoint).
* Autenticación dentro de la app: `from databricks.sdk.core import Config; cfg = Config()`, luego
  `cfg.host` y `cfg.authenticate()` (encabezados nuevos en cada llamada). No usar la variable
  `DATABRICKS_HOST` directamente (puede venir sin `https://`).
* Streamlit no necesita banderas de puerto: la plataforma las define.
* Registros de la app: pestaña **Logs**, o agregar `/logz` a la URL de la app.

## 8. Qué está probado y qué no

| Parte | Estado |
|---|---|
| Notebooks 01, 02 y 03 | Corridos localmente con Spark 3.5 en modo ANSI sobre 61,536 vuelos reales de julio de 2024 más errores sembrados (500 duplicados exactos, 1 duplicado por llave, 2 nulos críticos, 1 valor no convertible, 6 violaciones de reglas). Todos se detectaron y contaron bien. |
| Lo mismo en Databricks | **No probado.** Diferencias conocidas del entorno local: tablas simuladas con vistas temporales (sin Delta ni Unity Catalog) y carpeta local en vez de volumen. |
| Descarga y nombres de columnas | Verificados contra el archivo real del BTS. |
| Model Serving y Apps en Free Edition | Basado en documentación, sin ejecutar en un workspace. |
