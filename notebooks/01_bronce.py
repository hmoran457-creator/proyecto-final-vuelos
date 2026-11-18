# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Capa BRONCE — ingesta de los datos crudos
# MAGIC
# MAGIC **Objetivo:** pasar los archivos del BTS a una tabla Delta **tal como vienen**, sin limpiar nada.
# MAGIC La capa bronce es la copia fiel de la fuente: si después cambia una regla de limpieza, se puede
# MAGIC reconstruir todo desde aquí sin volver a descargar.
# MAGIC
# MAGIC **Decisiones de esta capa**
# MAGIC | Decisión | Por qué |
# MAGIC |---|---|
# MAGIC | Todas las columnas se guardan como texto (`STRING`) | Bronce no interpreta los datos; los tipos se corrigen y se justifican en plata. Así ningún valor raro se pierde en la carga. |
# MAGIC | Se agregan `_archivo_zip`, `_ruta_origen` y `_fecha_ingesta` | Trazabilidad: de qué archivo salió cada fila y cuándo se cargó. |
# MAGIC | Carga incremental: un zip que ya se cargó no se vuelve a cargar | El notebook se puede correr varias veces (o programar como job) sin duplicar filas. |
# MAGIC | Se descarta la columna vacía del final | Cada línea del CSV termina en coma, lo que crea una columna 110 sin nombre y sin datos. |
# MAGIC
# MAGIC **Qué deberías ver al final:** 12 archivos cargados, cerca de **7 millones de filas** y el mensaje
# MAGIC `OK: la tabla bronce cumple el mínimo de 4,000,000 de registros`.

# COMMAND ----------

# MAGIC %run ./_config

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. ¿Qué archivos hay y cuáles faltan por cargar?

# COMMAND ----------

import os
import shutil
import tempfile
import zipfile

from pyspark.sql import functions as F

zips = sorted(n for n in os.listdir(RUTA_ZIPS) if n.lower().endswith(".zip"))
assert zips, f"No hay archivos .zip en {RUTA_ZIPS}. Corre primero 00_preparar_entorno y sube los datos."

ya_cargados = set()
if spark.catalog.tableExists(T_BRONCE):
    ya_cargados = {f["_archivo_zip"] for f in spark.table(T_BRONCE).select("_archivo_zip").distinct().collect()}

pendientes = [z for z in zips if z not in ya_cargados]
print(f"Zips en el volumen : {len(zips)}")
print(f"Ya cargados        : {len(ya_cargados)}")
print(f"Pendientes         : {len(pendientes)} -> {pendientes}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Descomprimir y cargar cada archivo
# MAGIC
# MAGIC Spark no lee `.zip` directamente, así que cada archivo se descomprime al volumen, se carga a la
# MAGIC tabla y el CSV temporal se borra (cada uno pesa unos 280 MB; no tiene caso guardarlos dos veces).

# COMMAND ----------

os.makedirs(RUTA_CSV, exist_ok=True)


def extraer_csv(ruta_zip, ruta_csv):
    """Saca el único CSV del zip y lo escribe de corrido (escritura secuencial) en ruta_csv."""
    with zipfile.ZipFile(ruta_zip) as z:
        internos = [n for n in z.namelist() if n.lower().endswith(".csv")]
        assert len(internos) == 1, f"{os.path.basename(ruta_zip)}: se esperaba 1 CSV y trae {len(internos)}"
        with z.open(internos[0]) as origen, open(ruta_csv, "wb") as destino:
            shutil.copyfileobj(origen, destino, length=16 * 1024 * 1024)


def descomprimir_al_volumen(ruta_zip, ruta_csv):
    """Descomprime en disco local y copia el CSV al volumen (lo que recomienda Databricks para zips).

    Si el disco local del cómputo no se puede usar, descomprime directo de volumen a volumen: leer un zip
    y escribir un archivo de corrido sí está permitido en los volúmenes.
    """
    carpeta_local = None
    try:
        carpeta_local = tempfile.mkdtemp(prefix="vuelos_")
        zip_local = os.path.join(carpeta_local, "datos.zip")
        csv_local = os.path.join(carpeta_local, "datos.csv")
        shutil.copyfile(ruta_zip, zip_local)
        extraer_csv(zip_local, csv_local)
        shutil.copyfile(csv_local, ruta_csv)
    except OSError as error:
        print(f"   aviso: no se pudo usar el disco local ({type(error).__name__}: {error}); "
              "se descomprime directo en el volumen")
        extraer_csv(ruta_zip, ruta_csv)
    finally:
        if carpeta_local:
            shutil.rmtree(carpeta_local, ignore_errors=True)


for nombre_zip_actual in pendientes:
    ruta_zip = os.path.join(RUTA_ZIPS, nombre_zip_actual)
    ruta_csv = os.path.join(RUTA_CSV, nombre_zip_actual[:-4] + ".csv")

    # 2.1 Descomprimir el CSV al volumen
    descomprimir_al_volumen(ruta_zip, ruta_csv)

    # 2.2 Leer el CSV como texto, sin inferir tipos
    crudo = (
        spark.read.option("header", True)
        .option("inferSchema", False)
        .csv(ruta_csv)
        .select("*", F.col("_metadata.file_path").alias("_ruta_origen"))
    )

    # 2.3 Quitar la columna vacía del final (Spark la nombra _c109) y agregar metadatos de ingesta
    columnas_fuente = [c for c in crudo.columns if not c.startswith("_c")]
    bronce_mes = (
        crudo.select(*columnas_fuente)
        .withColumn("_archivo_zip", F.lit(nombre_zip_actual))
        .withColumn("_fecha_ingesta", F.current_timestamp())
    )

    # 2.4 Agregar a la tabla bronce
    bronce_mes.write.mode("append").option("mergeSchema", "true").saveAsTable(T_BRONCE)
    os.remove(ruta_csv)
    print(f"cargado: {nombre_zip_actual}")

print("Carga terminada.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Verificación de la capa bronce

# COMMAND ----------

bronce = spark.table(T_BRONCE)

por_archivo = (
    bronce.groupBy("_archivo_zip")
    .agg(
        F.count("*").alias("filas"),
        F.min("FlightDate").alias("fecha_min"),
        F.max("FlightDate").alias("fecha_max"),
    )
    .orderBy("fecha_min")
)
display(por_archivo)

n_bronce = bronce.count()
n_columnas = len(bronce.columns)
print(f"Filas en bronce   : {n_bronce:,}")
print(f"Columnas en bronce: {n_columnas} (109 de la fuente + 3 de trazabilidad)")

# Un mismo mes subido dos veces con nombres distintos duplicaría filas: se avisa aquí y se corrige en plata.
meses_repetidos = (
    bronce.select("Year", "Month", "_archivo_zip").distinct().groupBy("Year", "Month").count().filter("count > 1").count()
)
if meses_repetidos:
    print(f"AVISO: {meses_repetidos} mes(es) aparecen en más de un archivo. La capa plata elimina esos duplicados.")

assert n_bronce >= MINIMO_REGISTROS, (
    f"Hay {n_bronce:,} filas y el proyecto pide al menos {MINIMO_REGISTROS:,}. Sube más meses y vuelve a correr."
)
print(f"OK: la tabla bronce cumple el mínimo de {MINIMO_REGISTROS:,} de registros.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Vistazo a los datos crudos
# MAGIC
# MAGIC Fíjate en los problemas que la capa plata va a resolver: horas como texto `0700`, números con
# MAGIC `.00`, vacíos en los vuelos cancelados y más de 100 columnas que no se necesitan.

# COMMAND ----------

display(
    bronce.select(
        "FlightDate", "Reporting_Airline", "Flight_Number_Reporting_Airline", "Origin", "Dest",
        "CRSDepTime", "DepTime", "ArrDelay", "ArrDel15", "Cancelled", "Diverted", "Distance", "_archivo_zip",
    ).limit(20)
)
