# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Preparar el entorno y conseguir los datos
# MAGIC
# MAGIC **Qué hace este notebook**
# MAGIC 1. Crea el esquema `vuelos` (dentro del catálogo de tu workspace, normalmente `workspace`) y el
# MAGIC    volumen `crudo`, donde viven los archivos crudos.
# MAGIC 2. Intenta descargar los 12 archivos mensuales de 2024 directo desde el BTS (Bureau of Transportation Statistics).
# MAGIC 3. Revisa qué meses ya están en el volumen y te dice cuáles faltan.
# MAGIC
# MAGIC **Importante:** Databricks Free Edition restringe la salida a internet a una lista corta de dominios.
# MAGIC Si la descarga directa falla es **normal**: descarga los zips en tu computadora y súbelos a mano
# MAGIC (la guía trae las ligas y los clics). Después vuelve a correr la última celda para verificar.
# MAGIC
# MAGIC **Qué deberías ver al final:** una tabla con 12 archivos `.zip` de entre 20 y 40 MB cada uno y el
# MAGIC mensaje `LISTO: están los 12 meses`.

# COMMAND ----------

# MAGIC %run ./_config

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Crear esquema, volumen y carpetas

# COMMAND ----------

import os

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOGO}.{ESQUEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOGO}.{ESQUEMA}.{VOLUMEN}")

os.makedirs(RUTA_ZIPS, exist_ok=True)
os.makedirs(RUTA_CSV, exist_ok=True)

print("Esquema :", f"{CATALOGO}.{ESQUEMA}")
print("Volumen :", RUTA_VOLUMEN)
print("Sube los zips a:", RUTA_ZIPS)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Intento de descarga directa (puede fallar en Free Edition, y no pasa nada)

# COMMAND ----------

import shutil
import urllib.request


def nombre_zip(anio, mes):
    return f"vuelos_{anio}_{mes:02d}.zip"


def descargar(anio, mes, segundos=120):
    """Descarga un mes al volumen. Devuelve (ok, mensaje)."""
    url = URL_BTS.format(anio=anio, mes=mes)
    destino = os.path.join(RUTA_ZIPS, nombre_zip(anio, mes))
    if os.path.exists(destino) and os.path.getsize(destino) > 1_000_000:
        return True, "ya estaba"
    try:
        peticion = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(peticion, timeout=segundos) as resp, open(destino, "wb") as salida:
            shutil.copyfileobj(resp, salida, length=4 * 1024 * 1024)
        return True, f"descargado ({os.path.getsize(destino) / 1e6:.1f} MB)"
    except Exception as error:  # red bloqueada, tiempo agotado, etc.
        if os.path.exists(destino):
            os.remove(destino)  # no dejar archivos a medias
        return False, f"{type(error).__name__}: {error}"


resultados = []
for mes in MESES_DATOS:
    ok, mensaje = descargar(ANIO_DATOS, mes, segundos=30 if not resultados else 120)
    resultados.append((mes, ok, mensaje))
    print(f"mes {mes:02d}: {'OK ' if ok else 'FALLO'} {mensaje}")
    if not ok and mes == MESES_DATOS[0]:
        print("\nLa descarga directa no está permitida desde este workspace.")
        print("No hay problema: descarga los zips en tu computadora y súbelos al volumen (ver la guía).")
        break

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Verificación: ¿qué meses hay en el volumen?
# MAGIC
# MAGIC Vuelve a correr **solo esta celda** después de subir archivos a mano.
# MAGIC Los nombres no importan (pueden ser los originales del BTS): lo que cuenta es que sean 12 zips distintos.

# COMMAND ----------

import zipfile

filas = []
for nombre in sorted(os.listdir(RUTA_ZIPS)):
    ruta = os.path.join(RUTA_ZIPS, nombre)
    if not nombre.lower().endswith(".zip"):
        continue
    try:
        with zipfile.ZipFile(ruta) as z:
            csvs = [n for n in z.namelist() if n.lower().endswith(".csv")]
        estado = "ok" if len(csvs) == 1 else f"revisar: trae {len(csvs)} csv"
        contenido = csvs[0] if csvs else ""
    except zipfile.BadZipFile:
        estado, contenido = "ZIP DAÑADO: vuelve a subirlo", ""
    except Exception as error:
        estado, contenido = f"no se pudo abrir ({type(error).__name__})", ""
    filas.append((nombre, round(os.path.getsize(ruta) / 1e6, 1), estado, contenido))

if filas:
    display(spark.createDataFrame(filas, "archivo STRING, tamano_mb DOUBLE, estado STRING, csv_dentro STRING"))

buenos = [f for f in filas if f[2] == "ok"]
print(f"Zips válidos en {RUTA_ZIPS}: {len(buenos)} de {len(MESES_DATOS)} esperados")
if len(buenos) >= len(MESES_DATOS):
    print("LISTO: están los 12 meses. Sigue con el notebook 01_bronce.")
elif len(buenos) >= 8:
    print("Con 8 meses o más ya se rebasan los 4 millones de registros, pero lo ideal es subir los 12.")
else:
    print("FALTAN ARCHIVOS: sube los zips que faltan y vuelve a correr esta celda.")
