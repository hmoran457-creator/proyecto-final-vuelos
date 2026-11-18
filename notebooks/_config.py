# Databricks notebook source
# MAGIC %md
# MAGIC # `_config` — nombres y constantes compartidas
# MAGIC
# MAGIC Este notebook **no se corre solo**. Los demás lo cargan con `%run ./_config` para que
# MAGIC todos usen exactamente los mismos nombres de catálogo, tablas, modelo y endpoint.
# MAGIC
# MAGIC Si quieres cambiar un nombre (por ejemplo el esquema), cámbialo **solo aquí**.

# COMMAND ----------

# --- Unity Catalog -----------------------------------------------------------
# En Databricks Free Edition el catálogo por defecto es el del propio workspace (normalmente se
# llama "workspace"). Se detecta solo; escribe un nombre en CATALOGO_FIJO únicamente si quieres otro.
CATALOGO_FIJO = None
CATALOGO = CATALOGO_FIJO or spark.sql("SELECT current_catalog()").first()[0]
ESQUEMA = "vuelos"
VOLUMEN = "crudo"

# --- Rutas del volumen (archivos crudos) -------------------------------------
RUTA_VOLUMEN = f"/Volumes/{CATALOGO}/{ESQUEMA}/{VOLUMEN}"
RUTA_ZIPS = f"{RUTA_VOLUMEN}/zips"  # aquí se suben los .zip del BTS
RUTA_CSV = f"{RUTA_VOLUMEN}/csv"    # carpeta temporal donde se descomprimen

# --- Tablas (arquitectura medallion) -----------------------------------------
T_BRONCE = f"{CATALOGO}.{ESQUEMA}.bronce_vuelos"
T_PLATA = f"{CATALOGO}.{ESQUEMA}.plata_vuelos"
T_BITACORA = f"{CATALOGO}.{ESQUEMA}.plata_bitacora_calidad"
T_ORO_MODELO = f"{CATALOGO}.{ESQUEMA}.oro_vuelos_modelo"
T_ORO_AEROLINEA = f"{CATALOGO}.{ESQUEMA}.oro_resumen_aerolinea"
T_ORO_RUTA = f"{CATALOGO}.{ESQUEMA}.oro_resumen_ruta"
T_ORO_HORA = f"{CATALOGO}.{ESQUEMA}.oro_resumen_hora"
T_ORO_MES = f"{CATALOGO}.{ESQUEMA}.oro_resumen_mes"

# --- Modelo y servicio --------------------------------------------------------
NOMBRE_MODELO = f"{CATALOGO}.{ESQUEMA}.modelo_retrasos"  # modelo registrado en Unity Catalog
ALIAS_MODELO = "campeon"                                 # alias de la versión que se publica
NOMBRE_ENDPOINT = "vuelos-retrasos"                      # endpoint de Model Serving

# --- Datos de origen ----------------------------------------------------------
ANIO_DATOS = 2024
MESES_DATOS = list(range(1, 13))
URL_BTS = (
    "https://transtats.bts.gov/PREZIP/"
    "On_Time_Reporting_Carrier_On_Time_Performance_1987_present_{anio}_{mes}.zip"
)
MINIMO_REGISTROS = 4_000_000  # requisito del proyecto

# --- Variables del modelo -----------------------------------------------------
# Solo se usan datos que se conocen ANTES de que el vuelo salga (sin fuga de información).
COLUMNAS_ENTERAS = ["mes", "dia_semana", "hora_salida_prog", "hora_llegada_prog"]
COLUMNAS_DECIMALES = ["distancia_millas", "duracion_prog_min"]
COLUMNAS_TEXTO = ["aerolinea", "origen", "destino"]
COLUMNAS_MODELO = COLUMNAS_ENTERAS + COLUMNAS_DECIMALES + COLUMNAS_TEXTO
ETIQUETA = "retrasado"  # 1 = llegó con 15 minutos o más de retraso

# Nombres legibles de las aerolíneas que reportan al BTS (para tablas y gráficas).
AEROLINEAS = {
    "9E": "Endeavor Air",
    "AA": "American Airlines",
    "AS": "Alaska Airlines",
    "B6": "JetBlue",
    "DL": "Delta Air Lines",
    "F9": "Frontier",
    "G4": "Allegiant",
    "HA": "Hawaiian",
    "MQ": "Envoy Air",
    "NK": "Spirit",
    "OH": "PSA Airlines",
    "OO": "SkyWest",
    "UA": "United",
    "WN": "Southwest",
    "YX": "Republic",
}

print(f"Configuración cargada: catálogo={CATALOGO}, esquema={ESQUEMA}, endpoint={NOMBRE_ENDPOINT}")
