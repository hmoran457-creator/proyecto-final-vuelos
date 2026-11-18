# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Capa ORO — tablas listas para el modelo y para el análisis
# MAGIC
# MAGIC **Objetivo:** dejar tablas que ya responden a una necesidad concreta, sin más limpieza por hacer.
# MAGIC
# MAGIC | Tabla | Para qué sirve |
# MAGIC |---|---|
# MAGIC | `oro_vuelos_modelo` | Un renglón por vuelo operado, solo con las variables del modelo y la etiqueta. De aquí se entrena. |
# MAGIC | `oro_resumen_aerolinea`, `oro_resumen_ruta`, `oro_resumen_hora`, `oro_resumen_mes` | Agregados para tableros y para explicar los resultados. |
# MAGIC
# MAGIC **Decisiones de esta capa**
# MAGIC | Decisión | Por qué |
# MAGIC |---|---|
# MAGIC | Se excluyen vuelos cancelados y desviados de la tabla del modelo | No tienen retraso de llegada: la etiqueta no existe. |
# MAGIC | Solo entran variables que se conocen **antes** del despegue | Evita la fuga de información. El retraso de salida predice muy bien el de llegada, pero no se conoce cuando alguien consulta su vuelo con días de anticipación. |
# MAGIC | El día del mes no entra como variable | Con un solo año de datos el modelo memorizaría días específicos (una tormenta, una caída de sistemas) que no se repiten. |
# MAGIC | La separación entrenamiento/prueba es por **días completos** | Los vuelos de un mismo día comparten clima y saturación. Si se mezclaran al azar, el modelo vería "pistas" del día de prueba y la métrica saldría inflada. |
# MAGIC
# MAGIC **Qué deberías ver al final:** cerca de 7 millones de filas en `oro_vuelos_modelo`, entre 18 y 25 %
# MAGIC de vuelos retrasados y el mensaje `OK: capa oro escrita`.

# COMMAND ----------

# MAGIC %run ./_config

# COMMAND ----------

from pyspark.sql import functions as F

plata = spark.table(T_PLATA)
n_plata = plata.count()
print(f"Filas en plata: {n_plata:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Tabla para el modelo

# COMMAND ----------

operados = plata.filter(
    (F.col("cancelado") == 0) & (F.col("desviado") == 0) & F.col("retrasado_15").isNotNull()
)

# 1 de cada 5 días (elegidos con un hash de la fecha, siempre los mismos) se aparta para la prueba final.
es_dia_de_prueba = F.pmod(F.crc32(F.col("fecha_vuelo").cast("string")), F.lit(5)) == 0

oro_modelo = operados.select(
    F.col("fecha_vuelo"),
    *[F.col(c).cast("bigint").alias(c) for c in COLUMNAS_ENTERAS],
    *[F.col(c).cast("double").alias(c) for c in COLUMNAS_DECIMALES],
    *[F.col(c) for c in COLUMNAS_TEXTO],
    F.col("retrasado_15").cast("int").alias(ETIQUETA),
    F.when(es_dia_de_prueba, F.lit("prueba")).otherwise(F.lit("entrenamiento")).alias("conjunto"),
)

oro_modelo.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(T_ORO_MODELO)

oro_modelo = spark.table(T_ORO_MODELO)
n_oro = oro_modelo.count()
print(f"Filas en {T_ORO_MODELO}: {n_oro:,} (se excluyeron {n_plata - n_oro:,} vuelos cancelados o desviados)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.1 Balance de clases
# MAGIC Este resultado es el que justifica la métrica: si solo 1 de cada 5 vuelos se retrasa, un modelo que
# MAGIC diga siempre "a tiempo" tendría cerca de 80 % de exactitud **sin servir para nada**. Por eso la
# MAGIC métrica principal es **F1 de la clase "retrasado"** y no la exactitud.

# COMMAND ----------

balance = (
    oro_modelo.groupBy("conjunto")
    .agg(
        F.count("*").alias("vuelos"),
        F.countDistinct("fecha_vuelo").alias("dias"),
        F.sum(ETIQUETA).alias("retrasados"),
        F.round(100 * F.avg(ETIQUETA), 2).alias("pct_retrasados"),
    )
    .orderBy("conjunto")
)
display(balance)

pct_total = oro_modelo.agg(F.avg(ETIQUETA)).first()[0] * 100
print(f"Vuelos retrasados (15 min o más) en todo el conjunto: {pct_total:.2f} %")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Tablas de resumen
# MAGIC Los promedios de minutos usan la columna acotada de plata (`retraso_llegada_min_acotado`) para que
# MAGIC unos pocos retrasos extremos no distorsionen el promedio.

# COMMAND ----------

def resumen(df, *llaves):
    return df.groupBy(*llaves).agg(
        F.count("*").alias("vuelos"),
        F.round(100 * F.avg("cancelado"), 2).alias("pct_cancelados"),
        F.round(100 * F.avg("retrasado_15"), 2).alias("pct_retrasados"),
        F.round(F.avg("retraso_llegada_min_acotado"), 1).alias("retraso_promedio_min"),
    )


nombres = spark.createDataFrame(list(AEROLINEAS.items()), "aerolinea STRING, aerolinea_nombre STRING")

resumen_aerolinea = (
    resumen(plata, "aerolinea")
    .join(nombres, "aerolinea", "left")
    .withColumn("aerolinea_nombre", F.coalesce("aerolinea_nombre", "aerolinea"))
    .orderBy(F.desc("pct_retrasados"))
)
resumen_aerolinea.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(T_ORO_AEROLINEA)

resumen_ruta = (
    plata.groupBy("origen", "destino", "ruta")
    .agg(
        F.count("*").alias("vuelos"),
        F.round(100 * F.avg("retrasado_15"), 2).alias("pct_retrasados"),
        F.round(F.avg("retraso_llegada_min_acotado"), 1).alias("retraso_promedio_min"),
        F.percentile_approx("distancia_millas", 0.5).alias("distancia_millas"),
        F.percentile_approx("duracion_prog_min", 0.5).alias("duracion_prog_min"),
    )
    .orderBy(F.desc("vuelos"))
)
resumen_ruta.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(T_ORO_RUTA)

resumen(plata, "hora_salida_prog").orderBy("hora_salida_prog").write.mode("overwrite").option(
    "overwriteSchema", "true"
).saveAsTable(T_ORO_HORA)

resumen(plata, "mes").orderBy("mes").write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(T_ORO_MES)

print("Tablas de resumen escritas.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.1 ¿Qué aerolíneas se retrasan más?

# COMMAND ----------

display(spark.table(T_ORO_AEROLINEA))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.2 ¿A qué hora conviene volar?
# MAGIC Los retrasos se acumulan a lo largo del día: un avión que se atrasa en la mañana arrastra sus vuelos siguientes.

# COMMAND ----------

display(spark.table(T_ORO_HORA).orderBy("hora_salida_prog"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.3 Las 20 rutas con más vuelos
# MAGIC Sirve también como referencia de distancia y duración para probar la aplicación.

# COMMAND ----------

display(spark.table(T_ORO_RUTA).orderBy(F.desc("vuelos")).limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Verificación

# COMMAND ----------

nulos_modelo = oro_modelo.select(
    *[F.sum(F.col(c).isNull().cast("int")).alias(c) for c in COLUMNAS_MODELO + [ETIQUETA]]
).first().asDict()
total_nulos = sum(v or 0 for v in nulos_modelo.values())

print(f"Filas para el modelo        : {n_oro:,}")
print(f"Nulos en variables o etiqueta: {total_nulos}")
print(f"Aerolíneas distintas        : {oro_modelo.select('aerolinea').distinct().count()}")
print(f"Aeropuertos de origen       : {oro_modelo.select('origen').distinct().count()}")

assert total_nulos == 0, f"La tabla del modelo no debe tener nulos: {nulos_modelo}"
assert n_oro >= MINIMO_REGISTROS, f"Solo hay {n_oro:,} filas; se necesitan {MINIMO_REGISTROS:,}"
print(f"OK: capa oro escrita. {T_ORO_MODELO} tiene {n_oro:,} filas y {pct_total:.1f} % de retrasos.")
