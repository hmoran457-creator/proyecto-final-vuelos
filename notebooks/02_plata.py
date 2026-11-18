# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Capa PLATA — limpieza y transformación
# MAGIC
# MAGIC **Objetivo:** convertir los datos crudos de bronce en una tabla confiable: tipos correctos, sin
# MAGIC duplicados, con los nulos tratados y con los valores imposibles fuera.
# MAGIC
# MAGIC Cada decisión queda registrada en una **bitácora de calidad** (tabla `plata_bitacora_calidad`) con
# MAGIC cuántas filas había antes, cuántas quedaron y **por qué** se hizo. Esa bitácora es la evidencia del
# MAGIC criterio 1 de la rúbrica.
# MAGIC
# MAGIC | Paso | Qué se trata |
# MAGIC |---|---|
# MAGIC | 1 | Selección de columnas y **tipos de dato** |
# MAGIC | 2 | **Nulos** |
# MAGIC | 3 | **Duplicados** |
# MAGIC | 4 | Valores imposibles y **outliers** |
# MAGIC | 5 | Verificaciones finales y escritura |
# MAGIC
# MAGIC La **normalización** (escalado) y la **codificación** de variables categóricas se hacen en el
# MAGIC notebook `04_entrenamiento`, dentro del pipeline del modelo. Va ahí a propósito: así el endpoint
# MAGIC aplica exactamente la misma transformación a cada consulta que la que se usó al entrenar.
# MAGIC
# MAGIC **Qué deberías ver al final:** la bitácora con 9 renglones y el mensaje `OK: capa plata escrita`.
# MAGIC Lo normal es que plata conserve más del 99 % de las filas de bronce.

# COMMAND ----------

# MAGIC %run ./_config

# COMMAND ----------

from pyspark.sql import functions as F

bronce = spark.table(T_BRONCE)
n_bronce = bronce.count()
print(f"Filas en bronce: {n_bronce:,}")

bitacora = []


def anotar(paso, tratamiento, antes, despues, justificacion):
    """Guarda un renglón de la bitácora de calidad y lo imprime."""
    bitacora.append(
        (len(bitacora) + 1, paso, tratamiento, int(antes), int(despues), int(antes) - int(despues), justificacion)
    )
    print(f"[{paso}] {tratamiento}: {int(antes):,} -> {int(despues):,} filas (se quitaron {int(antes) - int(despues):,})")


def tabla(filas, esquema):
    """Muestra una lista de tuplas de Python como tabla."""
    display(spark.createDataFrame(filas, esquema))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Selección de columnas y tipos de dato
# MAGIC
# MAGIC De las 109 columnas de la fuente se conservan 33. Se descartan los identificadores redundantes
# MAGIC (el mismo aeropuerto viene en 6 columnas distintas) y las 48 columnas de desvíos `Div*`, que están
# MAGIC vacías en más del 99 % de los vuelos.
# MAGIC
# MAGIC En bronce todo es texto. Aquí cada columna se convierte a su tipo real con `try_cast`:
# MAGIC si un valor no se puede convertir queda en `NULL` **y se cuenta**, en lugar de tronar el proceso
# MAGIC o colarse como basura. (El cómputo serverless de Databricks usa modo ANSI: un `CAST` normal sobre
# MAGIC un texto inválido detiene el notebook con error.)

# COMMAND ----------

# (columna en bronce, nombre en plata, tipo)
# CODIGO = texto en mayúsculas y sin espacios; TEXTO = texto sin espacios sobrantes
COLUMNAS = [
    ("FlightDate", "fecha_vuelo", "DATE"),
    ("Year", "anio", "INT"),
    ("Month", "mes", "INT"),
    ("DayofMonth", "dia_mes", "INT"),
    ("DayOfWeek", "dia_semana", "INT"),  # 1 = lunes ... 7 = domingo
    ("Reporting_Airline", "aerolinea", "CODIGO"),
    ("Flight_Number_Reporting_Airline", "numero_vuelo", "INT"),
    ("Tail_Number", "matricula", "CODIGO"),
    ("Origin", "origen", "CODIGO"),
    ("OriginCityName", "origen_ciudad", "TEXTO"),
    ("OriginState", "origen_estado", "CODIGO"),
    ("Dest", "destino", "CODIGO"),
    ("DestCityName", "destino_ciudad", "TEXTO"),
    ("DestState", "destino_estado", "CODIGO"),
    ("CRSDepTime", "salida_prog_hhmm", "INT"),  # hora programada, formato HHMM
    ("DepTime", "salida_real_hhmm", "INT"),
    ("DepDelay", "retraso_salida_min", "DOUBLE"),
    ("CRSArrTime", "llegada_prog_hhmm", "INT"),
    ("ArrTime", "llegada_real_hhmm", "INT"),
    ("ArrDelay", "retraso_llegada_min", "DOUBLE"),
    ("ArrDel15", "retrasado_15", "INT"),  # 1 = llegó 15 min o más tarde
    ("Cancelled", "cancelado", "INT"),
    ("CancellationCode", "codigo_cancelacion", "CODIGO"),
    ("Diverted", "desviado", "INT"),
    ("CRSElapsedTime", "duracion_prog_min", "DOUBLE"),
    ("ActualElapsedTime", "duracion_real_min", "DOUBLE"),
    ("AirTime", "tiempo_aire_min", "DOUBLE"),
    ("Distance", "distancia_millas", "DOUBLE"),
    ("CarrierDelay", "retraso_aerolinea_min", "DOUBLE"),
    ("WeatherDelay", "retraso_clima_min", "DOUBLE"),
    ("NASDelay", "retraso_sistema_aereo_min", "DOUBLE"),
    ("SecurityDelay", "retraso_seguridad_min", "DOUBLE"),
    ("LateAircraftDelay", "retraso_avion_previo_min", "DOUBLE"),
]


def expresion_tipo(origen, tipo):
    """Expresión SQL que convierte una columna de texto de bronce a su tipo en plata."""
    limpio = f"NULLIF(TRIM(`{origen}`), '')"
    if tipo == "DATE":
        return f"try_cast({limpio} AS DATE)"
    if tipo == "DOUBLE":
        return f"try_cast({limpio} AS DOUBLE)"
    if tipo == "INT":
        # Los enteros vienen como '1.00': primero a decimal y luego a entero.
        return f"try_cast(try_cast({limpio} AS DOUBLE) AS INT)"
    if tipo == "CODIGO":
        return f"UPPER({limpio})"
    return limpio  # TEXTO


tipado = bronce.select(
    *[F.expr(expresion_tipo(origen, tipo)).alias(destino) for origen, destino, tipo in COLUMNAS],
    F.col("_archivo_zip"),
)
tipado.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.1 ¿Cuántos valores no se pudieron convertir?
# MAGIC Un valor "no convertible" es uno que traía texto en bronce y quedó `NULL` después del cambio de tipo.
# MAGIC Lo esperado es **0** en todas las columnas; si no, la tabla dice exactamente dónde está el problema.

# COMMAND ----------

fallas = bronce.select(
    *[
        F.sum(
            F.when(
                F.expr(f"NULLIF(TRIM(`{origen}`), '')").isNotNull() & F.expr(expresion_tipo(origen, tipo)).isNull(), 1
            ).otherwise(0)
        ).alias(destino)
        for origen, destino, tipo in COLUMNAS
        if tipo in ("DATE", "INT", "DOUBLE")
    ]
).first().asDict()

total_fallas = sum(fallas.values())
tabla(sorted(fallas.items(), key=lambda par: -par[1]), "columna STRING, valores_no_convertibles LONG")
print(f"Valores no convertibles en total: {total_fallas:,}")

anotar(
    "1. Tipos de dato",
    f"33 columnas convertidas de texto a su tipo real; {total_fallas:,} valores no convertibles pasan a NULL",
    n_bronce,
    n_bronce,
    "Bronce guarda todo como texto. Se usa try_cast para que un valor inválido se cuente y quede NULL "
    "en vez de detener el proceso o guardarse como dato falso.",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Nulos
# MAGIC
# MAGIC Primero el diagnóstico: cuántos nulos tiene cada columna.

# COMMAND ----------

columnas_plata = [destino for _, destino, _ in COLUMNAS]
nulos = tipado.select(*[F.sum(F.col(c).isNull().cast("int")).alias(c) for c in columnas_plata]).first().asDict()

tabla(
    [(c, int(n), round(100.0 * n / n_bronce, 2)) for c, n in sorted(nulos.items(), key=lambda par: -par[1])],
    "columna STRING, nulos LONG, porcentaje DOUBLE",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.1 Tratamiento, columna por columna
# MAGIC
# MAGIC En estos datos un nulo casi nunca es un error de captura: significa **"eso no ocurrió"**.
# MAGIC Por eso no se rellena todo con la media; cada grupo se trata según lo que significa.
# MAGIC
# MAGIC | Columnas | Qué significa el nulo | Tratamiento | Justificación |
# MAGIC |---|---|---|---|
# MAGIC | Fecha, aerolínea, número de vuelo, origen, destino, horas programadas, distancia, duración programada, `cancelado`, `desviado` | Registro incompleto | **Se elimina la fila** | Sin estos datos no se puede identificar el vuelo ni usarlo en el modelo. Inventarlos metería ruido. |
# MAGIC | Las 5 causas de retraso (`retraso_*_min`) | El BTS solo reporta causas cuando el vuelo llegó 15 min o más tarde | **Se rellena con 0** en vuelos no cancelados | Si no hubo retraso reportable, los minutos atribuibles a cada causa son 0. |
# MAGIC | `matricula` | Avión no asignado (típico en cancelados) | **`DESCONOCIDA`** | Es un identificador, no se puede estimar. |
# MAGIC | `codigo_cancelacion` | El vuelo no se canceló | **`NA`** (no aplica) | Evita un nulo que en realidad es una categoría. |
# MAGIC | `retraso_llegada_min`, `retrasado_15`, horas reales, duración real | El vuelo se canceló o se desvió: nunca llegó a su destino programado | **Se deja NULL** | Es la variable objetivo. Imputarla sería inventar el resultado que queremos predecir. Estos vuelos se excluyen del modelo en la capa oro. |

# COMMAND ----------

CRITICAS = [
    "fecha_vuelo", "mes", "dia_semana", "aerolinea", "numero_vuelo", "origen", "destino",
    "salida_prog_hhmm", "llegada_prog_hhmm", "distancia_millas", "duracion_prog_min", "cancelado", "desviado",
]
CAUSAS = [
    "retraso_aerolinea_min", "retraso_clima_min", "retraso_sistema_aereo_min",
    "retraso_seguridad_min", "retraso_avion_previo_min",
]

sin_nulos_criticos = tipado.dropna(subset=CRITICAS)
n_sin_criticos = sin_nulos_criticos.count()
anotar(
    "2. Nulos",
    "Se eliminan filas con nulo en columnas críticas (identificación del vuelo y variables del modelo)",
    n_bronce,
    n_sin_criticos,
    "Sin fecha, ruta, horario o distancia no se puede identificar el vuelo ni usarlo para predecir.",
)

con_nulos_tratados = sin_nulos_criticos
for causa in CAUSAS:
    con_nulos_tratados = con_nulos_tratados.withColumn(
        causa, F.when(F.col("cancelado") == 0, F.coalesce(F.col(causa), F.lit(0.0))).otherwise(F.col(causa))
    )
con_nulos_tratados = con_nulos_tratados.fillna({"matricula": "DESCONOCIDA", "codigo_cancelacion": "NA"})

n_causas_rellenadas = sum(nulos[c] for c in CAUSAS)
anotar(
    "2. Nulos",
    f"Causas de retraso: {n_causas_rellenadas:,} nulos se rellenan con 0; matrícula y código de cancelación reciben categoría explícita",
    n_sin_criticos,
    n_sin_criticos,
    "El BTS solo reporta causas cuando hubo retraso de 15 min o más: ahí el nulo equivale a 0 minutos.",
)

# La etiqueta nula debe explicarse SIEMPRE por un vuelo cancelado o desviado.
sin_etiqueta = con_nulos_tratados.filter(F.col("retrasado_15").isNull())
n_sin_etiqueta = sin_etiqueta.count()
n_explicados = sin_etiqueta.filter((F.col("cancelado") == 1) | (F.col("desviado") == 1)).count()
print(f"Vuelos sin etiqueta de retraso: {n_sin_etiqueta:,} ({100.0 * n_sin_etiqueta / n_sin_criticos:.2f} %)")
print(f"  explicados por cancelación o desvío: {n_explicados:,}")
print(f"  sin explicación                    : {n_sin_etiqueta - n_explicados:,}")
anotar(
    "2. Nulos",
    f"Etiqueta de retraso: {n_sin_etiqueta:,} nulos se conservan como NULL (no se imputan)",
    n_sin_criticos,
    n_sin_criticos,
    "Son vuelos cancelados o desviados: no tienen hora de llegada. Imputar la variable objetivo sería "
    "inventar el resultado; se excluyen del modelo en la capa oro.",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Duplicados
# MAGIC
# MAGIC Se revisan dos tipos:
# MAGIC * **Duplicado exacto:** la fila completa repetida (pasa, por ejemplo, si el mismo mes se sube dos veces).
# MAGIC * **Duplicado por llave:** dos filas distintas para el mismo vuelo. La llave natural de un vuelo es
# MAGIC   fecha + aerolínea + número de vuelo + origen + destino + hora programada de salida.
# MAGIC
# MAGIC La revisión se hace y se registra **aunque el resultado sea 0**: saber que no hay duplicados también
# MAGIC es un resultado, y el paso protege las siguientes cargas.

# COMMAND ----------

LLAVE = ["fecha_vuelo", "aerolinea", "numero_vuelo", "origen", "destino", "salida_prog_hhmm"]

sin_exactos = con_nulos_tratados.dropDuplicates(columnas_plata)  # ignora _archivo_zip a propósito
n_sin_exactos = sin_exactos.count()
anotar(
    "3. Duplicados",
    "Se eliminan filas repetidas exactas (mismos valores en las 33 columnas)",
    n_sin_criticos,
    n_sin_exactos,
    "Una fila idéntica no aporta información y contaría dos veces el mismo vuelo.",
)

sin_duplicados = sin_exactos.dropDuplicates(LLAVE)
n_sin_duplicados = sin_duplicados.count()
anotar(
    "3. Duplicados",
    "Se eliminan duplicados por llave natural del vuelo (se conserva una fila por vuelo)",
    n_sin_exactos,
    n_sin_duplicados,
    "Un vuelo es un solo evento: dos registros del mismo vuelo sesgarían los conteos y el entrenamiento.",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Valores imposibles y outliers
# MAGIC
# MAGIC Se separan dos casos que **no** deben tratarse igual:
# MAGIC
# MAGIC **4.1 Valores imposibles (errores):** se eliminan con reglas de dominio.
# MAGIC
# MAGIC **4.2 Valores extremos pero reales (outliers estadísticos):** un retraso de 20 horas es raro, pero
# MAGIC ocurrió. Borrarlo sería borrar justo el fenómeno que se estudia.

# COMMAND ----------

con_horas = (
    sin_duplicados
    .withColumn("hora_salida_prog", (F.floor(F.col("salida_prog_hhmm") / 100) % 24).cast("int"))
    .withColumn("hora_llegada_prog", (F.floor(F.col("llegada_prog_hhmm") / 100) % 24).cast("int"))
    # try_divide: si la duración fuera 0, da NULL en vez de detener el notebook (modo ANSI).
    .withColumn("velocidad_implicita_mph", F.expr("try_divide(distancia_millas, duracion_prog_min / 60.0)"))
)


def hhmm_invalida(columna):
    c = F.col(columna)
    return (c < 0) | (c > 2400) | ((c % 100) >= 60)


# (regla, condición que marca la fila como inválida, justificación)
REGLAS = [
    (
        "Fecha inconsistente con año o mes",
        (F.year("fecha_vuelo") != F.col("anio")) | (F.month("fecha_vuelo") != F.col("mes")),
        "La fecha del vuelo debe coincidir con las columnas de año y mes del mismo registro.",
    ),
    (
        "Mes o día de la semana fuera de rango",
        (~F.col("mes").between(1, 12)) | (~F.col("dia_semana").between(1, 7)),
        "Solo existen 12 meses y 7 días.",
    ),
    (
        "Hora programada inválida (no es HHMM)",
        hhmm_invalida("salida_prog_hhmm") | hhmm_invalida("llegada_prog_hhmm"),
        "Una hora válida va de 0000 a 2400 y sus minutos de 00 a 59.",
    ),
    (
        "Distancia fuera de rango (<= 0 o > 6,000 millas)",
        (F.col("distancia_millas") <= 0) | (F.col("distancia_millas") > 6000),
        "La ruta doméstica más larga de EE.UU. ronda las 5,100 millas.",
    ),
    (
        "Duración programada fuera de rango (< 15 o > 900 min)",
        (F.col("duracion_prog_min") < 15) | (F.col("duracion_prog_min") > 900),
        "Ningún vuelo comercial doméstico se programa en menos de 15 min ni en más de 15 horas.",
    ),
    (
        "Velocidad implícita imposible (< 40 o > 700 mph)",
        (F.col("velocidad_implicita_mph") < 40) | (F.col("velocidad_implicita_mph") > 700),
        "Distancia entre duración programada da una velocidad que ningún avión comercial alcanza: el horario está mal capturado.",
    ),
    (
        "Origen igual a destino",
        F.col("origen") == F.col("destino"),
        "Un vuelo comercial no despega y aterriza en el mismo aeropuerto como ruta programada.",
    ),
]

# Si una regla no se puede evaluar (por un nulo), la fila se trata como inválida: así lo que se
# cuenta en la tabla es exactamente lo que se elimina.
REGLAS = [(regla, F.coalesce(condicion, F.lit(True)), justificacion) for regla, condicion, justificacion in REGLAS]

conteo_reglas = con_horas.select(
    *[F.sum(F.when(condicion, 1).otherwise(0)).alias(f"r{i}") for i, (_, condicion, _) in enumerate(REGLAS)]
).first()

tabla(
    [(regla, int(conteo_reglas[f"r{i}"] or 0), justificacion) for i, (regla, _, justificacion) in enumerate(REGLAS)],
    "regla STRING, filas_que_la_violan LONG, justificacion STRING",
)

invalida = REGLAS[0][1]
for _, condicion, _ in REGLAS[1:]:
    invalida = invalida | condicion

validos = con_horas.filter(~invalida)
n_validos = validos.count()
anotar(
    "4. Outliers",
    f"Se eliminan filas que violan alguna de las {len(REGLAS)} reglas de dominio (valores imposibles)",
    n_sin_duplicados,
    n_validos,
    "Son errores de captura, no vuelos raros: fechas incoherentes, horas inexistentes, distancias o "
    "velocidades físicamente imposibles.",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.2 Retrasos extremos: ¿se borran?
# MAGIC
# MAGIC La regla clásica del rango intercuartílico (IQR) marcaría como outlier todo retraso por encima de
# MAGIC `Q3 + 1.5·IQR`. Veamos cuántos vuelos caerían ahí antes de decidir.

# COMMAND ----------

operados = validos.filter((F.col("cancelado") == 0) & (F.col("desviado") == 0) & F.col("retraso_llegada_min").isNotNull())
n_operados = operados.count()

p = operados.select(
    F.percentile_approx("retraso_llegada_min", [0.001, 0.25, 0.5, 0.75, 0.999], 10000).alias("p"),
    F.min("retraso_llegada_min").alias("minimo"),
    F.max("retraso_llegada_min").alias("maximo"),
).first()
p001, q1, mediana, q3, p999 = [float(x) for x in p["p"]]
iqr = q3 - q1
limite_iqr = q3 + 1.5 * iqr
n_sobre_iqr = operados.filter(F.col("retraso_llegada_min") > limite_iqr).count()

print(f"Retraso de llegada (min): mínimo {p['minimo']:.0f} | Q1 {q1:.0f} | mediana {mediana:.0f} | Q3 {q3:.0f} | máximo {p['maximo']:.0f}")
print(f"Límite superior por IQR : {limite_iqr:.0f} min")
print(f"Vuelos sobre ese límite : {n_sobre_iqr:,} ({100.0 * n_sobre_iqr / n_operados:.1f} % de los vuelos operados)")
print(f"Percentiles 0.1 y 99.9  : {p001:.0f} y {p999:.0f} min")

# COMMAND ----------

# MAGIC %md
# MAGIC **Decisión: no se eliminan.** La regla IQR marcaría como "atípicos" a una parte importante de los
# MAGIC vuelos (el porcentaje exacto sale en la celda anterior), y todos son vuelos retrasados: justo la clase
# MAGIC que el modelo debe aprender a reconocer. Los retrasos largos son reales (tormentas, fallas de
# MAGIC sistemas), no errores.
# MAGIC
# MAGIC Lo que sí se hace es **acotar** (winsorizar) el retraso a los percentiles 0.1 y 99.9 en una columna
# MAGIC nueva, `retraso_llegada_min_acotado`, que se usa para los **promedios** de la capa oro: un solo vuelo
# MAGIC con 45 horas de retraso no debe mover el promedio de toda una aerolínea. La columna original se conserva.
# MAGIC
# MAGIC Para el modelo esto no cambia nada: la etiqueta es binaria (15 minutos o más), así que un retraso de
# MAGIC 3 horas y uno de 30 horas pesan igual.

# COMMAND ----------

# Ojo: least/greatest ignoran los nulos, así que sin el when() un vuelo cancelado (retraso NULL)
# recibiría el valor del límite. El when() conserva el NULL.
plata = validos.withColumn(
    "retraso_llegada_min_acotado",
    F.when(
        F.col("retraso_llegada_min").isNotNull(),
        F.least(F.greatest(F.col("retraso_llegada_min"), F.lit(p001)), F.lit(p999)),
    ),
).withColumn("ruta", F.concat_ws("-", "origen", "destino"))

n_acotados = operados.filter((F.col("retraso_llegada_min") < p001) | (F.col("retraso_llegada_min") > p999)).count()
anotar(
    "4. Outliers",
    f"Retrasos extremos: NO se eliminan; {n_acotados:,} valores se acotan a los percentiles 0.1 y 99.9 "
    f"({p001:.0f} y {p999:.0f} min) en una columna nueva",
    n_validos,
    n_validos,
    f"La regla IQR marcaría {100.0 * n_sobre_iqr / n_operados:.1f} % de los vuelos, que son retrasos reales y la "
    "clase que se quiere predecir. Se acota solo para que los promedios no dependan de unos cuantos casos.",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Verificaciones finales y escritura
# MAGIC Antes de guardar se comprueba que la tabla cumple lo que promete. Si algo falla, el notebook se detiene aquí.

# COMMAND ----------

columnas_finales = (
    columnas_plata
    + ["hora_salida_prog", "hora_llegada_prog", "ruta", "retraso_llegada_min_acotado", "_archivo_zip"]
)
plata = plata.select(*columnas_finales)

revision = plata.select(
    F.count("*").alias("filas"),
    *[F.sum(F.col(c).isNull().cast("int")).alias(f"nulos_{c}") for c in CRITICAS],
    F.sum((~F.col("hora_salida_prog").between(0, 23)).cast("int")).alias("horas_malas"),
    F.sum((~F.col("retrasado_15").isin(0, 1)).cast("int")).alias("etiquetas_malas"),
    F.sum(
        (F.col("retrasado_15").isNull() & (F.col("cancelado") == 0) & (F.col("desviado") == 0)).cast("int")
    ).alias("sin_etiqueta_sin_motivo"),
    F.sum(
        (F.col("retraso_llegada_min").isNull() & F.col("retraso_llegada_min_acotado").isNotNull()).cast("int")
    ).alias("acotados_inventados"),
).first().asDict()

n_plata = revision["filas"]
nulos_criticos = sum(v or 0 for k, v in revision.items() if k.startswith("nulos_"))
n_llaves = plata.select(*LLAVE).distinct().count()

print(f"Filas en plata                       : {n_plata:,}")
print(f"Nulos en columnas críticas           : {nulos_criticos}")
print(f"Horas fuera de 0-23                  : {revision['horas_malas'] or 0}")
print(f"Etiquetas distintas de 0/1           : {revision['etiquetas_malas'] or 0}")
print(f"Vuelos operados sin etiqueta         : {revision['sin_etiqueta_sin_motivo'] or 0}")
print(f"Llaves únicas                        : {n_llaves:,}")

assert nulos_criticos == 0, "Quedaron nulos en columnas críticas"
assert (revision["horas_malas"] or 0) == 0, "Quedaron horas fuera de 0-23"
assert (revision["etiquetas_malas"] or 0) == 0, "La etiqueta tiene valores distintos de 0 y 1"
assert n_llaves == n_plata, "Quedaron vuelos duplicados por llave"
assert (revision["acotados_inventados"] or 0) == 0, "Un vuelo sin retraso registrado recibió retraso acotado"

plata.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(T_PLATA)

anotar(
    "5. Resultado",
    f"Tabla plata escrita con {len(columnas_finales)} columnas",
    n_bronce,
    n_plata,
    "Balance total de bronce a plata.",
)

spark.createDataFrame(
    bitacora,
    "orden INT, paso STRING, tratamiento STRING, filas_antes LONG, filas_despues LONG, "
    "filas_eliminadas LONG, justificacion STRING",
).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(T_BITACORA)

print(f"OK: capa plata escrita en {T_PLATA} ({n_plata:,} filas; {100.0 * n_plata / n_bronce:.2f} % de bronce)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Bitácora de calidad (evidencia para el README)

# COMMAND ----------

display(spark.table(T_BITACORA).orderBy("orden"))

# COMMAND ----------

# Copia el texto de esta celda y pégalo en el README, sección "Limpieza y transformación".
print("| # | Paso | Tratamiento | Filas antes | Filas después | Eliminadas | Justificación |")
print("|---|---|---|---:|---:|---:|---|")
for orden, paso, tratamiento, antes, despues, eliminadas, justificacion in bitacora:
    print(f"| {orden} | {paso} | {tratamiento} | {antes:,} | {despues:,} | {eliminadas:,} | {justificacion} |")
