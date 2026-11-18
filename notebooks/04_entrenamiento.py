# Databricks notebook source
# MAGIC %md
# MAGIC # 04 · Entrenamiento y evaluación del modelo
# MAGIC
# MAGIC **Objetivo:** entrenar un modelo que, con datos que se conocen **antes del despegue**, estime la
# MAGIC probabilidad de que un vuelo llegue con 15 minutos o más de retraso; medirlo con honestidad y
# MAGIC registrarlo en Unity Catalog para servirlo.
# MAGIC
# MAGIC ### El problema
# MAGIC * **Tipo:** clasificación binaria. Etiqueta `retrasado` = 1 si el vuelo llegó 15 min o más tarde.
# MAGIC * **Desbalanceado:** los vuelos retrasados son minoría (el porcentaje exacto lo calcula `03_oro`).
# MAGIC * **Variables (9):** mes, día de la semana, hora programada de salida y de llegada, distancia,
# MAGIC   duración programada, aerolínea, origen y destino.
# MAGIC
# MAGIC ### Por qué la métrica principal es F1 de la clase "retrasado"
# MAGIC Si, por ejemplo, 4 de cada 5 vuelos llegan a tiempo, un "modelo" que diga siempre "a tiempo" acierta 4 de
# MAGIC cada 5 veces (alta **exactitud**) y no sirve para nada: nunca avisa de un retraso. Su exactitud real sale
# MAGIC en la columna "siempre a tiempo" de la sección 5. F1 combina la **precisión** (de los vuelos que el modelo
# MAGIC marca como retrasados, cuántos lo estuvieron) y el **recall** (de los vuelos retrasados, cuántos detectó),
# MAGIC y vale 0 para ese modelo trivial. Por eso el modelo se elige y el umbral se ajusta para maximizar F1.
# MAGIC También se reportan PR-AUC y ROC-AUC (calidad del ordenamiento sin depender del umbral) y la
# MAGIC exactitud, junto con dos líneas base ingenuas para que el F1 tenga contra qué compararse.
# MAGIC
# MAGIC ### Cómo se evita la fuga de información
# MAGIC | Riesgo | Qué se hace |
# MAGIC |---|---|
# MAGIC | Usar datos que no se conocen al consultar (retraso de salida, hora real, causas del retraso) | La tabla `oro_vuelos_modelo` solo trae las 9 variables previas al despegue. |
# MAGIC | Vuelos del mismo día en entrenamiento y en prueba (comparten clima y saturación) | La prueba son días completos que el modelo nunca ve (`conjunto = 'prueba'`, definido en `03_oro`), y la validación también se separa **por días** dentro del entrenamiento. |
# MAGIC | Elegir modelo o umbral mirando la prueba | Modelo y umbral se eligen en **validación**; la prueba se usa una sola vez, al final. |
# MAGIC | La codificación por objetivo (`TargetEncoder`) "ve" la etiqueta | Se ajusta solo con los datos de ajuste y usa validación cruzada interna. |
# MAGIC
# MAGIC ### Qué esperar (honestamente)
# MAGIC Sin datos del clima ni del estado del aeropuerto en el día, un retraso es difícil de anticipar: lo
# MAGIC esperable es un **F1 modesto, aproximadamente entre 0.35 y 0.50**, claramente mejor que las líneas
# MAGIC base pero lejos de 1. **Un F1 por encima de ~0.8 sería sospechoso** y casi seguro indicaría una fuga
# MAGIC de información; el notebook lo avisa si pasa.
# MAGIC
# MAGIC | Paso | Qué se hace |
# MAGIC |---|---|
# MAGIC | 1 | Muestra de entrenamiento y de prueba (el cómputo de Free Edition es pequeño) |
# MAGIC | 2 | Separación ajuste / validación por días |
# MAGIC | 3 | Normalización y codificación (dentro del pipeline) |
# MAGIC | 4 | Entrenar línea base (logística) y modelo de gradiente; elegir modelo y umbral en validación |
# MAGIC | 5 | Evaluación final en los días de prueba |
# MAGIC | 6 | Registro en MLflow y Unity Catalog |
# MAGIC
# MAGIC **Qué deberías ver al final:** la tabla de métricas de prueba, dos gráficas y el mensaje
# MAGIC `OK: modelo registrado: workspace.vuelos.modelo_retrasos versión N (alias campeon)`, seguido del
# MAGIC bloque `RESUMEN PARA EL README` para copiar.

# COMMAND ----------

# MAGIC %run ./_config

# COMMAND ----------

# MAGIC %run ./_modelo

# COMMAND ----------

import time

import matplotlib.pyplot as plt
import mlflow
import numpy as np
import pandas as pd
from pyspark.sql import functions as F
from sklearn.metrics import (
    accuracy_score,
    average_precision_score,
    confusion_matrix,
    f1_score,
    precision_recall_curve,
    precision_score,
    recall_score,
    roc_auc_score,
)
from sklearn.model_selection import GroupShuffleSplit

# Tamaño de las muestras: el cómputo de Free Edition es pequeño. Súbelos solo si te sobra tiempo y memoria.
FILAS_ENTRENAMIENTO = 800_000
FILAS_PRUEBA = 300_000
FILAS_LOGISTICA = 200_000  # la línea base se entrena con una submuestra (es más lenta y solo es referencia)
FRACCION_VALIDACION = 0.2  # 20 % de los días de entrenamiento se apartan para validar
SEMILLA = 42

mlflow.sklearn.autolog(disable=True)  # todo se registra a mano en la sección 6

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Muestra de entrenamiento y de prueba
# MAGIC
# MAGIC Se toma una muestra aleatoria de cada conjunto con `sample` (fracción calculada para obtener cerca
# MAGIC del número de filas pedido) y `limit` solo recorta el pequeño sobrante. Después se pasa a pandas para
# MAGIC entrenar con scikit-learn.
# MAGIC
# MAGIC Las aerolíneas y aeropuertos que el modelo aceptará se toman de **todo** el conjunto de entrenamiento
# MAGIC (no solo de la muestra), para no rechazar aeropuertos pequeños que sí existen en los datos.

# COMMAND ----------

oro = spark.table(T_ORO_MODELO)
conteo_por_conjunto = {f["conjunto"]: int(f["count"]) for f in oro.groupBy("conjunto").count().collect()}
print("Filas por conjunto en la tabla oro:", conteo_por_conjunto)


def muestra_pandas(conjunto, filas):
    """Muestra aleatoria de un conjunto de la tabla oro, como DataFrame de pandas con tipos explícitos."""
    total = conteo_por_conjunto.get(conjunto, 0)
    assert total > 0, f"No hay filas con conjunto = '{conjunto}' en {T_ORO_MODELO}. Corre 03_oro."
    fraccion = min(1.0, filas / total)
    datos = (
        oro.filter(F.col("conjunto") == conjunto)
        .sample(fraction=fraccion, seed=SEMILLA)
        .limit(filas)
        .select("fecha_vuelo", *COLUMNAS_MODELO, ETIQUETA)
        .toPandas()
    )
    for c in COLUMNAS_ENTERAS:
        datos[c] = datos[c].astype("int64")
    for c in COLUMNAS_DECIMALES:
        datos[c] = datos[c].astype("float64")
    for c in COLUMNAS_TEXTO:
        datos[c] = datos[c].astype(str)
    datos[ETIQUETA] = datos[ETIQUETA].astype("int64")
    datos["fecha_vuelo"] = datos["fecha_vuelo"].astype(str)
    return datos


inicio = time.time()
entrenamiento = muestra_pandas("entrenamiento", FILAS_ENTRENAMIENTO)
prueba = muestra_pandas("prueba", FILAS_PRUEBA)
print(f"Muestra de entrenamiento: {len(entrenamiento):,} filas | muestra de prueba: {len(prueba):,} filas "
      f"({time.time() - inicio:.0f} s)")

datos_entrenamiento = oro.filter(F.col("conjunto") == "entrenamiento")
AEROLINEAS_CONOCIDAS = sorted(f[0] for f in datos_entrenamiento.select("aerolinea").distinct().collect())
AEROPUERTOS_CONOCIDOS = sorted(
    f[0]
    for f in datos_entrenamiento.select(F.col("origen").alias("aeropuerto"))
    .union(datos_entrenamiento.select(F.col("destino").alias("aeropuerto")))
    .distinct()
    .collect()
)
print(f"Aerolíneas conocidas: {len(AEROLINEAS_CONOCIDAS)} -> {', '.join(AEROLINEAS_CONOCIDAS)}")
print(f"Aeropuertos conocidos: {len(AEROPUERTOS_CONOCIDOS)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Separación ajuste / validación por días
# MAGIC
# MAGIC La muestra de entrenamiento se divide en **ajuste** (para entrenar) y **validación** (para elegir
# MAGIC modelo y umbral) agrupando por `fecha_vuelo`: todos los vuelos de un mismo día quedan del mismo lado.
# MAGIC La prueba ya viene separada por días desde la capa oro.

# COMMAND ----------

divisor = GroupShuffleSplit(n_splits=1, test_size=FRACCION_VALIDACION, random_state=SEMILLA)
indices_ajuste, indices_validacion = next(divisor.split(entrenamiento, groups=entrenamiento["fecha_vuelo"]))
ajuste = entrenamiento.iloc[indices_ajuste].reset_index(drop=True)
validacion = entrenamiento.iloc[indices_validacion].reset_index(drop=True)

dias = {nombre: set(df["fecha_vuelo"]) for nombre, df in [("ajuste", ajuste), ("validacion", validacion), ("prueba", prueba)]}
assert not dias["ajuste"] & dias["validacion"], "Un día quedó en ajuste y en validación"
assert not (dias["ajuste"] | dias["validacion"]) & dias["prueba"], "Un día de prueba quedó en entrenamiento"

partes = pd.DataFrame(
    [
        (nombre, len(df), len(dias[nombre]), round(100 * df[ETIQUETA].mean(), 2))
        for nombre, df in [("ajuste", ajuste), ("validacion", validacion), ("prueba", prueba)]
    ],
    columns=["parte", "vuelos", "dias", "pct_retrasados"],
)
display(partes)
print("OK: ningún día se repite entre ajuste, validación y prueba.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Normalización y codificación
# MAGIC
# MAGIC Las transformaciones viven **dentro del pipeline** del modelo: se aprenden solo con los datos de
# MAGIC ajuste y el endpoint aplica exactamente las mismas a cada consulta.
# MAGIC
# MAGIC | Paso del pipeline | Columnas | Transformación | Por qué |
# MAGIC |---|---|---|---|
# MAGIC | `escalado` | numéricas | `StandardScaler`: resta la media y divide entre la desviación estándar (quedan con media 0 y desviación 1) | La regresión logística es sensible a la escala: sin esto, la distancia (cientos o miles) dominaría a las horas (0 a 23). |
# MAGIC | `codificacion_one_hot` | aerolínea (y en la logística también aeropuertos, mes, día y horas) | `OneHotEncoder`: una columna 0/1 por categoría | Un modelo no puede usar texto, y numerar las categorías (AA=1, DL=2...) inventaría un orden que no existe. |
# MAGIC | `codificacion_objetivo` | origen, destino (solo en el modelo de gradiente) | `TargetEncoder`: cada aeropuerto se cambia por su tasa de retraso suavizada | Hay unos 350 aeropuertos: con one-hot serían cientos de columnas casi vacías. Usa validación cruzada interna para no filtrar la etiqueta. |
# MAGIC
# MAGIC La celda siguiente muestra el efecto en 5 vuelos: antes (datos crudos) y después (lo que recibe el clasificador).

# COMMAND ----------

print(construir_pipeline("logistica"))
print(construir_pipeline("gradiente"))

demostracion = construir_pipeline("gradiente").named_steps["preprocesamiento"]
muestra_demo = ajuste.sample(n=min(20_000, len(ajuste)), random_state=SEMILLA)
demostracion.fit(muestra_demo[COLUMNAS_MODELO], muestra_demo[ETIQUETA])
cinco = ajuste[COLUMNAS_MODELO].head(5)
transformados = pd.DataFrame(demostracion.transform(cinco), columns=demostracion.get_feature_names_out())

print("ANTES (datos crudos):")
display(cinco)
print("DESPUÉS (escalado + codificación):")
display(transformados.round(3))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Entrenamiento y elección de modelo y umbral (en validación)
# MAGIC
# MAGIC * **Línea base:** regresión logística (rápida e interpretable), entrenada con una submuestra.
# MAGIC * **Modelo principal:** `HistGradientBoostingClassifier`, que capta interacciones (por ejemplo,
# MAGIC   "vuelos tarde en la noche desde ciertos aeropuertos") que la logística no ve.
# MAGIC
# MAGIC Ambos usan `class_weight="balanced"`. Después, para cada modelo se busca en validación el **umbral**
# MAGIC de probabilidad que maximiza F1 (con `precision_recall_curve`): 0.5 casi nunca es el mejor en un
# MAGIC problema desbalanceado. Gana el modelo con mayor F1 en validación.

# COMMAND ----------


def mejor_umbral(y, probabilidad):
    """Umbral (redondeado a 4 decimales) que maximiza F1, según la curva precisión-recall."""
    precision, recall, umbrales = precision_recall_curve(y, probabilidad)
    precision, recall = precision[:-1], recall[:-1]
    suma = precision + recall
    f1 = np.divide(2 * precision * recall, suma, out=np.zeros_like(suma), where=suma > 0)
    return round(float(umbrales[int(np.argmax(f1))]), 4)


def probabilidades(pipeline, datos):
    # Mismo redondeo que ModeloRetrasos.predict, para que las métricas sean las del endpoint.
    return np.round(pipeline.predict_proba(datos[COLUMNAS_MODELO])[:, 1], 4)


y_validacion = validacion[ETIQUETA].to_numpy()
submuestra_logistica = ajuste.sample(n=min(FILAS_LOGISTICA, len(ajuste)), random_state=SEMILLA)

candidatos = {}
for tipo, datos in [("logistica", submuestra_logistica), ("gradiente", ajuste)]:
    inicio = time.time()
    pipeline = construir_pipeline(tipo).fit(datos[COLUMNAS_MODELO], datos[ETIQUETA])
    segundos = time.time() - inicio
    prob = probabilidades(pipeline, validacion)
    umbral = mejor_umbral(y_validacion, prob)
    candidatos[tipo] = {
        "pipeline": pipeline,
        "umbral": umbral,
        "filas_ajuste": len(datos),
        "segundos": round(segundos, 1),
        "f1_umbral_05": f1_score(y_validacion, prob >= 0.5),
        "f1_validacion": f1_score(y_validacion, prob >= umbral),
        "pr_auc_validacion": average_precision_score(y_validacion, prob),
    }
    print(f"{tipo}: entrenado con {len(datos):,} filas en {segundos:.0f} s | F1 validación = {candidatos[tipo]['f1_validacion']:.4f} (umbral {umbral})")

comparacion = pd.DataFrame(
    [
        (tipo, c["filas_ajuste"], c["segundos"], round(c["f1_umbral_05"], 4), c["umbral"], round(c["f1_validacion"], 4), round(c["pr_auc_validacion"], 4))
        for tipo, c in candidatos.items()
    ],
    columns=["modelo", "filas_ajuste", "segundos", "f1_con_umbral_0.5", "mejor_umbral", "f1_validacion", "pr_auc_validacion"],
)
display(comparacion)

tipo_elegido = max(candidatos, key=lambda t: candidatos[t]["f1_validacion"])
pipeline_elegido = candidatos[tipo_elegido]["pipeline"]
umbral = candidatos[tipo_elegido]["umbral"]
print(f"Modelo elegido: {tipo_elegido} con umbral {umbral} (F1 en validación {candidatos[tipo_elegido]['f1_validacion']:.4f})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Evaluación final en los días de prueba
# MAGIC
# MAGIC Se evalúa **una sola vez** el modelo elegido, con su umbral, en días que nunca participaron en el
# MAGIC entrenamiento ni en las decisiones. Es exactamente el modelo que se registra (no se reentrena).
# MAGIC
# MAGIC Líneas base para interpretar el resultado:
# MAGIC * **Siempre "retrasado":** recall perfecto, precisión igual al porcentaje de retrasos. Su F1 es el
# MAGIC   mínimo que un modelo útil debe superar.
# MAGIC * **Siempre "a tiempo":** exactitud alta, F1 = 0. Muestra por qué la exactitud engaña.

# COMMAND ----------

y_prueba = prueba[ETIQUETA].to_numpy()
prob_prueba = probabilidades(pipeline_elegido, prueba)
pred_prueba = (prob_prueba >= umbral).astype(int)


def metricas(y, prediccion, puntaje):
    return {
        "f1": f1_score(y, prediccion, zero_division=0),
        "precision": precision_score(y, prediccion, zero_division=0),
        "recall": recall_score(y, prediccion, zero_division=0),
        "pr_auc": average_precision_score(y, puntaje),
        "roc_auc": roc_auc_score(y, puntaje),
        "exactitud": accuracy_score(y, prediccion),
    }


resultados = {
    f"modelo ({tipo_elegido})": metricas(y_prueba, pred_prueba, prob_prueba),
    "siempre retrasado": metricas(y_prueba, np.ones_like(y_prueba), np.ones(len(y_prueba))),
    "siempre a tiempo": metricas(y_prueba, np.zeros_like(y_prueba), np.zeros(len(y_prueba))),
}
tabla_metricas = pd.DataFrame(resultados).round(4)
tabla_metricas.index.name = "metrica"
display(tabla_metricas.reset_index())

vn, fp, fn, vp = confusion_matrix(y_prueba, pred_prueba, labels=[0, 1]).ravel()
metricas_modelo = resultados[f"modelo ({tipo_elegido})"]
print(f"Vuelos de prueba: {len(y_prueba):,} ({100 * y_prueba.mean():.1f} % retrasados)")
print(f"Matriz de confusión: verdaderos negativos {vn:,} | falsos positivos {fp:,} | falsos negativos {fn:,} | verdaderos positivos {vp:,}")
print(f"F1 en prueba: {metricas_modelo['f1']:.4f} (siempre retrasado: {resultados['siempre retrasado']['f1']:.4f})")
if metricas_modelo["f1"] > 0.8:
    print("AVISO: un F1 tan alto sin datos del clima es sospechoso. Revisa que no se haya colado una variable "
          "que solo se conoce después del vuelo (fuga de información).")
if metricas_modelo["f1"] <= resultados["siempre retrasado"]["f1"]:
    print("AVISO: el modelo no supera a la línea base 'siempre retrasado'. Revisa los datos antes de publicarlo.")

# COMMAND ----------

fig_matriz, ax = plt.subplots(figsize=(5, 4))
matriz = np.array([[vn, fp], [fn, vp]])
ax.imshow(matriz, cmap="Blues")
for i in range(2):
    for j in range(2):
        ax.text(j, i, f"{matriz[i, j]:,}", ha="center", va="center",
                color="white" if matriz[i, j] > matriz.max() / 2 else "black", fontsize=12)
ax.set_xticks([0, 1], labels=["a tiempo", "retrasado"])
ax.set_yticks([0, 1], labels=["a tiempo", "retrasado"])
ax.set_xlabel("Predicción")
ax.set_ylabel("Realidad")
ax.set_title(f"Matriz de confusión en prueba (umbral {umbral})")
fig_matriz.tight_layout()
plt.show()

precision_curva, recall_curva, _ = precision_recall_curve(y_prueba, prob_prueba)
fig_pr, ax = plt.subplots(figsize=(6, 4.5))
ax.plot(recall_curva, precision_curva, label=f"{tipo_elegido} (PR-AUC {metricas_modelo['pr_auc']:.3f})")
ax.axhline(y_prueba.mean(), color="gray", linestyle="--", label=f"azar / siempre retrasado ({y_prueba.mean():.3f})")
ax.scatter([metricas_modelo["recall"]], [metricas_modelo["precision"]], color="red", zorder=3,
           label=f"umbral elegido {umbral} (F1 {metricas_modelo['f1']:.3f})")
ax.set_xlabel("Recall (retrasos detectados)")
ax.set_ylabel("Precisión (avisos correctos)")
ax.set_title("Curva precisión-recall en los días de prueba")
ax.set_xlim(0, 1)
ax.set_ylim(0, 1)
ax.legend(loc="upper right")
fig_pr.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Registro en MLflow y en Unity Catalog
# MAGIC
# MAGIC En una sola corrida de MLflow (experimento `/Users/<tu usuario>/vuelos_retrasos`) quedan los
# MAGIC parámetros, las métricas, las dos gráficas y el modelo. El modelo se registra como
# MAGIC `modelo_retrasos` en Unity Catalog, con firma (tipos de cada columna), un ejemplo de entrada, las
# MAGIC versiones exactas de las librerías y el alias `campeon`, que es el que publica `05_publicar_endpoint`.

# COMMAND ----------

usuario = spark.sql("SELECT current_user()").first()[0]
EXPERIMENTO = f"/Users/{usuario}/vuelos_retrasos"
mlflow.set_experiment(EXPERIMENTO)
if mlflow.active_run() is not None:
    mlflow.end_run()  # por si quedó abierta una corrida de un intento anterior

with mlflow.start_run(run_name=f"entrenamiento_{tipo_elegido}") as corrida:
    mlflow.log_params(
        {
            "modelo_elegido": tipo_elegido,
            "umbral_decision": umbral,
            "filas_ajuste": len(ajuste),
            "filas_validacion": len(validacion),
            "filas_prueba": len(prueba),
            "dias_ajuste": len(dias["ajuste"]),
            "dias_validacion": len(dias["validacion"]),
            "dias_prueba": len(dias["prueba"]),
            "filas_logistica": candidatos["logistica"]["filas_ajuste"],
            "semilla": SEMILLA,
            "aerolineas_conocidas": len(AEROLINEAS_CONOCIDAS),
            "aeropuertos_conocidos": len(AEROPUERTOS_CONOCIDOS),
        }
    )
    for tipo, c in candidatos.items():
        mlflow.log_metrics(
            {
                f"f1_validacion_{tipo}": c["f1_validacion"],
                f"f1_umbral_05_validacion_{tipo}": c["f1_umbral_05"],
                f"pr_auc_validacion_{tipo}": c["pr_auc_validacion"],
                f"segundos_entrenamiento_{tipo}": c["segundos"],
            }
        )
    mlflow.log_metrics({f"{nombre}_prueba": float(valor) for nombre, valor in metricas_modelo.items()})
    mlflow.log_metrics(
        {
            "f1_prueba_siempre_retrasado": resultados["siempre retrasado"]["f1"],
            "exactitud_prueba_siempre_a_tiempo": resultados["siempre a tiempo"]["exactitud"],
            "verdaderos_negativos": int(vn),
            "falsos_positivos": int(fp),
            "falsos_negativos": int(fn),
            "verdaderos_positivos": int(vp),
        }
    )
    mlflow.log_figure(fig_matriz, "graficas/matriz_confusion.png")
    mlflow.log_figure(fig_pr, "graficas/curva_precision_recall.png")
    mlflow.log_text(tabla_metricas.reset_index().to_csv(index=False), "metricas_prueba.csv")
    mlflow.log_text(comparacion.to_csv(index=False), "comparacion_validacion.csv")

    version = registrar_modelo(
        pipeline_elegido,
        umbral=umbral,
        ejemplo=ajuste[COLUMNAS_MODELO].head(3),
        aerolineas=AEROLINEAS_CONOCIDAS,
        aeropuertos=AEROPUERTOS_CONOCIDOS,
        nombre_modelo=NOMBRE_MODELO,
        alias=ALIAS_MODELO,
        descripcion=(
            f"Modelo {tipo_elegido} entrenado con {len(ajuste):,} vuelos de {len(dias['ajuste'])} días; "
            f"umbral {umbral}; F1 en {len(prueba):,} vuelos de prueba = {metricas_modelo['f1']:.4f}."
        ),
    )

print(f"Corrida de MLflow: {corrida.info.run_id} (experimento {EXPERIMENTO})")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.1 Comprobación: el modelo registrado predice igual y valida las entradas
# MAGIC Se carga **desde Unity Catalog** y se compara con el pipeline en memoria; después se le manda un vuelo inválido.

# COMMAND ----------

from mlflow.exceptions import MlflowException

modelo_registrado = mlflow.pyfunc.load_model(f"models:/{NOMBRE_MODELO}/{version}")
vuelos_ejemplo = ajuste[COLUMNAS_MODELO].head(5)
respuesta = modelo_registrado.predict(vuelos_ejemplo)
display(pd.concat([vuelos_ejemplo, respuesta], axis=1))
assert np.allclose(respuesta["probabilidad_retraso"].to_numpy(), probabilidades(pipeline_elegido, vuelos_ejemplo)), (
    "El modelo registrado no predice lo mismo que el pipeline entrenado"
)

vuelo_invalido = vuelos_ejemplo.head(1).assign(mes=13, destino=vuelos_ejemplo["origen"].iloc[0])
try:
    modelo_registrado.predict(vuelo_invalido)
    raise AssertionError("El modelo debió rechazar mes=13 y origen igual a destino")
except MlflowException as error:
    print("Rechazo esperado para un vuelo inválido:", error.message)

print(f"OK: modelo registrado: {NOMBRE_MODELO} versión {version} (alias {ALIAS_MODELO})")

# COMMAND ----------

# Copia el bloque de abajo (entre las líneas) y pégalo en el README, sección "2. Definición del problema y métrica".
def fila(nombre, clave):
    modelo, siempre_si, siempre_no = (resultados[k][clave] for k in resultados)
    return f"| {nombre} | {modelo:.3f} | {siempre_si:.3f} | {siempre_no:.3f} |"


lineas = [
    "RESUMEN PARA EL README",
    "-" * 80,
    "### Resultados en los días de prueba (el modelo nunca los vio)",
    "",
    f"| Métrica | Modelo ({tipo_elegido}) | Siempre \"retrasado\" | Siempre \"a tiempo\" |",
    "|---|---:|---:|---:|",
    fila("**F1 (clase retrasado)**", "f1"),
    fila("Precisión", "precision"),
    fila("Recall", "recall"),
    fila("PR-AUC", "pr_auc"),
    fila("ROC-AUC", "roc_auc"),
    fila("Exactitud", "exactitud"),
    "",
    f"* **Modelo elegido:** {tipo_elegido} (F1 en validación: "
    + ", ".join(f"{t} {c['f1_validacion']:.3f}" for t, c in candidatos.items())
    + ").",
    f"* **Umbral de decisión:** {umbral} (maximiza F1 en los días de validación).",
    f"* **Muestras:** ajuste {len(ajuste):,} vuelos ({len(dias['ajuste'])} días), validación {len(validacion):,} "
    f"({len(dias['validacion'])} días), prueba {len(prueba):,} ({len(dias['prueba'])} días). "
    f"La línea base logística se entrenó con {candidatos['logistica']['filas_ajuste']:,} vuelos.",
    f"* **Porcentaje de retrasos en prueba:** {100 * y_prueba.mean():.1f} %.",
    f"* **Matriz de confusión (prueba):** VN {vn:,} · FP {fp:,} · FN {fn:,} · VP {vp:,}.",
    f"* **Modelo registrado:** `{NOMBRE_MODELO}` versión {version} (alias `{ALIAS_MODELO}`), "
    f"{len(AEROLINEAS_CONOCIDAS)} aerolíneas y {len(AEROPUERTOS_CONOCIDOS)} aeropuertos conocidos.",
    "-" * 80,
]
print("\n".join(lineas))
