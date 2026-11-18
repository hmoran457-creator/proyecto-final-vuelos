# Databricks notebook source
# MAGIC %md
# MAGIC # Prueba de humo — ¿se puede registrar y servir un modelo propio en Free Edition?
# MAGIC
# MAGIC **Para qué sirve:** antes de invertir horas en cargar datos y entrenar, este notebook comprueba en
# MAGIC unos minutos la parte más incierta del proyecto: que tu workspace de Free Edition deja **registrar un
# MAGIC modelo propio en Unity Catalog** y, después (con `05_publicar_endpoint`), **servirlo en un endpoint**.
# MAGIC
# MAGIC **Qué hace**
# MAGIC 1. Inventa unos 5,000 vuelos **sintéticos** (no son datos reales) con las mismas 9 columnas del modelo.
# MAGIC 2. Entrena el mismo pipeline que usará el modelo real (`construir_pipeline("gradiente")`).
# MAGIC 3. Lo registra como `modelo_retrasos` con el alias `campeon`, igual que lo hará `04_entrenamiento`.
# MAGIC 4. Lo vuelve a cargar desde Unity Catalog y comprueba que predice y que rechaza una entrada inválida.
# MAGIC
# MAGIC **Importante:** este modelo **no sirve para predecir nada**; sus probabilidades salen de datos
# MAGIC inventados. Su única función es probar la tubería completa (registro, endpoint, aplicación). Cuando
# MAGIC corras `04_entrenamiento`, el modelo real se registra como una versión nueva y se queda con el alias.
# MAGIC
# MAGIC **Qué deberías ver al final:** `OK: modelo de prueba registrado: workspace.vuelos.modelo_retrasos versión 1 (alias campeon)`
# MAGIC (el número de versión puede ser otro si ya habías corrido este notebook) y la indicación de seguir con `05_publicar_endpoint`.

# COMMAND ----------

# MAGIC %run ./_config

# COMMAND ----------

# MAGIC %run ./_modelo

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Vuelos sintéticos
# MAGIC
# MAGIC Se usan 17 aeropuertos grandes (incluyen todos los de las rutas de ejemplo de la aplicación) con sus coordenadas reales para que la distancia de cada ruta sea
# MAGIC creíble, y la duración se calcula a unos 480 mph más 25 minutos de despegue y aterrizaje. Así todas
# MAGIC las filas cumplen las mismas reglas que valida el modelo (por ejemplo, velocidad entre 40 y 700 mph).
# MAGIC La etiqueta depende un poco de la hora de salida (más retrasos en la tarde), para que el modelo
# MAGIC tenga algo que aprender.

# COMMAND ----------

import numpy as np
import pandas as pd

FILAS_SINTETICAS = 5_000
SEMILLA = 42

# (latitud, longitud) aproximadas
AEROPUERTOS_PRUEBA = {
    "ATL": (33.64, -84.43), "DFW": (32.90, -97.04), "DEN": (39.86, -104.67), "ORD": (41.97, -87.91),
    "LAX": (33.94, -118.41), "JFK": (40.64, -73.78), "LAS": (36.08, -115.15), "MCO": (28.43, -81.31),
    "CLT": (35.21, -80.94), "SEA": (47.45, -122.31), "PHX": (33.43, -112.01), "MIA": (25.79, -80.29),
    "SFO": (37.62, -122.38), "IAH": (29.98, -95.34), "EWR": (40.69, -74.17), "BOS": (42.36, -71.01),
    "LGA": (40.78, -73.87),
}


def distancia_millas(a, b):
    """Distancia de gran círculo entre dos aeropuertos (fórmula de haversine)."""
    lat1, lon1 = np.radians(AEROPUERTOS_PRUEBA[a])
    lat2, lon2 = np.radians(AEROPUERTOS_PRUEBA[b])
    h = np.sin((lat2 - lat1) / 2) ** 2 + np.cos(lat1) * np.cos(lat2) * np.sin((lon2 - lon1) / 2) ** 2
    return float(2 * 3958.8 * np.arcsin(np.sqrt(h)))


# Rutas posibles: pares distintos a más de 150 millas (JFK-EWR, por ejemplo, queda fuera).
rutas = [
    (a, b, distancia_millas(a, b))
    for a in AEROPUERTOS_PRUEBA
    for b in AEROPUERTOS_PRUEBA
    if a != b and distancia_millas(a, b) >= 150
]

rng = np.random.default_rng(SEMILLA)
elegidas = rng.integers(0, len(rutas), FILAS_SINTETICAS)
origen = [rutas[i][0] for i in elegidas]
destino = [rutas[i][1] for i in elegidas]
distancia = np.round([rutas[i][2] for i in elegidas], 0)
duracion = np.round(distancia / 480 * 60 + 25 + rng.uniform(-10, 20, FILAS_SINTETICAS), 0)
hora_salida = rng.integers(5, 24, FILAS_SINTETICAS)
mes = rng.integers(1, 13, FILAS_SINTETICAS)

sinteticos = pd.DataFrame(
    {
        "mes": mes.astype("int64"),
        "dia_semana": rng.integers(1, 8, FILAS_SINTETICAS).astype("int64"),
        "hora_salida_prog": hora_salida.astype("int64"),
        "hora_llegada_prog": ((hora_salida + duracion // 60 + 1) % 24).astype("int64"),
        "distancia_millas": distancia.astype("float64"),
        "duracion_prog_min": duracion.astype("float64"),
        "aerolinea": rng.choice(sorted(AEROLINEAS), FILAS_SINTETICAS),
        "origen": origen,
        "destino": destino,
    }
)
prob_real = 0.08 + 0.02 * (hora_salida - 5) + 0.05 * np.isin(mes, [6, 7, 12])
sinteticos[ETIQUETA] = (rng.uniform(0, 1, FILAS_SINTETICAS) < prob_real).astype("int64")

velocidad = sinteticos["distancia_millas"] / (sinteticos["duracion_prog_min"] / 60)
assert velocidad.between(40, 700).all(), "Algún vuelo sintético rompe la regla de velocidad"
assert sinteticos["duracion_prog_min"].between(15, 900).all()

print(f"Vuelos sintéticos: {len(sinteticos):,} | rutas distintas: {len(rutas)} | % retrasados: {100 * sinteticos[ETIQUETA].mean():.1f}")
print(f"Velocidad implícita: de {velocidad.min():.0f} a {velocidad.max():.0f} mph")
display(sinteticos.head(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Entrenar el pipeline y registrarlo
# MAGIC El umbral de decisión se fija en 0.5 porque aquí no importa la calidad; el notebook 04 sí lo elige con cuidado.

# COMMAND ----------

pipeline_prueba = construir_pipeline("gradiente")
pipeline_prueba.fit(sinteticos[COLUMNAS_MODELO], sinteticos[ETIQUETA])
print(pipeline_prueba)

# COMMAND ----------

import mlflow

mlflow.sklearn.autolog(disable=True)  # registramos a mano; así no se crean corridas extra
usuario = spark.sql("SELECT current_user()").first()[0]
mlflow.set_experiment(f"/Users/{usuario}/vuelos_retrasos")

with mlflow.start_run(run_name="prueba_de_humo"):
    mlflow.log_params({"tipo": "PRUEBA con datos sintéticos", "filas": len(sinteticos), "umbral": 0.5})
    version_prueba = registrar_modelo(
        pipeline_prueba,
        umbral=0.5,
        ejemplo=sinteticos[COLUMNAS_MODELO].head(3),
        aerolineas=sorted(AEROLINEAS),
        aeropuertos=sorted(AEROPUERTOS_PRUEBA),
        nombre_modelo=NOMBRE_MODELO,
        alias=ALIAS_MODELO,
        descripcion=(
            "MODELO DE PRUEBA entrenado con datos SINTÉTICOS (prueba_de_humo). Solo sirve para validar el "
            "registro, el endpoint y la aplicación; sus predicciones no significan nada."
        ),
    )
print(f"Registrado {NOMBRE_MODELO} versión {version_prueba}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Cargarlo de vuelta y probarlo
# MAGIC Se carga **desde Unity Catalog** (no desde la memoria del notebook), igual que lo hará el endpoint.

# COMMAND ----------

from mlflow.exceptions import MlflowException

modelo_cargado = mlflow.pyfunc.load_model(f"models:/{NOMBRE_MODELO}/{version_prueba}")

vuelo_valido = pd.DataFrame(
    [{"mes": 7, "dia_semana": 5, "hora_salida_prog": 18, "hora_llegada_prog": 21,
      "distancia_millas": 1947.0, "duracion_prog_min": 285.0, "aerolinea": "DL", "origen": "ATL", "destino": "LAX"}]
)
print("Predicción para un vuelo válido:")
display(modelo_cargado.predict(vuelo_valido))

vuelo_invalido = vuelo_valido.assign(mes=13, destino="ATL")
try:
    modelo_cargado.predict(vuelo_invalido)
    raise AssertionError("El modelo debió rechazar mes=13 y origen igual a destino")
except MlflowException as error:
    print("Rechazo esperado para un vuelo inválido:")
    print("  ", error.message)

# COMMAND ----------

print(f"OK: modelo de prueba registrado: {NOMBRE_MODELO} versión {version_prueba} (alias {ALIAS_MODELO})")
print("Siguiente paso: corre el notebook 05_publicar_endpoint para crear el endpoint y probarlo en vivo.")
