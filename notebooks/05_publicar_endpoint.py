# Databricks notebook source
# MAGIC %md
# MAGIC # 05 · Publicar el modelo en un endpoint REST y probarlo en vivo
# MAGIC
# MAGIC **Objetivo:** servir la versión del modelo que tiene el alias `campeon` en un endpoint de **Model
# MAGIC Serving** (`vuelos-retrasos`), comprobar con peticiones reales que responde bien a un vuelo válido y
# MAGIC que **rechaza** las entradas inválidas, y guardar su esquema **OpenAPI** como documentación de la API.
# MAGIC
# MAGIC **Este notebook se corre dos veces en la vida del proyecto:**
# MAGIC 1. Después de `prueba_de_humo`: crea el endpoint con el modelo de prueba (confirma que Free Edition lo permite).
# MAGIC 2. Después de `04_entrenamiento`: actualiza el mismo endpoint a la versión del modelo real.
# MAGIC Si lo corres cuando el endpoint ya sirve la versión del alias, no cambia nada y solo repite las pruebas.
# MAGIC
# MAGIC | Paso | Qué se hace |
# MAGIC |---|---|
# MAGIC | 1 | Averiguar qué versión tiene el alias `campeon` |
# MAGIC | 2 | Crear o actualizar el endpoint (`workload_size = Small`, escala a cero) |
# MAGIC | 3 | Esperar a que quede listo: **la primera vez tarda unos 10 minutos; una actualización, de 10 a 25** |
# MAGIC | 4 | Pruebas en vivo: 2 vuelos válidos y 7 inválidos |
# MAGIC | 5 | Esquema OpenAPI (se guarda en el volumen y, si se puede, en `docs/openapi.json` del repositorio) |
# MAGIC | 6 | Ejemplo `curl` listo para copiar |
# MAGIC
# MAGIC **Qué deberías ver al final:** `OK: endpoint vuelos-retrasos listo, sirviendo la versión N` y
# MAGIC `OK: las 9 pruebas del endpoint pasaron`.
# MAGIC
# MAGIC Si la espera termina en error, abre **Serving** en el menú izquierdo, entra a `vuelos-retrasos` y
# MAGIC revisa las pestañas **Events** y **Logs**: ahí aparece la causa (por ejemplo, una librería que no se instaló).

# COMMAND ----------

# MAGIC %run ./_config

# COMMAND ----------

# MAGIC %run ./_modelo

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. ¿Qué versión se va a publicar?

# COMMAND ----------

from mlflow import MlflowClient

configurar_registro()
cliente = MlflowClient()
version_alias = cliente.get_model_version_by_alias(NOMBRE_MODELO, ALIAS_MODELO)
VERSION = str(version_alias.version)
print(f"Modelo : {NOMBRE_MODELO}")
print(f"Alias  : {ALIAS_MODELO} -> versión {VERSION}")
print(f"Descripción de la versión: {version_alias.description or '(sin descripción)'}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Crear o actualizar el endpoint, y 3. esperar a que quede listo
# MAGIC La celda imprime el estado cada 30 segundos. Mientras dice `IN_PROGRESS`, Databricks está construyendo
# MAGIC la imagen con las librerías del modelo y cargándolo. No canceles la celda: si la cancelas, el
# MAGIC despliegue sigue de todos modos y puedes volver a correrla para seguir esperando.

# COMMAND ----------

accion = publicar_endpoint(NOMBRE_ENDPOINT, NOMBRE_MODELO, VERSION)
print(f"Acción: {accion}")
datos_endpoint = esperar_endpoint(NOMBRE_ENDPOINT, version=VERSION)

servidas = _versiones_servidas(datos_endpoint.get("config"), NOMBRE_MODELO)
assert VERSION in servidas, f"El endpoint quedó listo pero sirve {servidas}, no la versión {VERSION}"
print(f"El endpoint {NOMBRE_ENDPOINT} sirve {NOMBRE_MODELO} versión(es) {', '.join(servidas)}.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Pruebas en vivo
# MAGIC
# MAGIC Cada prueba manda una petición real al endpoint (formato `dataframe_split`, como la aplicación).
# MAGIC Los vuelos válidos deben recibir **HTTP 200** con una probabilidad entre 0 y 1; los inválidos deben
# MAGIC recibir un error **4xx** con un mensaje que explique qué está mal. Que la API rechace lo absurdo es
# MAGIC tan importante como que acierte en lo válido.
# MAGIC
# MAGIC La primera petición puede tardar o devolver 503/504 si el endpoint estaba dormido (escala a cero):
# MAGIC por eso las pruebas reintentan unas veces en ese caso.

# COMMAND ----------

import json
import time

import pandas as pd

VUELO_VALIDO = {
    "mes": 7, "dia_semana": 5, "hora_salida_prog": 18, "hora_llegada_prog": 21,
    "distancia_millas": 1947.0, "duracion_prog_min": 285.0, "aerolinea": "DL", "origen": "ATL", "destino": "LAX",
}
sin_destino = {k: v for k, v in VUELO_VALIDO.items() if k != "destino"}

# (nombre de la prueba, fila que se manda, resultado esperado)
PRUEBAS = [
    ("vuelo válido (DL, ATL -> LAX)", VUELO_VALIDO, "200"),
    ("válido con minúsculas y espacios ( dl , atl, lax )", {**VUELO_VALIDO, "aerolinea": " dl ", "origen": "atl", "destino": "lax "}, "200"),
    ("tipo incorrecto: mes='julio'", {**VUELO_VALIDO, "mes": "julio"}, "4xx"),
    ("mes fuera de rango: 13", {**VUELO_VALIDO, "mes": 13}, "4xx"),
    ("aerolínea desconocida: ZZ", {**VUELO_VALIDO, "aerolinea": "ZZ"}, "4xx"),
    ("aeropuerto desconocido: XXX", {**VUELO_VALIDO, "origen": "XXX"}, "4xx"),
    ("falta la columna destino", sin_destino, "4xx"),
    ("origen igual a destino", {**VUELO_VALIDO, "destino": "ATL"}, "4xx"),
    ("velocidad imposible: 2,000 millas en 30 min", {**VUELO_VALIDO, "distancia_millas": 2000.0, "duracion_prog_min": 30.0}, "4xx"),
]
INTENTOS = 4
ESPERA_REINTENTO_SEGUNDOS = 15


def consultar_con_reintentos(fila):
    for intento in range(1, INTENTOS + 1):
        try:
            status, cuerpo, segundos = consultar_endpoint(NOMBRE_ENDPOINT, [fila])
        except Exception as error:  # tiempo agotado, conexión caída
            status, cuerpo, segundos = None, f"{type(error).__name__}: {error}", None
        if status is not None and status != 429 and status < 500:
            return status, cuerpo, segundos
        if intento < INTENTOS:
            print(f"   intento {intento}: {status} -> se reintenta en {ESPERA_REINTENTO_SEGUNDOS} s")
            time.sleep(ESPERA_REINTENTO_SEGUNDOS)
    return status, cuerpo, segundos


def revisar(esperado, status, cuerpo):
    """Devuelve (pasa, mensaje)."""
    if esperado == "200":
        if status != 200:
            return False, f"se esperaba 200: {cuerpo}"
        try:
            prediccion = cuerpo["predictions"][0]
            probabilidad = float(prediccion["probabilidad_retraso"])
            umbral_respuesta = float(prediccion["umbral_decision"])
            predicho = int(prediccion["retraso_predicho"])
        except (KeyError, IndexError, TypeError, ValueError):
            return False, f"respuesta 200 sin el formato del contrato: {cuerpo}"
        coherente = predicho == int(probabilidad >= umbral_respuesta)
        pasa = 0.0 <= probabilidad <= 1.0 and predicho in (0, 1) and coherente
        return pasa, f"probabilidad_retraso={probabilidad}, retraso_predicho={predicho}, umbral_decision={umbral_respuesta}"
    pasa = status is not None and 400 <= status < 500
    return pasa, mensaje_legible(cuerpo)


def mensaje_legible(cuerpo):
    """El servidor de MLflow antepone 'Failed to predict data ...' al mensaje del modelo: se recorta."""
    mensaje = str(cuerpo.get("message", cuerpo) if isinstance(cuerpo, dict) else cuerpo)
    inicio = mensaje.find("Entrada inválida:")
    if inicio >= 0:
        return mensaje[inicio:]
    if mensaje.startswith("Failed to predict data") and "Error: " in mensaje:
        return mensaje[mensaje.rfind("Error: ") + len("Error: "):]
    return mensaje


resultados_pruebas = []
respuestas_crudas = {}
for nombre, fila, esperado in PRUEBAS:
    status, cuerpo, segundos = consultar_con_reintentos(fila)
    pasa, mensaje = revisar(esperado, status, cuerpo)
    respuestas_crudas[nombre] = (status, cuerpo)
    resultados_pruebas.append((nombre, esperado, status, segundos, "PASA" if pasa else "FALLA", mensaje[:220]))
    print(f"[{'PASA ' if pasa else 'FALLA'}] {nombre}: HTTP {status} -> {mensaje[:160]}")

tabla_pruebas = pd.DataFrame(resultados_pruebas, columns=["prueba", "esperado", "status", "segundos", "resultado", "mensaje"])
display(tabla_pruebas)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.1 Cuerpos exactos de las respuestas
# MAGIC Esto es lo que la aplicación debe saber leer: el JSON de una respuesta correcta y el de un error.
# MAGIC
# MAGIC En un error, el servidor de MLflow **envuelve** el mensaje del modelo: `message` empieza con
# MAGIC `Failed to predict data '...'` (una copia de los datos recibidos) y el texto útil viene después, desde
# MAGIC `Entrada inválida:`. Los errores de tipo (por ejemplo `mes="julio"`) los rechaza la firma antes de llegar
# MAGIC al modelo, con `error_code` `BAD_REQUEST` y un mensaje en inglés.

# COMMAND ----------

nombre_valido = PRUEBAS[0][0]
nombre_error = PRUEBAS[3][0]
print(f"Respuesta a '{nombre_valido}' (HTTP {respuestas_crudas[nombre_valido][0]}):")
print(json.dumps(respuestas_crudas[nombre_valido][1], ensure_ascii=False, indent=2))
print(f"Respuesta a '{nombre_error}' (HTTP {respuestas_crudas[nombre_error][0]}):")
print(json.dumps(respuestas_crudas[nombre_error][1], ensure_ascii=False, indent=2))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Esquema OpenAPI (documentación de la API)
# MAGIC Databricks lo genera a partir de la firma del modelo. Se guarda en el volumen y, si este notebook está
# MAGIC dentro de la carpeta del repositorio, también en `docs/openapi.json` (luego haz commit desde la carpeta Git).

# COMMAND ----------

import os


def resumir_y_guardar_openapi(esquema_openapi):
    print("Versión de OpenAPI:", esquema_openapi.get("openapi"))
    print("Título:", (esquema_openapi.get("info") or {}).get("title"))
    for servidor in esquema_openapi.get("servers") or []:
        print("Servidor:", servidor.get("url"))
    for ruta, operaciones in (esquema_openapi.get("paths") or {}).items():
        for metodo, operacion in (operaciones or {}).items():
            if not isinstance(operacion, dict):
                continue
            formatos = []
            for contenido in ((operacion.get("requestBody") or {}).get("content") or {}).values():
                esquema_cuerpo = contenido.get("schema") or {}
                for opcion in esquema_cuerpo.get("oneOf") or esquema_cuerpo.get("anyOf") or [esquema_cuerpo]:
                    formatos += list((opcion.get("properties") or {}).keys()) or [opcion.get("$ref", "?")]
            print(f"  {metodo.upper()} {ruta} -> formatos de entrada: {', '.join(formatos) or '(ver el JSON)'}")

    texto_openapi = json.dumps(esquema_openapi, ensure_ascii=False, indent=2)
    carpeta_volumen = f"{RUTA_VOLUMEN}/docs"
    os.makedirs(carpeta_volumen, exist_ok=True)
    with open(f"{carpeta_volumen}/openapi.json", "w", encoding="utf-8") as archivo:
        archivo.write(texto_openapi)
    print(f"Guardado en el volumen: {carpeta_volumen}/openapi.json")

    try:
        carpeta_repo = os.path.abspath(os.path.join(os.getcwd(), "..", "docs"))
        if os.path.exists(os.path.join(carpeta_repo, "CONTEXTO_TECNICO.md")):
            with open(os.path.join(carpeta_repo, "openapi.json"), "w", encoding="utf-8") as archivo:
                archivo.write(texto_openapi)
            print(f"Guardado en el repositorio: {os.path.join(carpeta_repo, 'openapi.json')}")
        else:
            print(f"No encontré la carpeta docs del repositorio junto a este notebook ({carpeta_repo}); "
                  "descarga el archivo del volumen y súbelo a docs/openapi.json.")
    except Exception as error:
        print(f"No se pudo guardar en el repositorio ({type(error).__name__}: {error}); usa la copia del volumen.")


# El esquema en vivo es una función en vista previa de Databricks: si tu workspace no la ofrece, el
# notebook NO se detiene. El repositorio ya trae docs/openapi.json, escrito a partir de la firma del modelo.
try:
    resumir_y_guardar_openapi(obtener_openapi(NOMBRE_ENDPOINT))
except Exception as error:
    print("AVISO: no se pudo obtener el esquema OpenAPI del endpoint en vivo.")
    print(f"  Detalle: {type(error).__name__}: {error}")
    print("  No afecta al endpoint ni a las pruebas. Queda como documentación el archivo docs/openapi.json "
          "del repositorio, escrito a partir de la firma del modelo.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Ejemplo para consultar la API desde fuera de Databricks
# MAGIC Necesitas un token personal (**Settings > Developer > Access tokens**). Guárdalo en la variable de
# MAGIC entorno `DATABRICKS_TOKEN`; nunca lo pegues en el código ni en el repositorio.

# COMMAND ----------

host, _ = credenciales()
cuerpo_ejemplo = json.dumps(
    {"dataframe_split": {"columns": COLUMNAS_MODELO, "data": [[VUELO_VALIDO[c] for c in COLUMNAS_MODELO]]}}
)
print("Ejemplo para bash, macOS o Git Bash (una sola línea):")
print()
print(
    f'curl -X POST "{host}/serving-endpoints/{NOMBRE_ENDPOINT}/invocations" '
    f'-H "Authorization: Bearer $DATABRICKS_TOKEN" -H "Content-Type: application/json" '
    f"-d '{cuerpo_ejemplo}'"
)

# COMMAND ----------

status_final, datos_final = llamar_api("GET", f"/api/2.0/serving-endpoints/{NOMBRE_ENDPOINT}")
_exigir_ok(status_final, datos_final, "consultar el endpoint")
version_servida = ", ".join(_versiones_servidas(datos_final.get("config"), NOMBRE_MODELO)) or "ninguna"
print(f"OK: endpoint {NOMBRE_ENDPOINT} listo, sirviendo la versión {version_servida}")

fallidas = [r for r in resultados_pruebas if r[4] != "PASA"]
if fallidas:
    print(f"FALLARON {len(fallidas)} de {len(resultados_pruebas)} pruebas del endpoint:")
    for nombre, esperado, status, _, _, mensaje in fallidas:
        print(f"  - {nombre}: se esperaba {esperado} y llegó {status} -> {mensaje}")
    raise AssertionError("Hay pruebas del endpoint que fallaron (ver la lista de arriba)")
print(f"OK: las {len(resultados_pruebas)} pruebas del endpoint pasaron")
