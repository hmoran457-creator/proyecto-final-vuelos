"""Plan B: API FastAPI con el mismo contrato que Model Serving.

Se usa solo si Model Serving no está disponible en tu workspace. Carga el modelo registrado en
Unity Catalog una vez al arrancar y lo expone en:

  GET  /health                                  estado de la API
  POST /predict                                 un vuelo (o una lista) con validación de Pydantic
  POST /serving-endpoints/{nombre}/invocations  igual que Model Serving (dataframe_split o dataframe_records)
  GET  /docs                                    Swagger

Para correrla en tu computadora: ``python main.py`` (usa el puerto de DATABRICKS_APP_PORT, o 8000).
"""

import json
import logging
import os
from contextlib import asynccontextmanager

import pandas as pd
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from mlflow.exceptions import MlflowException
from pydantic import BaseModel, ConfigDict, Field

logger = logging.getLogger("api_vuelos")

# Columnas del modelo, en el orden del contrato (docs/CONTEXTO_TECNICO.md, sección 6).
COLUMNAS_MODELO = [
    "mes", "dia_semana", "hora_salida_prog", "hora_llegada_prog",
    "distancia_millas", "duracion_prog_min", "aerolinea", "origen", "destino",
]
MAXIMO_FILAS = 1000
# Nombre que responde la ruta /serving-endpoints/{nombre}/invocations (otro nombre da 404, como en Model Serving).
NOMBRE_ENDPOINT = os.environ.get("NOMBRE_ENDPOINT", "vuelos-retrasos")
# Modelo por defecto: ajusta el catálogo si el tuyo no se llama "workspace", o define MODEL_URI en app.yaml.
MODEL_URI_POR_DEFECTO = "models:/workspace.vuelos.modelo_retrasos@campeon"


# --- Carga del modelo (separada para poder inyectar un modelo falso en las pruebas) ------------------
_modelo = None
_error_carga = None


def cargar_modelo():
    """Carga el modelo registrado en Unity Catalog con mlflow."""
    import mlflow

    # Registro de Unity Catalog solo si corremos en Databricks y nadie definió MLFLOW_REGISTRY_URI;
    # en cualquier otro caso (pruebas locales) se respeta la configuración de MLflow.
    en_databricks = bool(os.environ.get("DATABRICKS_HOST") or os.environ.get("DATABRICKS_APP_NAME"))
    if not os.environ.get("MLFLOW_REGISTRY_URI") and en_databricks:
        mlflow.set_registry_uri("databricks-uc")
    return mlflow.pyfunc.load_model(os.environ.get("MODEL_URI", MODEL_URI_POR_DEFECTO))


def fijar_modelo(modelo):
    """Usa un modelo ya construido (lo usan las pruebas)."""
    global _modelo, _error_carga
    _modelo, _error_carga = modelo, None


@asynccontextmanager
async def ciclo_de_vida(app):
    global _modelo, _error_carga
    if _modelo is None:
        try:
            _modelo = cargar_modelo()
            logger.info("Modelo cargado.")
        except Exception as exc:  # noqa: BLE001 - la API arranca igual y /health explica el problema
            _error_carga = f"{type(exc).__name__}: {exc}"
            logger.exception("No se pudo cargar el modelo")
    yield


app = FastAPI(
    title="API de retrasos de vuelos (plan B)",
    description="Mismo contrato que el endpoint de Model Serving. Predice si un vuelo llegará con 15 minutos o más de retraso.",
    version="1.0",
    lifespan=ciclo_de_vida,
)


# --- Esquema de entrada ------------------------------------------------------------------------------
class Vuelo(BaseModel):
    """Un vuelo con los datos que se conocen antes del despegue."""

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "mes": 7, "dia_semana": 5, "hora_salida_prog": 18, "hora_llegada_prog": 21,
                "distancia_millas": 1947.0, "duracion_prog_min": 285.0,
                "aerolinea": "DL", "origen": "ATL", "destino": "LAX",
            }
        }
    )

    mes: int = Field(ge=1, le=12, description="Mes del año (1 a 12).")
    dia_semana: int = Field(ge=1, le=7, description="Día de la semana: 1 = lunes ... 7 = domingo.")
    hora_salida_prog: int = Field(ge=0, le=23, description="Hora programada de salida (0 a 23).")
    hora_llegada_prog: int = Field(ge=0, le=23, description="Hora programada de llegada (0 a 23).")
    distancia_millas: float = Field(gt=0, le=6000, description="Distancia en millas (mayor que 0 y hasta 6,000).")
    duracion_prog_min: float = Field(ge=15, le=900, description="Duración programada en minutos (15 a 900).")
    aerolinea: str = Field(min_length=2, max_length=3, description="Código de la aerolínea, por ejemplo DL.")
    origen: str = Field(min_length=3, max_length=3, description="Código IATA del aeropuerto de origen, por ejemplo ATL.")
    destino: str = Field(min_length=3, max_length=3, description="Código IATA del aeropuerto de destino, por ejemplo LAX.")


# --- Errores con el mismo formato que Model Serving --------------------------------------------------
class ErrorAPI(Exception):
    def __init__(self, status, error_code, message):
        self.status, self.error_code, self.message = status, error_code, message


@app.exception_handler(ErrorAPI)
async def manejar_error_api(_peticion, exc):
    return JSONResponse(status_code=exc.status, content={"error_code": exc.error_code, "message": exc.message})


# --- Lógica de predicción ------------------------------------------------------------------------------
def _a_lista_de_dicts(salida):
    """Convierte lo que devuelve el modelo (DataFrame o lista) en una lista de diccionarios JSON."""
    if isinstance(salida, pd.DataFrame):
        return json.loads(salida.to_json(orient="records"))
    if isinstance(salida, dict) and "predictions" in salida:
        salida = salida["predictions"]
    return json.loads(json.dumps(list(salida), default=lambda x: x.item() if hasattr(x, "item") else str(x)))


COLUMNAS_ENTERAS = ["mes", "dia_semana", "hora_salida_prog", "hora_llegada_prog"]
COLUMNAS_DECIMALES = ["distancia_millas", "duracion_prog_min"]


def _es_numero(valor):
    return isinstance(valor, (int, float)) and not isinstance(valor, bool) and valor == valor and abs(valor) != float("inf")


def preparar_tabla(filas):
    """Arma el DataFrame con los tipos de la firma: enteros a int64 y decimales a float64.

    El modelo cargado en este proceso es más estricto que el servidor HTTP (rechaza int64 en una columna
    double). Los valores que no son números (o enteros con decimales) se dejan tal cual para que
    la validación del propio modelo los reporte.
    """
    limpias = []
    for fila in filas:
        fila = dict(fila)
        for col in COLUMNAS_DECIMALES:
            if _es_numero(fila.get(col)):
                fila[col] = float(fila[col])
        for col in COLUMNAS_ENTERAS:
            valor = fila.get(col)
            if _es_numero(valor) and valor == int(valor):
                fila[col] = int(valor)
        limpias.append(fila)
    return pd.DataFrame(limpias, columns=COLUMNAS_MODELO)


def predecir(filas):
    """Pasa las filas (lista de diccionarios) al modelo y arma {"predictions": [...]}."""
    if _modelo is None:
        raise ErrorAPI(503, "SERVICE_UNAVAILABLE", f"El modelo no está cargado. {_error_carga or ''}".strip())
    try:
        salida = _modelo.predict(preparar_tabla(filas))
    except MlflowException as exc:
        raise ErrorAPI(400, exc.error_code, exc.message) from exc
    except Exception as exc:  # noqa: BLE001
        logger.exception("Error inesperado al predecir")
        raise ErrorAPI(500, "INTERNAL_ERROR", f"Error interno al predecir: {type(exc).__name__}") from exc
    return {"predictions": _a_lista_de_dicts(salida)}


# --- Rutas ---------------------------------------------------------------------------------------------
@app.get("/health", summary="Estado de la API")
def salud():
    return {"estado": "ok" if _modelo is not None else "sin_modelo", "modelo_cargado": _modelo is not None, "error": _error_carga}


@app.post("/predict", summary="Predice el retraso de uno o varios vuelos")
def predict(vuelos: Vuelo | list[Vuelo]):
    lista = vuelos if isinstance(vuelos, list) else [vuelos]
    if not 1 <= len(lista) <= MAXIMO_FILAS:
        raise ErrorAPI(400, "INVALID_PARAMETER_VALUE", f"Envía entre 1 y {MAXIMO_FILAS} vuelos.")
    return predecir([v.model_dump() for v in lista])


@app.post("/serving-endpoints/{nombre}/invocations", summary="Mismo contrato que Model Serving")
async def invocar(nombre: str, peticion: Request):
    if nombre != NOMBRE_ENDPOINT:
        raise ErrorAPI(404, "RESOURCE_DOES_NOT_EXIST", f"No existe el endpoint '{nombre}'.")
    try:
        cuerpo = await peticion.json()
    except ValueError as exc:
        raise ErrorAPI(400, "MALFORMED_REQUEST", "El cuerpo de la petición no es JSON válido.") from exc
    if not isinstance(cuerpo, dict):
        raise ErrorAPI(400, "MALFORMED_REQUEST", "El cuerpo debe ser un objeto JSON.")

    if "dataframe_split" in cuerpo:
        partes = cuerpo["dataframe_split"]
        if not isinstance(partes, dict) or not isinstance(partes.get("columns"), list) or not isinstance(partes.get("data"), list):
            raise ErrorAPI(400, "MALFORMED_REQUEST", "dataframe_split debe traer 'columns' y 'data'.")
        columnas = partes["columns"]
        try:
            filas = [dict(zip(columnas, fila, strict=True)) for fila in partes["data"]]
        except (TypeError, ValueError) as exc:
            raise ErrorAPI(400, "MALFORMED_REQUEST", "Cada fila de 'data' debe tener una celda por columna.") from exc
    elif "dataframe_records" in cuerpo:
        filas = cuerpo["dataframe_records"]
        if not isinstance(filas, list) or not all(isinstance(f, dict) for f in filas):
            raise ErrorAPI(400, "MALFORMED_REQUEST", "dataframe_records debe ser una lista de objetos.")
    else:
        raise ErrorAPI(400, "MALFORMED_REQUEST", "Falta 'dataframe_split' o 'dataframe_records' en el cuerpo.")

    if not 1 <= len(filas) <= MAXIMO_FILAS:
        raise ErrorAPI(400, "INVALID_PARAMETER_VALUE", f"Envía entre 1 y {MAXIMO_FILAS} filas.")
    faltantes = [c for c in COLUMNAS_MODELO if any(c not in f for f in filas)]
    if faltantes:
        raise ErrorAPI(400, "INVALID_PARAMETER_VALUE", f"Faltan columnas: {faltantes}.")
    return predecir(filas)


if __name__ == "__main__":
    import uvicorn

    puerto = int(os.environ.get("DATABRICKS_APP_PORT") or os.environ.get("PORT") or 8000)
    uvicorn.run(app, host="0.0.0.0", port=puerto)
