"""Pruebas de la API FastAPI (plan B) con un modelo falso, más una integración con el cliente real."""

import os
import subprocess
import sys
import time

import pytest

pytest.importorskip("fastapi")
pytest.importorskip("mlflow")
from fastapi.testclient import TestClient  # noqa: E402
from mlflow.exceptions import MlflowException  # noqa: E402

import cliente_endpoint as ce  # noqa: E402
import main  # noqa: E402
from conftest import DIR_API, PUERTO_API, VUELO  # noqa: E402
from stub_modelo import ModeloFalso  # noqa: E402

COLUMNAS = ["mes", "dia_semana", "hora_salida_prog", "hora_llegada_prog",
            "distancia_millas", "duracion_prog_min", "aerolinea", "origen", "destino"]
SPLIT = {"dataframe_split": {"columns": COLUMNAS, "data": [[VUELO[c] for c in COLUMNAS]]}}
RUTA = "/serving-endpoints/vuelos-retrasos/invocations"


@pytest.fixture
def cliente():
    modelo = ModeloFalso()
    main.fijar_modelo(modelo)
    yield TestClient(main.app), modelo
    main.fijar_modelo(None)


def test_health(cliente):
    http, _ = cliente
    r = http.get("/health")
    assert r.status_code == 200 and r.json()["modelo_cargado"] is True


def test_predict_un_vuelo(cliente):
    http, modelo = cliente
    r = http.post("/predict", json=VUELO)
    assert r.status_code == 200
    assert r.json() == {"predictions": [{"probabilidad_retraso": 0.6, "retraso_predicho": 1, "umbral_decision": 0.41}]}
    assert list(modelo.entradas[0].columns) == COLUMNAS  # orden del contrato


def test_predict_lista_de_vuelos(cliente):
    http, _ = cliente
    r = http.post("/predict", json=[VUELO, dict(VUELO, hora_salida_prog=8)])
    assert r.status_code == 200
    assert [p["probabilidad_retraso"] for p in r.json()["predictions"]] == [0.6, 0.25]


@pytest.mark.parametrize(
    "cambio",
    [{"mes": 13}, {"dia_semana": 0}, {"hora_salida_prog": 24}, {"distancia_millas": 0}, {"duracion_prog_min": 5},
     {"origen": "AT"}, {"aerolinea": "X"}, {"mes": "julio"}],
)
def test_predict_pydantic_rechaza_con_422(cliente, cambio):
    http, modelo = cliente
    assert http.post("/predict", json=dict(VUELO, **cambio)).status_code == 422
    assert modelo.entradas == []  # ni siquiera llegó al modelo


def test_predict_campo_faltante_422(cliente):
    http, _ = cliente
    sin_mes = {k: v for k, v in VUELO.items() if k != "mes"}
    assert http.post("/predict", json=sin_mes).status_code == 422


def test_invocations_dataframe_split(cliente):
    http, _ = cliente
    r = http.post(RUTA, json=SPLIT)
    assert r.status_code == 200 and r.json()["predictions"][0]["probabilidad_retraso"] == 0.6


def test_invocations_dataframe_records(cliente):
    http, _ = cliente
    r = http.post(RUTA, json={"dataframe_records": [VUELO, dict(VUELO, hora_salida_prog=3)]})
    assert r.status_code == 200 and len(r.json()["predictions"]) == 2


def test_invocations_cualquier_nombre_distinto_da_404(cliente):
    http, _ = cliente
    r = http.post("/serving-endpoints/x/invocations", json=SPLIT)
    assert r.status_code == 404 and set(r.json()) == {"error_code", "message"}


@pytest.mark.parametrize(
    "cuerpo",
    [
        {"otra_cosa": 1},
        {"dataframe_split": {"columns": COLUMNAS}},
        {"dataframe_split": {"columns": COLUMNAS, "data": [[1, 2]]}},
        {"dataframe_records": [1, 2]},
        {"dataframe_records": [{"mes": 7}]},
        {"dataframe_records": []},
        [1, 2],
    ],
)
def test_invocations_cuerpos_mal_formados_dan_400(cliente, cuerpo):
    http, _ = cliente
    r = http.post(RUTA, json=cuerpo)
    assert r.status_code == 400 and "message" in r.json() and "error_code" in r.json()


def test_invocations_no_json_400(cliente):
    http, _ = cliente
    r = http.post(RUTA, content=b"esto no es json", headers={"Content-Type": "application/json"})
    assert r.status_code == 400


def test_invocations_mas_de_mil_filas_400(cliente):
    http, _ = cliente
    r = http.post(RUTA, json={"dataframe_records": [VUELO] * 1001})
    assert r.status_code == 400


def test_mlflow_exception_del_modelo_es_400(cliente):
    http, _ = cliente
    r = http.post(RUTA, json={"dataframe_split": dict(SPLIT["dataframe_split"], data=[[13] + [VUELO[c] for c in COLUMNAS[1:]]])})
    assert r.status_code == 400
    assert r.json()["error_code"] == "INVALID_PARAMETER_VALUE" and "mes" in r.json()["message"]


def test_excepcion_inesperada_es_500(cliente):
    http, modelo = cliente

    def explota(df):
        raise RuntimeError("secreto interno")

    modelo.predict = explota
    r = http.post(RUTA, json=SPLIT)
    assert r.status_code == 500 and r.json()["error_code"] == "INTERNAL_ERROR"
    assert "secreto interno" not in r.text  # no se filtran detalles internos


def test_sin_modelo_cargado_503():
    main.fijar_modelo(None)
    http = TestClient(main.app)
    assert http.get("/health").json()["modelo_cargado"] is False
    assert http.post(RUTA, json=SPLIT).status_code == 503


def test_lifespan_carga_el_modelo_y_sobrevive_si_falla(monkeypatch):
    main.fijar_modelo(None)
    monkeypatch.setattr(main, "cargar_modelo", lambda: ModeloFalso())
    with TestClient(main.app) as http:
        assert http.get("/health").json()["modelo_cargado"] is True
    main.fijar_modelo(None)

    def falla():
        raise OSError("sin acceso al registro")

    monkeypatch.setattr(main, "cargar_modelo", falla)
    with TestClient(main.app) as http:
        salud = http.get("/health").json()
        assert salud["modelo_cargado"] is False and "sin acceso" in salud["error"]
    main.fijar_modelo(None)


def test_openapi_contiene_el_esquema_vuelo(cliente):
    http, _ = cliente
    esquema = http.get("/openapi.json").json()
    vuelo = esquema["components"]["schemas"]["Vuelo"]
    assert set(vuelo["properties"]) == set(COLUMNAS)
    assert vuelo["properties"]["mes"]["minimum"] == 1 and vuelo["properties"]["mes"]["maximum"] == 12
    assert vuelo["example"]["origen"] == "ATL" and "description" in vuelo["properties"]["mes"]
    assert "/predict" in esquema["paths"]
    assert "/serving-endpoints/{nombre}/invocations" in esquema["paths"]


def test_preparar_tabla_castea_tipos():
    fila = dict(VUELO, distancia_millas=1947, duracion_prog_min=285, mes=7.0)
    tabla = main.preparar_tabla([fila])
    assert str(tabla["distancia_millas"].dtype) == "float64" and str(tabla["duracion_prog_min"].dtype) == "float64"
    for col in ("mes", "dia_semana", "hora_salida_prog", "hora_llegada_prog"):
        assert str(tabla[col].dtype) == "int64"
    # lo que no es entero o no es número se deja tal cual para que el modelo lo reporte
    raro = main.preparar_tabla([dict(VUELO, mes="julio", hora_salida_prog=7.5, distancia_millas="mucha")])
    assert raro["mes"][0] == "julio" and raro["hora_salida_prog"][0] == 7.5 and raro["distancia_millas"][0] == "mucha"


def test_el_modelo_recibe_los_tipos_casteados(cliente):
    http, modelo = cliente
    assert http.post(RUTA, json={"dataframe_records": [dict(VUELO, distancia_millas=1947, duracion_prog_min=285)]}).status_code == 200
    assert str(modelo.entradas[0]["distancia_millas"].dtype) == "float64"
    assert str(modelo.entradas[0]["mes"].dtype) == "int64"


def test_registro_databricks_uc_solo_en_databricks(monkeypatch):
    import types

    llamadas = []
    falso = types.SimpleNamespace(
        set_registry_uri=lambda uri: llamadas.append(uri),
        pyfunc=types.SimpleNamespace(load_model=lambda uri: ("modelo", uri)),
    )
    monkeypatch.setitem(sys.modules, "mlflow", falso)  # `import mlflow` dentro de cargar_modelo recibe este objeto
    for var in ("DATABRICKS_HOST", "DATABRICKS_APP_NAME", "MLFLOW_REGISTRY_URI"):
        monkeypatch.delenv(var, raising=False)
    main.cargar_modelo()
    assert llamadas == []  # local: no se toca la configuración
    monkeypatch.setenv("DATABRICKS_APP_NAME", "api-vuelos")
    main.cargar_modelo()
    assert llamadas == ["databricks-uc"]
    monkeypatch.setenv("MLFLOW_REGISTRY_URI", "sqlite:///x.db")
    main.cargar_modelo()
    assert llamadas == ["databricks-uc"]  # MLFLOW_REGISTRY_URI manda


def test_mlflow_exception_directa_usa_su_codigo():
    modelo = ModeloFalso()
    modelo.predict = lambda df: (_ for _ in ()).throw(MlflowException("falla del modelo", error_code=1))
    main.fijar_modelo(modelo)
    try:
        r = TestClient(main.app).post(RUTA, json=SPLIT)
        assert r.status_code == 400 and r.json()["message"] == "falla del modelo"
    finally:
        main.fijar_modelo(None)


# --- Integración: el cliente real contra la API real servida por uvicorn -----------------------------------
def _esperar_api(proceso, url, segundos=60):
    import requests

    limite = time.time() + segundos
    while time.time() < limite:
        if proceso.poll() is not None:
            raise RuntimeError(f"uvicorn terminó con código {proceso.returncode}")
        try:
            if requests.get(url + "/health", timeout=1).status_code == 200:
                return
        except requests.RequestException:
            time.sleep(0.3)
    raise RuntimeError("la API no arrancó a tiempo")


def test_integracion_cliente_real_contra_fastapi():
    aqui = os.path.dirname(os.path.abspath(__file__))
    url = f"http://127.0.0.1:{PUERTO_API}"
    proceso = subprocess.Popen(
        [sys.executable, os.path.join(aqui, "servir_api_stub.py"), str(PUERTO_API), DIR_API],
        cwd=aqui,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    try:
        _esperar_api(proceso, url)
        comunes = dict(host="http://127.0.0.1:1", url_base=url, encabezados={}, endpoint="vuelos-retrasos", reintentos=0)

        r = ce.consultar_prediccion(dict(VUELO), **comunes)
        assert (r.probabilidad_retraso, r.retraso_predicho, r.umbral_decision, r.status) == (0.6, 1, 0.41, 200)

        with pytest.raises(ce.ErrorEndpoint) as exc:  # el modelo rechaza mes=13 -> 400 con el mensaje de la API
            ce.consultar_prediccion(dict(VUELO, mes=13), **comunes)
        assert exc.value.tipo == "validacion" and "mes fuera de rango" in exc.value.mensaje

        with pytest.raises(ce.ErrorEndpoint) as exc:  # nombre de endpoint equivocado -> 404
            ce.consultar_prediccion(dict(VUELO), **dict(comunes, endpoint="vuelos-retrasos-no-existe"))
        assert exc.value.tipo == "no_encontrado"

        esquema = ce.obtener_openapi(url, {}, "vuelos-retrasos", ruta="/openapi.json")
        assert "Vuelo" in esquema["components"]["schemas"]
    finally:
        proceso.terminate()
        try:
            proceso.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proceso.kill()
