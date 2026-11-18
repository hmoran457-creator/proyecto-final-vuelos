"""Pruebas de la interfaz Streamlit con AppTest, contra un servidor HTTP falso."""

import json
import os
import time

import pytest

st = pytest.importorskip("streamlit")
from streamlit.testing.v1 import AppTest  # noqa: E402

import cliente_endpoint as ce  # noqa: E402
from conftest import DIR_APP, OK, Respuesta  # noqa: E402

RUTA_APP = os.path.join(DIR_APP, "app.py")
MODOS = {
    "normal": "Normal",
    "timeout": "Tiempo de espera agotado",
    "http": "Error HTTP: la API rechaza la entrada",
    "404": "Endpoint inexistente (404)",
    "invalida": "Respuesta inválida",
}
ESTADO_LISTO = {"state": {"ready": "READY", "config_update": "NOT_UPDATING"}, "config": {"served_entities": [{"entity_version": "7"}]}}


class Escenario:
    """Qué contesta el servidor falso en cada prueba."""

    def __init__(self):
        self.retraso_post = 0.0
        self.status_post = 200  # para simular, por ejemplo, un 403

    def __call__(self, metodo, ruta, cuerpo):
        if metodo == "GET":
            if ruta.endswith("/openapi"):
                return Respuesta(200, {"openapi": "3.0.0", "info": {"title": "vuelos-retrasos"}})
            return Respuesta(200, ESTADO_LISTO)
        if "-no-existe" in ruta:
            return Respuesta(404, {"error_code": "RESOURCE_DOES_NOT_EXIST", "message": "Endpoint does not exist"})
        if self.status_post != 200:
            return Respuesta(self.status_post, {"message": "prohibido"})
        mes = json.loads(cuerpo)["dataframe_split"]["data"][0][0]
        if mes == 13:
            return Respuesta(400, {"error_code": "INVALID_PARAMETER_VALUE", "message": "Entrada inválida: fila 0: mes=13 fuera de rango (1-12)"})
        return Respuesta(200, OK, retraso=self.retraso_post)


@pytest.fixture
def entorno(servidor, monkeypatch):
    st.cache_data.clear()
    escenario = Escenario()
    servidor.responder = escenario
    monkeypatch.setenv("SERVING_ENDPOINT", "vuelos-retrasos")
    monkeypatch.delenv("URL_BASE_API", raising=False)
    monkeypatch.setattr(ce, "credenciales_databricks", lambda: (servidor.url, lambda: {"Authorization": "Bearer t"}))
    return servidor, escenario


def abrir():
    at = AppTest.from_file(RUTA_APP, default_timeout=30).run()
    assert not at.exception, at.exception
    return at


def enviar(at, modo="normal"):
    at.sidebar.selectbox(key="modo").select(MODOS[modo])
    boton = [b for b in at.button if "Predecir" in b.label][0]
    boton.click().run()
    assert not at.exception, at.exception
    return at


def textos(elementos):
    return " ".join(e.value for e in elementos)


def test_pantalla_inicial(entorno):
    at = abrir()
    assert at.title[0].value == "¿Se retrasará mi vuelo?"
    assert "listo" in textos(at.sidebar.success)
    assert at.sidebar.selectbox(key="modo").options == list(MODOS.values())
    assert [t.label for t in at.tabs] == ["Predicción", "Documentación de la API"]
    assert len(at.error) == 0 and len(at.metric) == 0


def test_modo_normal_muestra_prediccion(entorno):
    servidor, _ = entorno
    at = enviar(abrir())
    assert at.metric[0].value == "31.2%"
    assert "a tiempo" in textos(at.success)
    assert "vuelos-retrasos" in textos(at.caption) and "no de un modelo copiado" in textos(at.caption)
    assert "Detalle técnico" in [e.label for e in at.expander]
    assert len(at.error) == 0
    posts = [p for p in servidor.peticiones if p[0].endswith("/invocations")]
    assert json.loads(posts[0][2])["dataframe_split"]["data"][0][:2] == [at.session_state["fecha"].month, at.session_state["fecha"].isoweekday()]


def test_retraso_predicho_se_muestra_como_advertencia(entorno, servidor):
    servidor.responder = lambda m, r, c: Respuesta(200, {"predictions": [{"probabilidad_retraso": 0.8, "retraso_predicho": 1, "umbral_decision": 0.41}]}) if m == "POST" else Respuesta(200, ESTADO_LISTO)
    at = enviar(abrir())
    assert at.metric[0].value == "80.0%" and "con retraso" in textos(at.warning)


def test_modo_tiempo_de_espera(entorno):
    servidor, escenario = entorno
    escenario.retraso_post = 0.4
    inicio = time.perf_counter()
    at = enviar(abrir(), "timeout")
    assert "tardó demasiado" in textos(at.error)
    assert len(at.metric) == 0
    assert len([p for p in servidor.peticiones if p[0].endswith("/invocations")]) == 1  # reintentos=0
    assert time.perf_counter() - inicio < 10


def test_modo_error_http_muestra_el_mensaje_de_la_api(entorno):
    at = enviar(abrir(), "http")
    assert "no son válidos" in textos(at.error) and "mes=13" in textos(at.error)
    assert len(at.metric) == 0


def test_modo_endpoint_inexistente(entorno):
    servidor, _ = entorno
    at = enviar(abrir(), "404")
    assert "El endpoint no existe" in textos(at.error)
    assert any("vuelos-retrasos-no-existe" in p[0] for p in servidor.peticiones)


def test_modo_respuesta_invalida_es_honesto_sobre_la_simulacion(entorno):
    at = enviar(abrir(), "invalida")
    assert "inesperado" in textos(at.error)
    assert "SIMULANDO" in textos(at.warning)
    assert len(at.metric) == 0


def test_error_de_permisos(entorno):
    _, escenario = entorno
    escenario.status_post = 403
    at = enviar(abrir())
    assert "no tiene permiso" in textos(at.error) and "Can query" in textos(at.info)


def test_validacion_local_bloquea_la_llamada(entorno):
    servidor, _ = entorno
    at = abrir()
    at.text_input(key="destino").set_value("atl")
    enviar(at)
    assert "distintos" in textos(at.error)
    assert [p for p in servidor.peticiones if p[0].endswith("/invocations")] == []


def test_ruta_de_ejemplo_rellena_el_formulario(entorno):
    at = abrir()
    at.selectbox(key="ruta").select("JFK → LAX (Nueva York – Los Ángeles)").run()
    assert not at.exception
    assert (at.text_input(key="origen").value, at.text_input(key="destino").value) == ("JFK", "LAX")
    assert at.number_input(key="distancia").value == 2475.0 and at.number_input(key="duracion").value == 364.0
    enviar(at)
    assert at.metric[0].value == "31.2%"


def test_sin_credenciales(entorno, monkeypatch):
    def sin_credenciales():
        raise ce.ErrorEndpoint("permisos", "No se encontraron credenciales de Databricks.")

    monkeypatch.setattr(ce, "credenciales_databricks", sin_credenciales)
    at = abrir()
    assert "credenciales" in textos(at.sidebar.error)
    enviar(at)
    assert "credenciales" in textos(at.error)


def test_endpoint_caido_se_ve_en_la_barra_lateral(entorno, servidor):
    servidor.responder = lambda m, r, c: Respuesta(500, "caído")
    at = abrir()
    assert "desconocido" in textos(at.sidebar.warning)


def test_documentacion_y_openapi(entorno):
    at = abrir()
    assert len(at.table) == 1
    codigo = " ".join(c.value for c in at.code)
    assert "curl -X POST" in codigo and "requests.post" in codigo and "vuelos-retrasos" in codigo
    [b for b in at.button if "OpenAPI" in b.label][0].click().run()
    assert not at.exception
    assert any("openapi" in j.value for j in at.json)


def test_documentacion_openapi_falla_con_gracia(entorno, servidor):
    servidor.responder = lambda m, r, c: Respuesta(404, {"message": "no existe"}) if r.endswith("/openapi") else Respuesta(200, ESTADO_LISTO)
    at = abrir()
    [b for b in at.button if "OpenAPI" in b.label][0].click().run()
    assert not at.exception
    assert "No se pudo obtener el esquema" in textos(at.warning)


def test_modo_plan_b_usa_url_base_api(entorno, servidor, monkeypatch):
    monkeypatch.setenv("URL_BASE_API", servidor.url)
    at = enviar(abrir())
    assert at.metric[0].value == "31.2%"
    assert "plan B" in textos(at.sidebar.warning)
