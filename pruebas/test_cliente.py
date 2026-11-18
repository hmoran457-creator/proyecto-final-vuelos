"""Pruebas de cliente_endpoint contra un servidor HTTP falso (cubre cada fila de la tabla de errores)."""

import json
import time

import pytest

import cliente_endpoint as ce
from conftest import OK, PUERTO_SIN_SERVIDOR, VUELO, Respuesta

ENC = {"Authorization": "Bearer prueba"}


def consultar(servidor, reloj, **extra):
    opciones = dict(host=servidor.url, encabezados=ENC, endpoint="vuelos-retrasos", dormir=reloj)
    opciones.update(extra)
    return ce.consultar_prediccion(dict(VUELO), **opciones)


# --- camino feliz ------------------------------------------------------------------------------
def test_camino_feliz_y_formato_de_la_peticion(servidor, reloj):
    servidor.programar(Respuesta(200, OK))
    r = consultar(servidor, reloj)
    assert r.probabilidad_retraso == pytest.approx(0.3124)
    assert (r.retraso_predicho, r.umbral_decision, r.status, r.intentos) == (0, 0.41, 200, 1)
    assert r.respuesta == OK and r.segundos >= 0
    ruta, cabeceras, cuerpo = servidor.peticiones[0]
    assert ruta == "/serving-endpoints/vuelos-retrasos/invocations"
    enviado = json.loads(cuerpo)
    assert enviado["dataframe_split"]["columns"] == [
        "mes", "dia_semana", "hora_salida_prog", "hora_llegada_prog",
        "distancia_millas", "duracion_prog_min", "aerolinea", "origen", "destino",
    ]
    assert enviado["dataframe_split"]["data"] == [[7, 5, 18, 21, 1947.0, 285.0, "DL", "ATL", "LAX"]]
    assert r.json_enviado == enviado
    assert cabeceras["Authorization"] == "Bearer prueba"
    assert reloj.esperas == []


def test_url_base_tiene_prioridad_sobre_host(servidor, reloj):
    servidor.programar(Respuesta(200, OK))
    r = consultar(servidor, reloj, host="http://127.0.0.1:1", url_base=servidor.url + "/")
    assert r.status == 200


# --- errores que NO se reintentan ---------------------------------------------------------------
@pytest.mark.parametrize(
    "status,tipo",
    [(400, "validacion"), (401, "permisos"), (403, "permisos"), (404, "no_encontrado")],
)
def test_errores_4xx_no_se_reintentan(servidor, reloj, status, tipo):
    servidor.programar(Respuesta(status, {"error_code": "X", "message": "texto de la API"}))
    with pytest.raises(ce.ErrorEndpoint) as exc:
        consultar(servidor, reloj)
    assert (exc.value.tipo, exc.value.status, exc.value.intentos) == (tipo, status, 1)
    assert len(servidor.peticiones) == 1 and reloj.esperas == []


def test_400_con_message_json_lo_muestra(servidor, reloj):
    servidor.programar(
        Respuesta(400, {"error_code": "INVALID_PARAMETER_VALUE", "message": "Entrada inválida: fila 0: mes=13 fuera de rango (1-12)"})
    )
    with pytest.raises(ce.ErrorEndpoint) as exc:
        consultar(servidor, reloj)
    assert exc.value.mensaje == "Entrada inválida: fila 0: mes=13 fuera de rango (1-12)"
    assert "mes=13" in exc.value.detalle


def test_400_sin_json_usa_el_texto(servidor, reloj):
    servidor.programar(Respuesta(400, "Bad request: columnas incorrectas"))
    with pytest.raises(ce.ErrorEndpoint) as exc:
        consultar(servidor, reloj)
    assert exc.value.tipo == "validacion" and "columnas incorrectas" in exc.value.mensaje


def test_400_json_sin_message_usa_el_texto(servidor, reloj):
    servidor.programar(Respuesta(400, {"otra_cosa": 1}))
    with pytest.raises(ce.ErrorEndpoint) as exc:
        consultar(servidor, reloj)
    assert exc.value.tipo == "validacion" and "otra_cosa" in exc.value.mensaje


# --- mensajes 400 envueltos por el servidor de MLflow -----------------------------------------------------
VOLCADO = "Failed to predict data '   mes  dia_semana ...\n0    13  5 ...  [1 rows x 9 columns]'. \nError: "


def error_400(servidor, reloj, cuerpo):
    servidor.programar(Respuesta(400, cuerpo))
    with pytest.raises(ce.ErrorEndpoint) as exc:
        consultar(servidor, reloj)
    assert exc.value.tipo == "validacion" and exc.value.intentos == 1
    return exc.value


def test_mensaje_legible_reglas_puras():
    assert ce.mensaje_legible(VOLCADO + "Entrada inválida: fila 0: mes=13 fuera de rango (1-12)") == (
        "Entrada inválida: fila 0: mes=13 fuera de rango (1-12)"
    )
    assert ce.mensaje_legible(VOLCADO + "Model is missing inputs ['destino'].") == "Model is missing inputs ['destino']."
    # usa el ÚLTIMO "Error: " (el volcado de datos podría contener otro)
    assert ce.mensaje_legible("Failed to predict data 'x Error: y'. \nError: algo") == "algo"
    assert ce.mensaje_legible("Mensaje sin envolver") == "Mensaje sin envolver"
    assert ce.mensaje_legible("Failed to predict data sin la marca") == "Failed to predict data sin la marca"


def test_400_envuelto_de_regla_de_negocio(servidor, reloj):
    original = VOLCADO + "Entrada inválida: fila 0: mes=13 fuera de rango (1-12)"
    err = error_400(servidor, reloj, {"error_code": "INVALID_PARAMETER_VALUE", "message": original})
    assert err.mensaje == "Entrada inválida: fila 0: mes=13 fuera de rango (1-12)"
    assert "Failed to predict data" not in err.mensaje
    assert original in err.detalle  # el texto completo queda en el detalle técnico


def test_400_envuelto_columna_faltante(servidor, reloj):
    original = VOLCADO + "Model is missing inputs ['destino']."
    err = error_400(servidor, reloj, {"error_code": "INVALID_PARAMETER_VALUE", "message": original})
    assert "Model is missing inputs ['destino']." in err.mensaje
    assert err.mensaje.startswith("Falta un campo en la petición")
    assert "Failed to predict data" not in err.mensaje and original in err.detalle


def test_400_sin_envolver_se_muestra_completo(servidor, reloj):
    err = error_400(servidor, reloj, {"error_code": "INVALID_PARAMETER_VALUE", "message": "Entrada inválida: fila 0: origen=XXX desconocido"})
    assert err.mensaje == "Entrada inválida: fila 0: origen=XXX desconocido"
    err = error_400(servidor, reloj, {"error_code": "OTRO", "message": "texto cualquiera de la API"})
    assert err.mensaje == "texto cualquiera de la API"


def test_400_bad_request_en_ingles_lleva_frase_en_espanol(servidor, reloj):
    original = "Invalid input. Data is not compatible with model signature. Failed to convert column mes to type 'int64'. Error: invalid literal for int()"
    err = error_400(servidor, reloj, {"error_code": "BAD_REQUEST", "message": original})
    assert err.mensaje.startswith("La API no aceptó el tipo de dato de un campo")
    assert original in err.mensaje and original in err.detalle


def test_400_plan_b_failed_to_enforce_schema_no_muestra_el_volcado(servidor, reloj):
    original = "Failed to enforce schema of data '     mes ...\n0  julio ...\n\n[1 rows x 9 columns]'"
    err = error_400(servidor, reloj, {"error_code": "INVALID_PARAMETER_VALUE", "message": original})
    assert err.mensaje.startswith("La API no aceptó el tipo de dato") and "julio" not in err.mensaje
    assert original in err.detalle


def test_400_con_diez_problemas(servidor, reloj):
    problemas = "; ".join(f"fila {i}: mes=13 fuera de rango (1-12)" for i in range(10))
    err = error_400(servidor, reloj, {"error_code": "INVALID_PARAMETER_VALUE", "message": VOLCADO + "Entrada inválida: " + problemas})
    assert err.mensaje == "Entrada inválida: " + problemas
    assert err.mensaje.count("fila ") == 10


# --- errores que SÍ se reintentan ------------------------------------------------------------------
def test_503_503_200_se_recupera(servidor, reloj):
    servidor.programar(Respuesta(503, "despertando"), Respuesta(503, "despertando"), Respuesta(200, OK))
    r = consultar(servidor, reloj)
    assert r.intentos == 3 and r.status == 200
    assert reloj.esperas == [2.0, 4.0]  # espera creciente, sin dormir de verdad
    assert len(servidor.peticiones) == 3


@pytest.mark.parametrize(
    "status,tipo",
    [(429, "saturado"), (500, "no_disponible"), (502, "no_disponible"), (503, "no_disponible"), (504, "no_disponible")],
)
def test_reintentos_agotados(servidor, reloj, status, tipo):
    servidor.programar(Respuesta(status, "no"))
    with pytest.raises(ce.ErrorEndpoint) as exc:
        consultar(servidor, reloj)
    assert (exc.value.tipo, exc.value.status, exc.value.intentos) == (tipo, status, 4)
    assert len(servidor.peticiones) == 4
    assert reloj.esperas == [2.0, 4.0, 8.0]


def test_504_al_despertar_y_luego_200(servidor, reloj):
    servidor.programar(Respuesta(504, "Gateway Timeout"), Respuesta(200, OK))
    assert consultar(servidor, reloj).intentos == 2


def test_reintentos_cero_hace_una_sola_llamada(servidor, reloj):
    servidor.programar(Respuesta(503, "no"))
    with pytest.raises(ce.ErrorEndpoint) as exc:
        consultar(servidor, reloj, reintentos=0)
    assert exc.value.intentos == 1 and reloj.esperas == []


def test_timeout_de_lectura(servidor, reloj):
    servidor.programar(Respuesta(200, OK, retraso=1.5))
    inicio = time.perf_counter()
    with pytest.raises(ce.ErrorEndpoint) as exc:
        consultar(servidor, reloj, timeout_lectura=0.3, reintentos=1)
    assert time.perf_counter() - inicio < 1.4  # cortó a los 0.3 s por intento, no esperó la respuesta
    assert (exc.value.tipo, exc.value.intentos, exc.value.status) == ("tiempo_agotado", 2, None)
    assert reloj.esperas == [2.0]


def test_conexion_rechazada(reloj):
    with pytest.raises(ce.ErrorEndpoint) as exc:
        ce.consultar_prediccion(
            dict(VUELO), host=f"http://127.0.0.1:{PUERTO_SIN_SERVIDOR}", encabezados=ENC,
            endpoint="vuelos-retrasos", reintentos=2, dormir=reloj,
        )
    assert (exc.value.tipo, exc.value.intentos) == ("conexion", 3)
    assert reloj.esperas == [2.0, 4.0]


# --- respuestas inválidas (200) -----------------------------------------------------------------------
def test_200_que_no_es_json(servidor, reloj):
    servidor.programar(Respuesta(200, "<html>hola</html>"))
    with pytest.raises(ce.ErrorEndpoint) as exc:
        consultar(servidor, reloj)
    assert (exc.value.tipo, exc.value.status, exc.value.intentos) == ("respuesta_invalida", 200, 1)
    assert len(servidor.peticiones) == 1  # no se reintenta


@pytest.mark.parametrize(
    "cuerpo",
    [
        {"resultado": []},
        {"predictions": "no es lista"},
        [1, 2, 3],
        {"predictions": []},
        {"predictions": [OK["predictions"][0], OK["predictions"][0]]},
        {"predictions": [{"probabilidad_retraso": 1.7, "retraso_predicho": 1, "umbral_decision": 0.4}]},
        {"predictions": [{"probabilidad_retraso": -0.1, "retraso_predicho": 0, "umbral_decision": 0.4}]},
        {"predictions": [{"probabilidad_retraso": "0.5", "retraso_predicho": 0, "umbral_decision": 0.4}]},
        {"predictions": [{"probabilidad_retraso": None, "retraso_predicho": 0, "umbral_decision": 0.4}]},
        {"predictions": [{"probabilidad_retraso": 0.5, "retraso_predicho": 2, "umbral_decision": 0.4}]},
        {"predictions": [{"probabilidad_retraso": 0.5, "retraso_predicho": 1}]},
        {"predictions": [{"probabilidad_retraso": 0.5, "retraso_predicho": 1, "umbral_decision": "x"}]},
        {"predictions": [0.5]},
    ],
)
def test_respuestas_invalidas(servidor, reloj, cuerpo):
    servidor.programar(Respuesta(200, cuerpo))
    with pytest.raises(ce.ErrorEndpoint) as exc:
        consultar(servidor, reloj)
    assert exc.value.tipo == "respuesta_invalida"


def test_probabilidad_nan(servidor, reloj):
    cuerpo = '{"predictions": [{"probabilidad_retraso": NaN, "retraso_predicho": 0, "umbral_decision": 0.4}]}'
    servidor.programar(Respuesta(200, cuerpo))
    with pytest.raises(ce.ErrorEndpoint) as exc:
        consultar(servidor, reloj)
    assert exc.value.tipo == "respuesta_invalida"


def test_validar_respuesta_directo_acepta_la_buena_y_el_numero_de_filas():
    assert ce.validar_respuesta(OK) == OK["predictions"]
    with pytest.raises(ce.ErrorEndpoint):
        ce.validar_respuesta(OK, n_filas=2)
    with pytest.raises(ce.ErrorEndpoint):
        ce.validar_respuesta({"predictions": [{"probabilidad_retraso": float("inf"), "retraso_predicho": 1, "umbral_decision": 0.4}]})


def test_alterar_respuesta_se_aplica_antes_de_validar(servidor, reloj):
    servidor.programar(Respuesta(200, OK))
    malformada = lambda datos: {"predictions": [{"probabilidad_retraso": 9, "retraso_predicho": 1, "umbral_decision": 0.4}]}
    with pytest.raises(ce.ErrorEndpoint) as exc:
        consultar(servidor, reloj, alterar_respuesta=malformada)
    assert exc.value.tipo == "respuesta_invalida"


def test_status_raro_es_respuesta_invalida(servidor, reloj):
    servidor.programar(Respuesta(204, ""))
    with pytest.raises(ce.ErrorEndpoint) as exc:
        consultar(servidor, reloj)
    assert exc.value.tipo == "respuesta_invalida"


# --- encabezados -------------------------------------------------------------------------------------
def test_encabezados_callable_se_renuevan_en_cada_intento(servidor, reloj):
    servidor.programar(Respuesta(503, "x"), Respuesta(503, "x"), Respuesta(200, OK))
    llamadas = []

    def encabezados():
        llamadas.append(1)
        return {"Authorization": f"Bearer token-{len(llamadas)}"}

    consultar(servidor, reloj, encabezados=encabezados)
    assert len(llamadas) == 3
    assert [p[1]["Authorization"] for p in servidor.peticiones] == ["Bearer token-1", "Bearer token-2", "Bearer token-3"]


def test_el_token_no_aparece_en_los_errores(servidor, reloj):
    servidor.programar(Respuesta(403, "prohibido"))
    with pytest.raises(ce.ErrorEndpoint) as exc:
        consultar(servidor, reloj, encabezados={"Authorization": "Bearer SECRETO123"})
    assert "SECRETO123" not in repr(exc.value.__dict__)


# --- validar_entrada ------------------------------------------------------------------------------------
def test_validar_entrada_ok():
    assert ce.validar_entrada(dict(VUELO)) == []


@pytest.mark.parametrize(
    "cambio,fragmento",
    [
        ({"mes": 13}, "mes"),
        ({"mes": 0}, "mes"),
        ({"mes": 1.5}, "entero"),
        ({"dia_semana": 8}, "día"),
        ({"hora_salida_prog": 24}, "salida"),
        ({"hora_llegada_prog": -1}, "llegada"),
        ({"distancia_millas": 0}, "distancia"),
        ({"distancia_millas": 6001}, "distancia"),
        ({"duracion_prog_min": 14}, "duración"),
        ({"duracion_prog_min": 901}, "duración"),
        ({"distancia_millas": 2000.0, "duracion_prog_min": 15.0}, "velocidad"),  # 8000 mph
        ({"distancia_millas": 10.0, "duracion_prog_min": 900.0}, "velocidad"),  # 0.7 mph
        ({"origen": "AT"}, "3 letras"),
        ({"destino": "LAXX"}, "3 letras"),
        ({"origen": "123"}, "3 letras"),
        ({"destino": "atl"}, "distintos"),
        ({"aerolinea": ""}, "aerolinea"),
        ({"aerolinea": "D"}, "aerolínea"),
        ({"distancia_millas": float("nan")}, "distancia"),
        ({"mes": True}, "mes"),
        ({"distancia_millas": "1947"}, "distancia"),
    ],
)
def test_validar_entrada_detecta_problemas(cambio, fragmento):
    problemas = ce.validar_entrada(dict(VUELO, **cambio))
    assert problemas, f"debía rechazar {cambio}"
    assert any(fragmento in p for p in problemas), problemas


def test_validar_entrada_campo_faltante_y_tipo_incorrecto():
    sin_mes = {k: v for k, v in VUELO.items() if k != "mes"}
    assert any("mes" in p for p in ce.validar_entrada(sin_mes))
    assert ce.validar_entrada("hola")


# --- estado y OpenAPI --------------------------------------------------------------------------------------
def test_estado_endpoint_listo(servidor):
    servidor.programar(Respuesta(200, {
        "state": {"ready": "READY", "config_update": "NOT_UPDATING"},
        "config": {"served_entities": [{"entity_version": "3"}]},
    }))
    estado = ce.estado_endpoint(servidor.url, ENC, "vuelos-retrasos")
    assert estado == {"listo": True, "estado": "listo", "version": "3"}
    assert servidor.peticiones[0][0] == "/api/2.0/serving-endpoints/vuelos-retrasos"


def test_estado_endpoint_nunca_lanza(servidor):
    servidor.programar(Respuesta(404, {"message": "no existe"}))
    assert ce.estado_endpoint(servidor.url, ENC, "x")["listo"] is False
    servidor.programar(Respuesta(500, "boom"))
    assert "desconocido" in ce.estado_endpoint(servidor.url, ENC, "x")["estado"]
    servidor.programar(Respuesta(200, "no json"))
    assert "desconocido" in ce.estado_endpoint(servidor.url, ENC, "x")["estado"]
    servidor.programar(Respuesta(200, [1, 2]))
    assert "desconocido" in ce.estado_endpoint(servidor.url, ENC, "x")["estado"]
    assert "desconocido" in ce.estado_endpoint(f"http://127.0.0.1:{PUERTO_SIN_SERVIDOR}", ENC, "x")["estado"]
    assert "desconocido" in ce.estado_endpoint(servidor.url, lambda: 1 / 0, "x")["estado"]


def test_obtener_openapi(servidor):
    servidor.programar(Respuesta(200, {"openapi": "3.0.0"}))
    assert ce.obtener_openapi(servidor.url, ENC, "vuelos-retrasos") == {"openapi": "3.0.0"}
    assert servidor.peticiones[0][0] == "/api/2.0/serving-endpoints/vuelos-retrasos/openapi"


def test_obtener_openapi_errores(servidor):
    servidor.programar(Respuesta(404, {"message": "no existe"}))
    with pytest.raises(ce.ErrorEndpoint) as exc:
        ce.obtener_openapi(servidor.url, ENC, "x")
    assert exc.value.tipo == "no_encontrado"
    servidor.programar(Respuesta(200, "texto"))
    with pytest.raises(ce.ErrorEndpoint) as exc:
        ce.obtener_openapi(servidor.url, ENC, "x")
    assert exc.value.tipo == "respuesta_invalida"
    with pytest.raises(ce.ErrorEndpoint) as exc:
        ce.obtener_openapi(f"http://127.0.0.1:{PUERTO_SIN_SERVIDOR}", ENC, "x")
    assert exc.value.tipo == "conexion"


def test_credenciales_usan_config_del_sdk(monkeypatch):
    core = pytest.importorskip("databricks.sdk.core")

    class ConfigFalsa:
        host = "mi-workspace.cloud.databricks.com/"

        def authenticate(self):
            return {"Authorization": "Bearer falso"}

    monkeypatch.setattr(core, "Config", ConfigFalsa)
    host, enc = ce.credenciales_databricks()
    assert host == "https://mi-workspace.cloud.databricks.com"
    assert enc() == {"Authorization": "Bearer falso"}


def test_credenciales_sin_config_lanza_error_claro(monkeypatch):
    core = pytest.importorskip("databricks.sdk.core")

    def config_rota():
        raise ValueError("sin credenciales")

    monkeypatch.setattr(core, "Config", config_rota)  # no se toca la configuración real del usuario
    with pytest.raises(ce.ErrorEndpoint) as exc:
        ce.credenciales_databricks()
    assert exc.value.tipo == "permisos"


def test_host_sin_esquema_se_normaliza():
    assert ce._normalizar_host("mi-workspace.cloud.databricks.com/") == "https://mi-workspace.cloud.databricks.com"
