"""Cliente del endpoint publicado (Model Serving o el plan B de FastAPI).

Aquí vive TODA la lógica de red de la aplicación: validar la entrada, llamar al endpoint por
REST, reintentar, y convertir cada tipo de falla en un ``ErrorEndpoint`` con un mensaje claro.
La interfaz (app.py) solo muestra lo que este módulo devuelve.

Solo usa la biblioteca estándar y ``requests``. El SDK de Databricks se importa de forma
perezosa dentro de ``credenciales_databricks`` para poder probar todo sin tenerlo.
"""

import math
import time
from dataclasses import dataclass, field
from urllib.parse import quote

import requests

# --- Contrato (docs/CONTEXTO_TECNICO.md, sección 6) --------------------------------------------
COLUMNAS_ENTERAS = ["mes", "dia_semana", "hora_salida_prog", "hora_llegada_prog"]
COLUMNAS_DECIMALES = ["distancia_millas", "duracion_prog_min"]
COLUMNAS_TEXTO = ["aerolinea", "origen", "destino"]
COLUMNAS_MODELO = COLUMNAS_ENTERAS + COLUMNAS_DECIMALES + COLUMNAS_TEXTO

TIPOS_ERROR = (
    "validacion",
    "permisos",
    "no_encontrado",
    "saturado",
    "no_disponible",
    "tiempo_agotado",
    "conexion",
    "respuesta_invalida",
)

# Códigos HTTP que vale la pena reintentar (el endpoint puede estar despertando de la escala a cero).
STATUS_REINTENTABLES = (429, 500, 502, 503, 504)


class ErrorEndpoint(Exception):
    """Falla al consultar el endpoint, ya clasificada.

    tipo: una de TIPOS_ERROR. mensaje: texto en español para la persona usuaria.
    detalle: texto técnico. status: código HTTP (o None). intentos: cuántas llamadas se hicieron.
    """

    def __init__(self, tipo, mensaje, detalle="", status=None, intentos=1):
        super().__init__(f"[{tipo}] {mensaje}")
        self.tipo = tipo
        self.mensaje = mensaje
        self.detalle = detalle
        self.status = status
        self.intentos = intentos


@dataclass
class ResultadoPrediccion:
    probabilidad_retraso: float
    retraso_predicho: int
    umbral_decision: float
    segundos: float
    intentos: int
    status: int
    json_enviado: dict = field(default_factory=dict)
    respuesta: dict = field(default_factory=dict)


# --- Validación de la entrada (mismas reglas de dominio que la API) ------------------------------
def _es_numero(x):
    return isinstance(x, (int, float)) and not isinstance(x, bool) and math.isfinite(x)


def validar_entrada(vuelo):
    """Revisa el vuelo antes de enviarlo. Devuelve una lista de mensajes; vacía = todo bien."""
    problemas = []
    if not isinstance(vuelo, dict):
        return ["Los datos del vuelo deben ser un diccionario."]

    for col in COLUMNAS_MODELO:
        if col not in vuelo or vuelo[col] is None or vuelo[col] == "":
            problemas.append(f"Falta el campo «{col}».")
    if problemas:
        return problemas

    rangos_enteros = {
        "mes": (1, 12, "el mes"),
        "dia_semana": (1, 7, "el día de la semana (1 = lunes ... 7 = domingo)"),
        "hora_salida_prog": (0, 23, "la hora de salida"),
        "hora_llegada_prog": (0, 23, "la hora de llegada"),
    }
    for col, (minimo, maximo, nombre) in rangos_enteros.items():
        valor = vuelo[col]
        if not _es_numero(valor) or valor != int(valor):
            problemas.append(f"{nombre.capitalize()} debe ser un número entero.")
        elif not minimo <= valor <= maximo:
            problemas.append(f"{nombre.capitalize()} debe estar entre {minimo} y {maximo} (recibí {valor}).")

    distancia, duracion = vuelo["distancia_millas"], vuelo["duracion_prog_min"]
    distancia_ok = _es_numero(distancia) and 0 < distancia <= 6000
    duracion_ok = _es_numero(duracion) and 15 <= duracion <= 900
    if not distancia_ok:
        problemas.append("La distancia debe ser mayor que 0 y de hasta 6,000 millas.")
    if not duracion_ok:
        problemas.append("La duración programada debe estar entre 15 y 900 minutos.")
    if distancia_ok and duracion_ok:
        velocidad = distancia / (duracion / 60)
        if not 40 <= velocidad <= 700:
            problemas.append(
                f"La velocidad implícita ({velocidad:.0f} mph) no es realista: debe estar entre 40 y 700 mph. "
                "Revisa la distancia y la duración."
            )

    aerolinea = vuelo["aerolinea"]
    if not isinstance(aerolinea, str) or not 2 <= len(aerolinea.strip()) <= 3:
        problemas.append("El código de aerolínea debe tener 2 o 3 caracteres (por ejemplo DL).")

    codigos = {}
    for col in ("origen", "destino"):
        valor = vuelo[col]
        if not isinstance(valor, str) or len(valor.strip()) != 3 or not valor.strip().isalpha():
            problemas.append(f"El código del aeropuerto de {col} debe tener exactamente 3 letras (por ejemplo ATL).")
        else:
            codigos[col] = valor.strip().upper()
    if len(codigos) == 2 and codigos["origen"] == codigos["destino"]:
        problemas.append("El origen y el destino deben ser distintos.")
    return problemas


# --- Validación de la respuesta -------------------------------------------------------------------
def _invalida(detalle, status=None):
    return ErrorEndpoint(
        "respuesta_invalida",
        "El servicio contestó algo que la aplicación no esperaba, así que no se muestra ninguna predicción.",
        detalle=detalle,
        status=status,
    )


def validar_respuesta(datos, n_filas=1):
    """Exige {"predictions": [n_filas dicts]} con probabilidad en [0, 1]. Devuelve la lista."""
    if not isinstance(datos, dict):
        raise _invalida(f"Se esperaba un objeto JSON y llegó {type(datos).__name__}.")
    if "predictions" not in datos:
        raise _invalida(f"Falta la clave 'predictions'. Claves recibidas: {sorted(datos.keys())}.")
    predicciones = datos["predictions"]
    if not isinstance(predicciones, list):
        raise _invalida(f"'predictions' debe ser una lista y es {type(predicciones).__name__}.")
    if len(predicciones) != n_filas:
        raise _invalida(f"Se enviaron {n_filas} fila(s) y llegaron {len(predicciones)} predicciones.")

    for i, fila in enumerate(predicciones):
        if not isinstance(fila, dict):
            raise _invalida(f"La predicción {i} no es un objeto JSON.")
        for clave in ("probabilidad_retraso", "retraso_predicho", "umbral_decision"):
            if clave not in fila:
                raise _invalida(f"A la predicción {i} le falta '{clave}'.")
            if not _es_numero(fila[clave]):
                raise _invalida(f"'{clave}' de la predicción {i} no es un número finito: {fila[clave]!r}.")
        if not 0 <= fila["probabilidad_retraso"] <= 1:
            raise _invalida(f"probabilidad_retraso={fila['probabilidad_retraso']} está fuera de [0, 1].")
        if fila["retraso_predicho"] not in (0, 1):
            raise _invalida(f"retraso_predicho={fila['retraso_predicho']!r} debe ser 0 o 1.")
    return predicciones


# --- Utilidades de red -----------------------------------------------------------------------------
def _normalizar_host(host):
    host = (host or "").strip().rstrip("/")
    if host and not host.startswith(("http://", "https://")):
        host = "https://" + host
    return host


def _encabezados_frescos(encabezados):
    """Acepta un dict o una función sin argumentos que devuelve encabezados nuevos (tokens que expiran)."""
    datos = encabezados() if callable(encabezados) else encabezados
    return dict(datos or {})


def _mensaje_de_la_api(resp):
    """Saca el 'message' del cuerpo JSON de la API; si no hay, usa el texto."""
    try:
        cuerpo = resp.json()
    except ValueError:
        cuerpo = None
    if isinstance(cuerpo, dict):
        for clave in ("message", "error", "detail"):
            if isinstance(cuerpo.get(clave), str) and cuerpo[clave].strip():
                return cuerpo[clave].strip()
    texto = (resp.text or "").strip()
    return texto[:300] if texto else f"HTTP {resp.status_code} sin mensaje."


def mensaje_legible(texto):
    """Quita el envoltorio que el servidor de MLflow pone al mensaje del modelo.

    Llega como: "Failed to predict data '<copia de los datos>'. \\nError: Entrada inválida: fila 0: ..."
    Regla: si contiene "Entrada inválida:", se devuelve desde ahí; si no, y empieza con
    "Failed to predict data" y contiene "Error: ", lo que sigue al último "Error: "; si no, el texto completo.
    """
    marca = "Entrada inválida:"
    if marca in texto:
        return texto[texto.index(marca):].strip()
    if texto.startswith("Failed to predict data") and "Error: " in texto:
        resto = texto[texto.rindex("Error: ") + len("Error: "):].strip()
        return resto or texto
    return texto


def _codigo_de_la_api(resp):
    try:
        cuerpo = resp.json()
    except ValueError:
        return None
    return cuerpo.get("error_code") if isinstance(cuerpo, dict) else None


def _mensaje_de_validacion(resp, mensaje_api):
    """Texto para la persona usuaria en un 400: sin el envoltorio y, si viene en inglés, con una frase que lo explica."""
    texto = mensaje_legible(mensaje_api)
    if "Entrada inválida:" in texto:
        return texto
    if texto.startswith("Failed to enforce schema"):  # plan B: la firma rechazó un tipo y el texto es solo un volcado de datos
        return (
            "La API no aceptó el tipo de dato de un campo (por ejemplo, texto donde va un número). "
            "El detalle técnico trae el mensaje completo."
        )
    if "missing inputs" in texto:
        return f"Falta un campo en la petición (detalle de la API, en inglés): {texto}"
    if _codigo_de_la_api(resp) == "BAD_REQUEST" or "not compatible with model signature" in texto:
        return f"La API no aceptó el tipo de dato de un campo (detalle de la API, en inglés): {texto}"
    return texto


def _error_por_status(resp):
    """Convierte un HTTP que no es 200 en (ErrorEndpoint, reintentable)."""
    status = resp.status_code
    mensaje_api = _mensaje_de_la_api(resp)
    detalle = f"HTTP {status}: {mensaje_api}"  # el texto original completo queda aquí
    if status == 400 or status in (413, 415, 422):
        return ErrorEndpoint("validacion", _mensaje_de_validacion(resp, mensaje_api), detalle, status), False
    if status in (401, 403):
        return ErrorEndpoint(
            "permisos",
            "La aplicación no tiene permiso para consultar el endpoint (falta el permiso «Can query»).",
            detalle,
            status,
        ), False
    if status == 404:
        return ErrorEndpoint(
            "no_encontrado", "El endpoint con ese nombre no existe en este workspace.", detalle, status
        ), False
    if status == 429:
        return ErrorEndpoint(
            "saturado", "El endpoint está recibiendo demasiadas consultas en este momento.", detalle, status
        ), True
    if status >= 500:
        return ErrorEndpoint(
            "no_disponible",
            "El endpoint no está disponible en este momento (puede estar despertando o reiniciándose).",
            detalle,
            status,
        ), status in STATUS_REINTENTABLES
    return _invalida(f"Código HTTP inesperado: {status}. {mensaje_api}", status), False


def _error_de_red(excepcion):
    if isinstance(excepcion, requests.Timeout):  # incluye ConnectTimeout y ReadTimeout
        return ErrorEndpoint(
            "tiempo_agotado",
            "El endpoint tardó demasiado en responder.",
            f"{type(excepcion).__name__}: {excepcion}",
        )
    return ErrorEndpoint(
        "conexion",
        "No se pudo establecer conexión con el endpoint.",
        f"{type(excepcion).__name__}: {excepcion}",
    )


# --- Consulta principal ------------------------------------------------------------------------------
def consultar_prediccion(
    vuelo,
    *,
    host,
    encabezados,
    endpoint,
    timeout_conexion=5,
    timeout_lectura=30,
    reintentos=3,
    espera_inicial=2.0,
    url_base=None,
    sesion=None,
    dormir=time.sleep,
    alterar_respuesta=None,
):
    """Pide una predicción al endpoint publicado. Devuelve ResultadoPrediccion o lanza ErrorEndpoint.

    reintentos = cuántas veces se vuelve a intentar después del primer intento (0 = una sola llamada).
    La espera entre intentos crece al doble: espera_inicial, 2x, 4x...
    """
    fila = [vuelo[col] for col in COLUMNAS_MODELO]
    cuerpo = {"dataframe_split": {"columns": list(COLUMNAS_MODELO), "data": [fila]}}
    base = _normalizar_host(url_base or host)
    url = f"{base}/serving-endpoints/{quote(endpoint, safe='')}/invocations"

    sesion = sesion or requests.Session()
    total_intentos = max(0, int(reintentos)) + 1
    inicio = time.perf_counter()
    ultimo_error = None

    for intento in range(1, total_intentos + 1):
        reintentable = True
        try:
            cabeceras = _encabezados_frescos(encabezados)
            cabeceras["Content-Type"] = "application/json"
            resp = sesion.post(url, json=cuerpo, headers=cabeceras, timeout=(timeout_conexion, timeout_lectura))
        except requests.RequestException as exc:
            ultimo_error = _error_de_red(exc)
        else:
            if resp.status_code == 200:
                return _procesar_200(resp, cuerpo, alterar_respuesta, inicio, intento)
            ultimo_error, reintentable = _error_por_status(resp)

        ultimo_error.intentos = intento
        if not reintentable or intento == total_intentos:
            break
        dormir(espera_inicial * (2 ** (intento - 1)))

    raise ultimo_error


def _procesar_200(resp, cuerpo, alterar_respuesta, inicio, intento):
    try:
        datos = resp.json()
    except ValueError as exc:
        raise ErrorEndpoint(
            "respuesta_invalida",
            "El servicio contestó algo que la aplicación no esperaba, así que no se muestra ninguna predicción.",
            f"HTTP 200 pero el cuerpo no es JSON ({exc}). Inicio del cuerpo: {resp.text[:200]!r}",
            200,
            intento,
        ) from exc
    if alterar_respuesta is not None:  # solo el modo demostración lo usa
        datos = alterar_respuesta(datos)
    try:
        pred = validar_respuesta(datos, n_filas=1)[0]
    except ErrorEndpoint as err:
        err.status, err.intentos = 200, intento
        raise
    return ResultadoPrediccion(
        probabilidad_retraso=float(pred["probabilidad_retraso"]),
        retraso_predicho=int(pred["retraso_predicho"]),
        umbral_decision=float(pred["umbral_decision"]),
        segundos=time.perf_counter() - inicio,
        intentos=intento,
        status=200,
        json_enviado=cuerpo,
        respuesta=datos,
    )


# --- Estado y OpenAPI ----------------------------------------------------------------------------------
def estado_endpoint(host, encabezados, endpoint, timeout=5):
    """Consulta el estado del endpoint. Nunca lanza excepciones: si algo falla, estado «desconocido»."""
    url = f"{_normalizar_host(host)}/api/2.0/serving-endpoints/{quote(endpoint, safe='')}"
    try:
        resp = requests.get(url, headers=_encabezados_frescos(encabezados), timeout=timeout)
    except Exception as exc:  # noqa: BLE001 - esta función promete no lanzar nada
        return {"listo": False, "estado": f"desconocido ({type(exc).__name__})", "version": None}
    if resp.status_code == 404:
        return {"listo": False, "estado": "no existe (404)", "version": None}
    if resp.status_code != 200:
        return {"listo": False, "estado": f"desconocido (HTTP {resp.status_code})", "version": None}
    try:
        datos = resp.json()
        estado = datos.get("state") or {}
        listo = estado.get("ready") == "READY"
        actualizacion = estado.get("config_update", "NOT_UPDATING")
        entidades = (datos.get("config") or {}).get("served_entities") or []
        version = entidades[0].get("entity_version") if entidades else None
    except (ValueError, AttributeError, IndexError, TypeError):
        return {"listo": False, "estado": "desconocido (respuesta ilegible)", "version": None}
    if actualizacion == "UPDATE_FAILED":
        texto = "falló la última actualización"
    elif actualizacion == "IN_PROGRESS":
        texto = "listo, actualizándose" if listo else "actualizándose"
    else:
        texto = "listo" if listo else "no está listo"
    return {"listo": listo, "estado": texto, "version": version}


def obtener_openapi(host, encabezados, endpoint, timeout=10, ruta=None):
    """Descarga el esquema OpenAPI del endpoint. ``ruta`` permite apuntar al plan B (/openapi.json)."""
    ruta = ruta or f"/api/2.0/serving-endpoints/{quote(endpoint, safe='')}/openapi"
    url = f"{_normalizar_host(host)}{ruta}"
    try:
        resp = requests.get(url, headers=_encabezados_frescos(encabezados), timeout=timeout)
    except requests.RequestException as exc:
        error = _error_de_red(exc)
        error.detalle = f"GET {ruta}: {error.detalle}"
        raise error from exc
    if resp.status_code != 200:
        error, _ = _error_por_status(resp)
        raise error
    try:
        datos = resp.json()
    except ValueError as exc:
        raise _invalida("El esquema OpenAPI no es JSON.", 200) from exc
    if not isinstance(datos, dict):
        raise _invalida("El esquema OpenAPI no es un objeto JSON.", 200)
    return datos


def credenciales_databricks():
    """Devuelve (host, función_de_encabezados) usando la identidad de la app en Databricks Apps."""
    try:
        from databricks.sdk.core import Config

        cfg = Config()
        return _normalizar_host(cfg.host), cfg.authenticate
    except Exception as exc:  # noqa: BLE001
        raise ErrorEndpoint(
            "permisos",
            "No se encontraron credenciales de Databricks. Dentro de Databricks Apps se configuran solas; "
            "si corres la app en tu computadora, define DATABRICKS_HOST y un token.",
            f"{type(exc).__name__}: {exc}",
        ) from exc
