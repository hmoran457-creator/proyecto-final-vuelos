"""¿Se retrasará mi vuelo? — interfaz Streamlit (Databricks Apps).

La app NO contiene ningún modelo: cada predicción se pide por REST al endpoint publicado en
Model Serving (o, en el plan B, a la API FastAPI). Toda la lógica de red y de errores está en
``cliente_endpoint.py``; aquí solo se arma la pantalla.

Compatible con Streamlit 1.38 (la versión preinstalada en Databricks Apps).
"""

import json
import os
from datetime import date

import streamlit as st

import cliente_endpoint as ce

# --- Configuración (variables de entorno definidas en app.yaml) -----------------------------------
ENDPOINT = os.environ.get("SERVING_ENDPOINT") or "vuelos-retrasos"
URL_BASE_API = (os.environ.get("URL_BASE_API") or "").strip() or None  # plan B: API FastAPI

AEROLINEAS = {
    "9E": "Endeavor Air",
    "AA": "American Airlines",
    "AS": "Alaska Airlines",
    "B6": "JetBlue",
    "DL": "Delta Air Lines",
    "F9": "Frontier",
    "G4": "Allegiant",
    "HA": "Hawaiian",
    "MQ": "Envoy Air",
    "NK": "Spirit",
    "OH": "PSA Airlines",
    "OO": "SkyWest",
    "UA": "United",
    "WN": "Southwest",
    "YX": "Republic",
}

# Rutas de ejemplo: nombre -> (origen, destino, distancia en millas, duración en minutos)
RUTAS = {
    "ATL → LAX (Atlanta – Los Ángeles)": ("ATL", "LAX", 1947, 285),
    "JFK → LAX (Nueva York – Los Ángeles)": ("JFK", "LAX", 2475, 364),
    "DFW → LAX (Dallas – Los Ángeles)": ("DFW", "LAX", 1235, 188),
    "ORD → LGA (Chicago – Nueva York)": ("ORD", "LGA", 733, 126),
    "LGA → MIA (Nueva York – Miami)": ("LGA", "MIA", 1096, 192),
    "CLT → MCO (Charlotte – Orlando)": ("CLT", "MCO", 468, 103),
    "IAH → DFW (Houston – Dallas)": ("IAH", "DFW", 224, 77),
    "LAX → JFK (Los Ángeles – Nueva York)": ("LAX", "JFK", 2475, 339),
}
SIN_RUTA = "(ninguna: escribo los datos yo)"

# Modo demostración de fallas
MODO_NORMAL = "Normal"
MODO_TIMEOUT = "Tiempo de espera agotado"
MODO_HTTP = "Error HTTP: la API rechaza la entrada"
MODO_404 = "Endpoint inexistente (404)"
MODO_INVALIDA = "Respuesta inválida"
EXPLICACION_MODO = {
    MODO_NORMAL: "Consulta normal al endpoint publicado.",
    MODO_TIMEOUT: "Llama al endpoint real con un tiempo de espera de 1 milisegundo y sin reintentos: demuestra el manejo de tiempos de espera.",
    MODO_HTTP: "Se salta la validación local y envía mes=13: el endpoint real responde HTTP 400 y se muestra su mensaje.",
    MODO_404: "Agrega «-no-existe» al nombre del endpoint: el servidor real responde HTTP 404.",
    MODO_INVALIDA: "SIMULADO: la app recibe la respuesta real pero la reemplaza por una malformada para demostrar que la valida.",
}

# Por tipo de error: (título, qué puede hacer la persona)
AYUDA_POR_TIPO = {
    "validacion": (
        "Los datos del vuelo no son válidos",
        "Corrige lo que indica el mensaje y vuelve a intentar. No se reintenta solo, porque la misma entrada daría el mismo error.",
    ),
    "permisos": (
        "La app no tiene permiso para usar el endpoint",
        "Pide a quien administra el workspace que agregue el endpoint como recurso de la app con el permiso «Can query».",
    ),
    "no_encontrado": (
        "El endpoint no existe",
        "Revisa que el nombre del endpoint (variable SERVING_ENDPOINT) sea correcto y que esté creado en este workspace.",
    ),
    "saturado": (
        "El endpoint está saturado",
        "Se reintentó con esperas cada vez más largas y siguió saturado. Espera un minuto y vuelve a intentar.",
    ),
    "no_disponible": (
        "El endpoint no está disponible",
        "Los endpoints con escala a cero se duermen y pueden tardar de 10 segundos a varios minutos en despertar. "
        "Espera un momento y vuelve a intentar; revisa también el estado en el panel de la izquierda.",
    ),
    "tiempo_agotado": (
        "El endpoint tardó demasiado",
        "Puede estar despertando de la escala a cero. Vuelve a intentar en un momento o aumenta el tiempo de espera en el panel de la izquierda.",
    ),
    "conexion": (
        "No hay conexión con el endpoint",
        "Revisa tu red y que la dirección del workspace sea correcta; luego vuelve a intentar.",
    ),
    "respuesta_invalida": (
        "El servicio contestó algo inesperado",
        "No se muestra ninguna predicción para no enseñarte un dato dudoso. Vuelve a intentar; si se repite, revisa el endpoint.",
    ),
}

CAMPOS_API = [
    {"Campo": "mes", "Tipo": "entero", "Rango o valores": "1 a 12"},
    {"Campo": "dia_semana", "Tipo": "entero", "Rango o valores": "1 (lunes) a 7 (domingo)"},
    {"Campo": "hora_salida_prog", "Tipo": "entero", "Rango o valores": "0 a 23"},
    {"Campo": "hora_llegada_prog", "Tipo": "entero", "Rango o valores": "0 a 23"},
    {"Campo": "distancia_millas", "Tipo": "decimal", "Rango o valores": "mayor que 0 y hasta 6,000"},
    {"Campo": "duracion_prog_min", "Tipo": "decimal", "Rango o valores": "15 a 900; velocidad implícita entre 40 y 700 mph"},
    {"Campo": "aerolinea", "Tipo": "texto", "Rango o valores": "código de aerolínea conocido (DL, AA, UA, WN...)"},
    {"Campo": "origen", "Tipo": "texto", "Rango o valores": "código IATA de 3 letras conocido; distinto del destino"},
    {"Campo": "destino", "Tipo": "texto", "Rango o valores": "código IATA de 3 letras conocido; distinto del origen"},
]
EJEMPLO_PETICION = {
    "dataframe_split": {
        "columns": ce.COLUMNAS_MODELO,
        "data": [[7, 5, 18, 21, 1947.0, 285.0, "DL", "ATL", "LAX"]],
    }
}
EJEMPLO_RESPUESTA = {"predictions": [{"probabilidad_retraso": 0.3124, "retraso_predicho": 0, "umbral_decision": 0.41}]}


# --- Estado del formulario -----------------------------------------------------------------------------
def inicializar_estado():
    """Valores iniciales de los campos (se guardan en st.session_state con la llave de cada widget)."""
    valores = {
        "aerolinea": "DL",
        "origen": "ATL",
        "destino": "LAX",
        "fecha": date.today(),
        "hora_salida": 18,
        "hora_llegada": 21,
        "distancia": 1947.0,
        "duracion": 285.0,
    }
    for llave, valor in valores.items():
        st.session_state.setdefault(llave, valor)


def al_elegir_ruta():
    """Callback: rellena origen, destino, distancia y duración con la ruta de ejemplo elegida."""
    ruta = RUTAS.get(st.session_state.get("ruta"))
    if ruta:
        st.session_state["origen"], st.session_state["destino"] = ruta[0], ruta[1]
        st.session_state["distancia"], st.session_state["duracion"] = float(ruta[2]), float(ruta[3])


def construir_vuelo():
    """Convierte lo que hay en el formulario en el diccionario que espera el endpoint."""
    fecha = st.session_state["fecha"]
    return {
        "mes": fecha.month,
        "dia_semana": fecha.isoweekday(),  # lunes = 1 ... domingo = 7
        "hora_salida_prog": int(st.session_state["hora_salida"]),
        "hora_llegada_prog": int(st.session_state["hora_llegada"]),
        "distancia_millas": float(st.session_state["distancia"]),
        "duracion_prog_min": float(st.session_state["duracion"]),
        "aerolinea": st.session_state["aerolinea"],
        "origen": str(st.session_state["origen"]).strip().upper(),
        "destino": str(st.session_state["destino"]).strip().upper(),
    }


# --- Barra lateral ---------------------------------------------------------------------------------------
@st.cache_data(ttl=30, show_spinner=False)
def estado_cacheado(host, endpoint, _encabezados):
    """Estado del endpoint, guardado 30 s para no consultarlo en cada interacción."""
    return ce.estado_endpoint(host, _encabezados, endpoint)


def barra_lateral(credenciales, error_credenciales):
    st.sidebar.header("Endpoint publicado")
    if URL_BASE_API:
        st.sidebar.warning(f"Modo plan B: las predicciones van a la API FastAPI en {URL_BASE_API}")
    st.sidebar.write(f"Nombre: `{ENDPOINT}`")

    if credenciales:
        host, encabezados = credenciales
        if URL_BASE_API:
            st.sidebar.caption("El estado de Model Serving no aplica en modo plan B.")
        else:
            estado = estado_cacheado(host, ENDPOINT, encabezados)
            if estado["listo"]:
                st.sidebar.success(f"Estado: {estado['estado']}")
            else:
                st.sidebar.warning(f"Estado: {estado['estado']}")
            if estado["version"]:
                st.sidebar.caption(f"Versión del modelo servida: {estado['version']}")
    else:
        st.sidebar.error(error_credenciales.mensaje)

    st.sidebar.divider()
    timeout_lectura = st.sidebar.slider("Tiempo máximo de espera (segundos)", 5, 120, 30, key="timeout_lectura")
    st.sidebar.divider()
    modo = st.sidebar.selectbox("Modo demostración de fallas", list(EXPLICACION_MODO), key="modo")
    st.sidebar.info(EXPLICACION_MODO[modo])
    return modo, timeout_lectura


# --- Formulario ------------------------------------------------------------------------------------------
def formulario():
    """Dibuja el formulario. Devuelve True si se presionó «Predecir»."""
    st.selectbox(
        "Ruta de ejemplo (opcional)",
        [SIN_RUTA] + list(RUTAS),
        key="ruta",
        on_change=al_elegir_ruta,
        help="Rellena origen, destino, distancia y duración. Después puedes editarlos.",
    )
    with st.form("form_vuelo"):
        col1, col2 = st.columns(2)
        with col1:
            st.selectbox(
                "Aerolínea",
                list(AEROLINEAS),
                key="aerolinea",
                format_func=lambda codigo: f"{codigo} — {AEROLINEAS[codigo]}",
            )
            st.text_input("Aeropuerto de origen (código de 3 letras)", key="origen", max_chars=3)
            st.text_input("Aeropuerto de destino (código de 3 letras)", key="destino", max_chars=3)
            st.date_input("Fecha del vuelo", key="fecha")
        with col2:
            st.number_input("Hora de salida programada (0–23)", min_value=0, max_value=23, step=1, key="hora_salida")
            st.number_input("Hora de llegada programada (0–23)", min_value=0, max_value=23, step=1, key="hora_llegada")
            st.number_input("Distancia (millas)", min_value=1.0, max_value=6000.0, step=1.0, key="distancia")
            st.number_input("Duración programada (minutos)", min_value=15.0, max_value=900.0, step=1.0, key="duracion")
        return st.form_submit_button("Predecir retraso", type="primary")


# --- Resultados --------------------------------------------------------------------------------------------
def mostrar_resultado(resultado):
    st.metric("Probabilidad de llegar con 15 minutos o más de retraso", f"{resultado.probabilidad_retraso:.1%}")
    umbral = f"{resultado.umbral_decision:.0%}"
    if resultado.retraso_predicho == 1:
        st.warning(
            f"Es probable que el vuelo llegue con retraso: la probabilidad supera el umbral de decisión del modelo ({umbral})."
        )
    else:
        st.success(
            f"Es probable que el vuelo llegue a tiempo: la probabilidad queda por debajo del umbral de decisión del modelo ({umbral})."
        )
    destino = f"la API del plan B ({URL_BASE_API})" if URL_BASE_API else "Model Serving de Databricks"
    st.caption(
        f"Esta predicción viene del endpoint publicado `{ENDPOINT}` ({destino}), "
        "no de un modelo copiado dentro de la app. Es una estimación estadística, no una garantía."
    )
    with st.expander("Detalle técnico"):
        st.write("JSON enviado")
        st.json(resultado.json_enviado)
        st.write(
            f"HTTP {resultado.status} · latencia {resultado.segundos:.2f} s · "
            f"intentos: {resultado.intentos}"
        )
        st.write("Respuesta cruda")
        st.json(resultado.respuesta)


def mostrar_error(err):
    titulo, ayuda = AYUDA_POR_TIPO.get(err.tipo, ("Ocurrió un error", "Vuelve a intentar."))
    st.error(f"**{titulo}**\n\n{err.mensaje}")
    st.info(ayuda)
    with st.expander("Detalle técnico del error"):
        st.write(f"Tipo: `{err.tipo}` · HTTP: {err.status if err.status is not None else 'sin respuesta'} · intentos: {err.intentos}")
        st.code(err.detalle or "(sin detalle)")


def respuesta_malformada(_datos_reales):
    """Solo para el modo demostración: sustituye la respuesta real por una con probabilidad imposible."""
    return {"predictions": [{"probabilidad_retraso": 1.7, "retraso_predicho": 1, "umbral_decision": 0.4}]}


def predecir(vuelo, modo, timeout_lectura, credenciales, error_credenciales):
    """Valida, consulta el endpoint y muestra el resultado o el error. No deja pasar excepciones."""
    if credenciales is None:
        mostrar_error(error_credenciales)
        return
    host, encabezados = credenciales

    if modo == MODO_HTTP:
        vuelo = dict(vuelo, mes=13)  # sin validación local: que la API real lo rechace
    else:
        problemas = ce.validar_entrada(vuelo)
        if problemas:
            st.error("**Revisa los datos del vuelo**\n\n" + "\n".join(f"- {p}" for p in problemas))
            return

    opciones = {"timeout_lectura": timeout_lectura}
    endpoint = ENDPOINT
    if modo == MODO_TIMEOUT:
        opciones = {"timeout_lectura": 0.001, "reintentos": 0}
    elif modo == MODO_404:
        endpoint = ENDPOINT + "-no-existe"
    elif modo == MODO_INVALIDA:
        opciones["alterar_respuesta"] = respuesta_malformada
        st.warning("Modo demostración: la respuesta se está SIMULANDO (se reemplaza la real por una malformada).")

    try:
        with st.spinner("Consultando el endpoint... si estaba dormido puede tardar unos segundos."):
            resultado = ce.consultar_prediccion(
                vuelo, host=host, encabezados=encabezados, endpoint=endpoint, url_base=URL_BASE_API, **opciones
            )
    except ce.ErrorEndpoint as err:
        mostrar_error(err)
        return
    mostrar_resultado(resultado)


# --- Pestaña de documentación --------------------------------------------------------------------------------
def pestana_documentacion(credenciales):
    st.subheader("Contrato de la API")
    st.write(
        f"El modelo se consulta con `POST /serving-endpoints/{ENDPOINT}/invocations`. "
        "Se envía un vuelo (hasta 1,000 por petición) en formato `dataframe_split`."
    )
    st.table(CAMPOS_API)

    st.subheader("Ejemplo de petición")
    peticion = json.dumps(EJEMPLO_PETICION)
    curl = (
        f"curl -X POST 'https://<host-del-workspace>/serving-endpoints/{ENDPOINT}/invocations' "
        f"-H 'Authorization: Bearer <token>' -H 'Content-Type: application/json' -d '{peticion}'"
    )
    python = (
        "import requests\n\n"
        f"url = 'https://<host-del-workspace>/serving-endpoints/{ENDPOINT}/invocations'\n"
        "encabezados = {'Authorization': 'Bearer <token>'}\n"
        f"cuerpo = {EJEMPLO_PETICION!r}\n"
        "respuesta = requests.post(url, json=cuerpo, headers=encabezados, timeout=(5, 30))\n"
        "print(respuesta.status_code, respuesta.json())\n"
    )
    st.write("curl")
    st.code(curl, language="bash")
    st.write("Python")
    st.code(python, language="python")

    st.subheader("Ejemplo de respuesta")
    st.json(EJEMPLO_RESPUESTA)
    st.write(
        "`probabilidad_retraso` va de 0 a 1; `retraso_predicho` es 1 si la probabilidad alcanza `umbral_decision`. "
        "Un error de entrada devuelve HTTP 400 con `{\"error_code\": ..., \"message\": ...}`."
    )

    st.subheader("Esquema OpenAPI en vivo")
    if st.button("Obtener el esquema OpenAPI del endpoint", key="boton_openapi"):
        if credenciales is None:
            st.warning("No hay credenciales de Databricks para consultar el esquema.")
        else:
            host, encabezados = credenciales
            try:
                if URL_BASE_API:
                    esquema = ce.obtener_openapi(URL_BASE_API, encabezados, ENDPOINT, ruta="/openapi.json")
                else:
                    esquema = ce.obtener_openapi(host, encabezados, ENDPOINT)
                st.json(esquema, expanded=False)
            except ce.ErrorEndpoint as err:
                st.warning(f"No se pudo obtener el esquema: {err.mensaje}")
                st.caption(err.detalle)


# --- Programa principal ---------------------------------------------------------------------------------------
def main():
    st.set_page_config(page_title="¿Se retrasará mi vuelo?", page_icon="✈️")
    st.title("¿Se retrasará mi vuelo?")
    st.caption(
        f"Predicción de retrasos de 15 minutos o más en vuelos domésticos de EE.UU. "
        f"La aplicación consulta el endpoint publicado `{ENDPOINT}` por REST."
    )
    inicializar_estado()

    try:
        credenciales, error_credenciales = ce.credenciales_databricks(), None
    except ce.ErrorEndpoint as err:
        credenciales, error_credenciales = None, err

    modo, timeout_lectura = barra_lateral(credenciales, error_credenciales)

    pestana_prediccion, pestana_docs = st.tabs(["Predicción", "Documentación de la API"])
    with pestana_prediccion:
        enviado = formulario()
        if enviado:
            predecir(construir_vuelo(), modo, timeout_lectura, credenciales, error_credenciales)
    with pestana_docs:
        pestana_documentacion(credenciales)


main()
