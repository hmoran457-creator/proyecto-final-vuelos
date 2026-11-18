# Databricks notebook source
# MAGIC %md
# MAGIC # `_modelo` — clase del modelo, pipeline y funciones del endpoint
# MAGIC
# MAGIC Este notebook **no se corre solo**. Los notebooks `prueba_de_humo`, `04_entrenamiento` y
# MAGIC `05_publicar_endpoint` lo cargan con `%run ./_modelo` **después** de `%run ./_config`, porque usa
# MAGIC las listas de columnas que define `_config`.
# MAGIC
# MAGIC | Pieza | Para qué sirve |
# MAGIC |---|---|
# MAGIC | `ModeloRetrasos` | Envuelve el pipeline de scikit-learn en un modelo `mlflow.pyfunc` que **valida cada consulta** (rangos, códigos conocidos, coherencia) antes de predecir y devuelve probabilidad, predicción y umbral. |
# MAGIC | `construir_pipeline(tipo)` | Arma el pipeline sin entrenar: **escalado** de números + **codificación** de categorías + clasificador. `tipo` es `"logistica"` (línea base) o `"gradiente"`. |
# MAGIC | `registrar_modelo(...)` | Registra el modelo en Unity Catalog con firma, ejemplo de entrada, librerías fijas y alias. |
# MAGIC | `credenciales()`, `llamar_api(...)` | Llaman a la API REST de Databricks con `requests`. |
# MAGIC | `publicar_endpoint`, `esperar_endpoint`, `consultar_endpoint`, `obtener_openapi` | Crean o actualizan el endpoint de Model Serving, esperan a que quede listo, lo consultan y bajan su esquema OpenAPI. |
# MAGIC
# MAGIC **Por qué la validación vive dentro del modelo:** así el endpoint rechaza una consulta absurda
# MAGIC (mes 13, una aerolínea que no existe, un vuelo de 2,000 millas en 30 minutos) con un mensaje claro
# MAGIC en lugar de devolver una probabilidad inventada. La aplicación y cualquier otro cliente quedan
# MAGIC protegidos sin repetir las reglas.

# COMMAND ----------

import inspect
import json
import os
import time

import mlflow
import mlflow.pyfunc
import numpy as np
import pandas as pd
import requests
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler, TargetEncoder

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. La clase del modelo
# MAGIC
# MAGIC Ojo si la modificas: MLflow guarda esta clase **completa** dentro del modelo (con `cloudpickle`),
# MAGIC y el endpoint la vuelve a armar en otra máquina donde no existen tus notebooks. Por eso todo lo
# MAGIC que necesita está adentro: los `import` van dentro de los métodos y las reglas se guardan como
# MAGIC atributos. No uses aquí variables ni funciones definidas fuera de la clase.

# COMMAND ----------


class ModeloRetrasos(mlflow.pyfunc.PythonModel):
    """Pipeline de scikit-learn + validación de entradas + umbral de decisión.

    Entrada: tabla con las 9 columnas del modelo (máximo 1,000 filas).
    Salida: tabla con probabilidad_retraso, retraso_predicho y umbral_decision.
    """

    def __init__(self, pipeline, umbral, aerolineas, aeropuertos, columnas_enteras, columnas_decimales, columnas_texto):
        self.pipeline = pipeline
        self.umbral = round(float(umbral), 4)
        self.aerolineas = sorted({str(a).strip().upper() for a in aerolineas})
        self.aeropuertos = sorted({str(a).strip().upper() for a in aeropuertos})
        self.columnas_enteras = list(columnas_enteras)
        self.columnas_decimales = list(columnas_decimales)
        self.columnas_texto = list(columnas_texto)
        self.columnas = self.columnas_enteras + self.columnas_decimales + self.columnas_texto
        # Mismas reglas de dominio que la capa plata (ver docs/CONTEXTO_TECNICO.md, sección 5).
        self.rangos_enteros = {"mes": (1, 12), "dia_semana": (1, 7), "hora_salida_prog": (0, 23), "hora_llegada_prog": (0, 23)}
        # (mínimo, máximo, ¿el mínimo queda excluido?)
        self.rangos_decimales = {"distancia_millas": (0.0, 6000.0, True), "duracion_prog_min": (15.0, 900.0, False)}
        self.velocidad_minima = 40.0
        self.velocidad_maxima = 700.0
        self.max_filas = 1000
        self.max_problemas = 10

    # ------------------------------------------------------------------------------------------
    def predict(self, context, model_input, params=None):
        import numpy as np
        import pandas as pd
        from mlflow.exceptions import MlflowException
        from mlflow.protos.databricks_pb2 import INVALID_PARAMETER_VALUE

        datos, problemas = self._normalizar_y_validar(model_input)
        if problemas:
            mostrados = problemas[: self.max_problemas]
            mensaje = "Entrada inválida: " + "; ".join(mostrados)
            if len(problemas) > len(mostrados):
                mensaje += f"; ... y {len(problemas) - len(mostrados)} problema(s) más"
            raise MlflowException(mensaje, error_code=INVALID_PARAMETER_VALUE)

        probabilidad = np.round(self.pipeline.predict_proba(datos)[:, 1].astype("float64"), 4)
        return pd.DataFrame(
            {
                "probabilidad_retraso": probabilidad,
                # Se compara la probabilidad YA redondeada: así el cliente puede comprobar la regla
                # retraso_predicho = 1 si probabilidad_retraso >= umbral_decision con lo que recibe.
                "retraso_predicho": (probabilidad >= self.umbral).astype("int64"),
                "umbral_decision": np.full(len(probabilidad), self.umbral, dtype="float64"),
            }
        )

    # ------------------------------------------------------------------------------------------
    def _normalizar_y_validar(self, model_input):
        """Devuelve (tabla_limpia, []) si todo está bien, o (None, [problemas]) si no."""
        import numpy as np
        import pandas as pd

        if isinstance(model_input, dict):
            try:
                model_input = pd.DataFrame(model_input)
            except ValueError:
                model_input = pd.DataFrame([model_input])
        elif isinstance(model_input, list):
            model_input = pd.DataFrame(model_input)
        if not isinstance(model_input, pd.DataFrame):
            return None, [f"se esperaba una tabla con columnas y llegó {type(model_input).__name__}"]

        n_filas = len(model_input)
        if n_filas == 0:
            return None, ["la petición no trae filas"]
        if n_filas > self.max_filas:
            return None, [f"la petición trae {n_filas} filas y el máximo es {self.max_filas}"]
        faltantes = [c for c in self.columnas if c not in model_input.columns]
        if faltantes:
            return None, [f"falta la columna '{c}'" for c in faltantes]

        df = model_input.reset_index(drop=True)
        hallazgos = []  # (fila, orden, mensaje): se ordenan al final por fila
        limpio = {}

        def anotar(mascara, orden, redactar):
            for i in np.flatnonzero(mascara):
                hallazgos.append((int(i), orden, redactar(int(i))))

        # --- Columnas de texto: sin espacios y en mayúsculas ---------------------------------
        texto = {}
        falta_texto = {}
        for c in self.columnas_texto:
            serie = df[c].astype("string").str.strip().str.upper()
            falta = (serie.isna() | (serie == "")).fillna(True).to_numpy(dtype=bool)
            texto[c] = serie.fillna("").astype(str).to_numpy(dtype=object)
            falta_texto[c] = falta
            limpio[c] = texto[c]

        # --- Columnas numéricas -----------------------------------------------------------------
        valores = {}
        utilizable = {}
        for orden, c in enumerate(self.columnas_enteras + self.columnas_decimales):
            original = df[c]
            falta = original.isna().to_numpy(dtype=bool)
            numero = pd.to_numeric(original, errors="coerce").astype("float64").to_numpy()
            no_numero = ~falta & np.isnan(numero)
            no_finito = ~falta & ~no_numero & ~np.isfinite(numero)
            es_entero = c in self.columnas_enteras
            no_entero = np.zeros(n_filas, dtype=bool)
            if es_entero:
                no_entero = ~falta & ~no_numero & ~no_finito & (numero != np.round(numero))
            ok = ~falta & ~no_numero & ~no_finito & ~no_entero

            tipo = "un número entero" if es_entero else "un número"
            anotar(falta, orden, lambda i, c=c: f"falta el valor de {c}")
            anotar(no_numero, orden, lambda i, c=c, o=original: f"{c}={o.iloc[i]!r} no es {tipo}")
            anotar(no_finito | no_entero, orden, lambda i, c=c, v=numero: f"{c}={v[i]:g} no es {tipo}")

            if es_entero:
                minimo, maximo = self.rangos_enteros[c]
                fuera = ok & ((numero < minimo) | (numero > maximo))
                anotar(fuera, orden, lambda i, c=c, v=numero, a=minimo, b=maximo: f"{c}={v[i]:g} fuera de rango ({a}-{b})")
            else:
                minimo, maximo, excluye_minimo = self.rangos_decimales[c]
                debajo = (numero <= minimo) if excluye_minimo else (numero < minimo)
                fuera = ok & (debajo | (numero > maximo))
                regla = f"mayor que {minimo:g} y hasta {maximo:g}" if excluye_minimo else f"{minimo:g}-{maximo:g}"
                anotar(fuera, orden, lambda i, c=c, v=numero, r=regla: f"{c}={v[i]:g} fuera de rango ({r})")
            valores[c] = numero
            utilizable[c] = ok & ~fuera

        # --- Velocidad implícita: distancia / (duración en horas) ----------------------------------
        orden = len(self.columnas_enteras) + len(self.columnas_decimales)
        ambos = utilizable["distancia_millas"] & utilizable["duracion_prog_min"]
        with np.errstate(divide="ignore", invalid="ignore"):
            velocidad = valores["distancia_millas"] / (valores["duracion_prog_min"] / 60.0)
        imposible = ambos & ((velocidad < self.velocidad_minima) | (velocidad > self.velocidad_maxima))
        anotar(
            imposible,
            orden,
            lambda i: (
                f"velocidad implícita de {velocidad[i]:.0f} mph fuera de rango "
                f"({self.velocidad_minima:g}-{self.velocidad_maxima:g}): revisa distancia_millas y duracion_prog_min"
            ),
        )

        # --- Categorías conocidas y coherencia ---------------------------------------------------
        orden += 1
        for c in self.columnas_texto:
            anotar(falta_texto[c], orden, lambda i, c=c: f"falta el valor de {c}")
        orden += 1
        aerolinea = texto["aerolinea"]
        desconocida = ~falta_texto["aerolinea"] & ~pd.Series(aerolinea).isin(self.aerolineas).to_numpy()
        lista = ", ".join(self.aerolineas[:20])
        anotar(desconocida, orden, lambda i: f"aerolinea='{aerolinea[i]}' no es una aerolínea conocida por el modelo (conocidas: {lista})")
        for c in ("origen", "destino"):
            orden += 1
            desconocido = ~falta_texto[c] & ~pd.Series(texto[c]).isin(self.aeropuertos).to_numpy()
            anotar(desconocido, orden, lambda i, c=c: f"{c}='{texto[c][i]}' no es un aeropuerto conocido por el modelo")
        orden += 1
        iguales = ~falta_texto["origen"] & ~falta_texto["destino"] & (texto["origen"] == texto["destino"])
        anotar(iguales, orden, lambda i: f"origen y destino son el mismo aeropuerto ({texto['origen'][i]})")

        if hallazgos:
            hallazgos.sort(key=lambda h: (h[0], h[1]))
            return None, [f"fila {fila}: {mensaje}" for fila, _, mensaje in hallazgos]

        for c in self.columnas_enteras:
            limpio[c] = np.round(valores[c]).astype("int64")
        for c in self.columnas_decimales:
            limpio[c] = valores[c].astype("float64")
        return pd.DataFrame({c: limpio[c] for c in self.columnas}), []


# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. El pipeline: normalización + codificación + clasificador
# MAGIC
# MAGIC | Paso (nombre en el pipeline) | `logistica` (línea base) | `gradiente` |
# MAGIC |---|---|---|
# MAGIC | `preprocesamiento` → `escalado` | `StandardScaler` en `distancia_millas` y `duracion_prog_min` | `StandardScaler` en las 6 columnas numéricas |
# MAGIC | `preprocesamiento` → `codificacion_one_hot` | `OneHotEncoder` en aerolínea, origen, destino **y** mes, día y horas (su efecto no es lineal: las 18 h no son "3 veces" las 6 h) | `OneHotEncoder` solo en `aerolinea` (15 categorías) |
# MAGIC | `preprocesamiento` → `codificacion_objetivo` | — | `TargetEncoder` en `origen` y `destino`: cada aeropuerto se resume en su tasa de retraso suavizada (con validación cruzada interna para no filtrar la etiqueta) en lugar de cientos de columnas |
# MAGIC | `clasificador` | `LogisticRegression(class_weight="balanced")` | `HistGradientBoostingClassifier(class_weight="balanced", early_stopping=True)` |
# MAGIC
# MAGIC `class_weight="balanced"` le da más peso a la clase minoritaria (vuelos retrasados) para que el
# MAGIC modelo no se limite a decir "a tiempo" siempre. El escalado no cambia nada para los árboles del
# MAGIC modelo de gradiente, pero se deja para que ambos pipelines tengan la misma estructura.

# COMMAND ----------

MIN_FRECUENCIA_CATEGORIA = 50  # en la logística, categorías con menos casos se agrupan como "poco frecuentes"


def construir_pipeline(tipo):
    """Pipeline de scikit-learn sin entrenar. tipo: "logistica" o "gradiente"."""
    enteras, decimales, texto = list(COLUMNAS_ENTERAS), list(COLUMNAS_DECIMALES), list(COLUMNAS_TEXTO)
    if tipo == "logistica":
        preprocesamiento = ColumnTransformer(
            transformers=[
                ("escalado", StandardScaler(), decimales),
                (
                    "codificacion_one_hot",
                    OneHotEncoder(handle_unknown="ignore", min_frequency=MIN_FRECUENCIA_CATEGORIA),
                    texto + enteras,
                ),
            ],
            remainder="drop",
        )
        clasificador = LogisticRegression(class_weight="balanced", max_iter=1000)
    elif tipo == "gradiente":
        preprocesamiento = ColumnTransformer(
            transformers=[
                ("escalado", StandardScaler(), enteras + decimales),
                ("codificacion_objetivo", TargetEncoder(target_type="binary", random_state=42), ["origen", "destino"]),
                ("codificacion_one_hot", OneHotEncoder(sparse_output=False, handle_unknown="ignore"), ["aerolinea"]),
            ],
            remainder="drop",
            sparse_threshold=0.0,  # salida densa
        )
        clasificador = HistGradientBoostingClassifier(
            class_weight="balanced", early_stopping=True, max_iter=300, random_state=42
        )
    else:
        raise ValueError(f"tipo debe ser 'logistica' o 'gradiente', no {tipo!r}")
    return Pipeline([("preprocesamiento", preprocesamiento), ("clasificador", clasificador)])


# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Registro en Unity Catalog

# COMMAND ----------


def configurar_registro():
    """Dentro de Databricks usa el registro de Unity Catalog; fuera (pruebas locales) deja el que haya."""
    if "DATABRICKS_RUNTIME_VERSION" in os.environ:
        mlflow.set_registry_uri("databricks-uc")


def firma_del_modelo():
    """Firma explícita: Unity Catalog la exige y el endpoint la usa para rechazar tipos incorrectos."""
    from mlflow.models.signature import ModelSignature
    from mlflow.types.schema import ColSpec, Schema

    entradas = Schema(
        [ColSpec("long", c) for c in COLUMNAS_ENTERAS]
        + [ColSpec("double", c) for c in COLUMNAS_DECIMALES]
        + [ColSpec("string", c) for c in COLUMNAS_TEXTO]
    )
    salidas = Schema(
        [
            ColSpec("double", "probabilidad_retraso"),
            ColSpec("long", "retraso_predicho"),
            ColSpec("double", "umbral_decision"),
        ]
    )
    return ModelSignature(inputs=entradas, outputs=salidas)


def requisitos_pip():
    """Versiones exactas del notebook. Se pide mlflow completo (no mlflow-skinny) para el endpoint."""
    import cloudpickle
    import sklearn

    requisitos = [
        f"mlflow=={mlflow.__version__}",
        f"scikit-learn=={sklearn.__version__}",
        f"pandas=={pd.__version__}",
        f"numpy=={np.__version__}",
        f"cloudpickle=={cloudpickle.__version__}",
    ]
    # Las versiones de MLflow cuyo servidor es FastAPI pero todavía usa app.route() (por ejemplo 3.8.1) no
    # arrancan con Starlette 1.x, que es lo que hoy instala pip junto con fastapi. Solo en ese caso se fija
    # Starlette (MLflow 2.x usa Flask y no lo necesita).
    try:
        import mlflow.pyfunc.scoring_server as servidor_mlflow

        fuente = inspect.getsource(servidor_mlflow.init)
        if "FastAPI" in fuente and "app.route(" in fuente:
            requisitos.append("starlette<1")
    except Exception:
        pass
    return requisitos


def registrar_modelo(pipeline, umbral, ejemplo, aerolineas, aeropuertos, nombre_modelo, alias, descripcion=""):
    """Registra el pipeline envuelto en ModeloRetrasos, le pone el alias y devuelve la versión (texto)."""
    from mlflow import MlflowClient

    configurar_registro()
    modelo = ModeloRetrasos(
        pipeline, umbral, aerolineas, aeropuertos, COLUMNAS_ENTERAS, COLUMNAS_DECIMALES, COLUMNAS_TEXTO
    )

    ejemplo = pd.DataFrame(ejemplo)[COLUMNAS_MODELO].copy()
    for c in COLUMNAS_ENTERAS:
        ejemplo[c] = ejemplo[c].astype("int64")
    for c in COLUMNAS_DECIMALES:
        ejemplo[c] = ejemplo[c].astype("float64")
    for c in COLUMNAS_TEXTO:
        ejemplo[c] = ejemplo[c].astype(str)

    requisitos = requisitos_pip()
    print("Librerías que instalará el endpoint:", ", ".join(requisitos))
    argumentos = dict(
        python_model=modelo,
        signature=firma_del_modelo(),
        input_example=ejemplo,
        pip_requirements=requisitos,
        registered_model_name=nombre_modelo,
    )
    # MLflow 3 usa name=; MLflow 2 usa artifact_path=.
    if "name" in inspect.signature(mlflow.pyfunc.log_model).parameters:
        argumentos["name"] = "modelo"
    else:
        argumentos["artifact_path"] = "modelo"

    if mlflow.active_run() is None:
        with mlflow.start_run(run_name="registro_modelo"):
            info = mlflow.pyfunc.log_model(**argumentos)
    else:
        info = mlflow.pyfunc.log_model(**argumentos)

    cliente = MlflowClient()
    version = getattr(info, "registered_model_version", None)
    if version is None:  # versiones viejas de MLflow: se busca por la corrida
        versiones = [v for v in cliente.search_model_versions(f"name='{nombre_modelo}'") if v.run_id == info.run_id]
        version = max(int(v.version) for v in versiones)
    version = str(version)

    if descripcion:
        cliente.update_model_version(nombre_modelo, version, description=descripcion)
    cliente.set_registered_model_alias(nombre_modelo, alias, version)
    return version


# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. API REST de Databricks (Model Serving)
# MAGIC
# MAGIC Se usa `requests` contra la API REST en lugar de las clases del SDK porque la firma de esas clases
# MAGIC cambió entre versiones del SDK; la API REST es la misma en todas.

# COMMAND ----------

_ESTADO_CREDENCIALES = {"config": None}


def credenciales():
    """Devuelve (host, encabezados) para llamar a la API REST del workspace."""
    host, encabezados = None, None
    try:
        if _ESTADO_CREDENCIALES["config"] is None:
            from databricks.sdk import WorkspaceClient

            _ESTADO_CREDENCIALES["config"] = WorkspaceClient().config
        config = _ESTADO_CREDENCIALES["config"]
        host, encabezados = config.host, config.authenticate()
        if not host or not encabezados:
            raise RuntimeError("el SDK no devolvió host o encabezados")
    except Exception as error_sdk:
        _ESTADO_CREDENCIALES["config"] = None
        try:
            # Plan B dentro de un notebook: el token de la sesión y la URL del workspace.
            contexto = dbutils.notebook.entry_point.getDbutils().notebook().getContext()  # noqa: F821
            token = contexto.apiToken().get()
            host = spark.conf.get("spark.databricks.workspaceUrl")  # noqa: F821
            encabezados = {"Authorization": f"Bearer {token}"}
        except Exception as error_contexto:
            raise RuntimeError(
                "No se pudieron obtener credenciales para la API de Databricks.\n"
                f"  SDK de Databricks: {type(error_sdk).__name__}: {error_sdk}\n"
                f"  Contexto del notebook: {type(error_contexto).__name__}: {error_contexto}\n"
                "Corre este notebook dentro de Databricks (o define DATABRICKS_HOST y DATABRICKS_TOKEN)."
            ) from error_contexto
    host = str(host).rstrip("/")
    if not host.startswith("http://") and not host.startswith("https://"):
        host = "https://" + host
    return host, dict(encabezados)


def llamar_api(metodo, ruta, cuerpo=None, timeout=60):
    """Llama a la API REST. Devuelve (código_http, datos); datos es dict si la respuesta es JSON."""
    host, encabezados = credenciales()
    respuesta = requests.request(metodo, host + ruta, headers=encabezados, json=cuerpo, timeout=timeout)
    try:
        datos = respuesta.json()
    except ValueError:
        datos = respuesta.text
    return respuesta.status_code, datos


def _mensaje_de(datos):
    if isinstance(datos, dict):
        return datos.get("message") or datos.get("error") or json.dumps(datos, ensure_ascii=False)[:500]
    return str(datos)[:500]


def _exigir_ok(status, datos, accion):
    if 200 <= status < 300:
        return
    pista = "Revisa el menú Serving de Databricks."
    if status in (401, 403):
        pista = "Tu usuario o token no tiene permiso para esta operación de Model Serving."
    elif status == 404:
        pista = "Revisa que el nombre del endpoint y del modelo (con su versión) existan."
    elif status in (400, 429) and any(p in _mensaje_de(datos).lower() for p in ("limit", "quota", "exceed")):
        pista = (
            "Parece un límite de Free Edition (número de endpoints activos o cuota). Borra endpoints que no "
            "uses en Serving o espera a que se renueve la cuota."
        )
    raise RuntimeError(f"No se pudo {accion}: HTTP {status}: {_mensaje_de(datos)}\n{pista}")


def _versiones_servidas(config, nombre_modelo=None):
    """Versiones que aparecen en una config (o pending_config) del endpoint."""
    versiones = []
    for entidad in (config or {}).get("served_entities") or (config or {}).get("served_models") or []:
        nombre = entidad.get("entity_name") or entidad.get("model_name")
        version = entidad.get("entity_version") or entidad.get("model_version")
        if version is not None and (nombre_modelo is None or nombre == nombre_modelo):
            versiones.append(str(version))
    return versiones


def publicar_endpoint(nombre_endpoint, nombre_modelo, version):
    """Crea el endpoint o lo actualiza a esa versión. Devuelve 'creado', 'actualizado', 'en curso' o 'sin cambios'."""
    version = str(version)
    entidad = {
        "entity_name": nombre_modelo,
        "entity_version": version,
        "workload_size": "Small",
        "scale_to_zero_enabled": True,
    }
    ruta = f"/api/2.0/serving-endpoints/{nombre_endpoint}"
    status, datos = llamar_api("GET", ruta)

    if status == 404:
        print(f"El endpoint {nombre_endpoint} no existe: se crea sirviendo {nombre_modelo} versión {version}.")
        status, datos = llamar_api(
            "POST", "/api/2.0/serving-endpoints", {"name": nombre_endpoint, "config": {"served_entities": [entidad]}}
        )
        _exigir_ok(status, datos, "crear el endpoint")
        return "creado"
    _exigir_ok(status, datos, "consultar el endpoint")

    estado = datos.get("state") or {}
    if estado.get("config_update") == "IN_PROGRESS":
        if version in _versiones_servidas(datos.get("pending_config"), nombre_modelo):
            print(f"El endpoint ya se está actualizando a la versión {version}: solo hay que esperar.")
            return "en curso"
        print("El endpoint tiene otra actualización en curso; se espera a que termine antes de pedir la nueva.")
        esperar_endpoint(nombre_endpoint)
        status, datos = llamar_api("GET", ruta)
        _exigir_ok(status, datos, "consultar el endpoint")
        estado = datos.get("state") or {}

    servidas = _versiones_servidas(datos.get("config"), nombre_modelo)
    if (
        version in servidas
        and estado.get("ready") == "READY"
        and estado.get("config_update") not in ("UPDATE_FAILED", "UPDATE_CANCELED")
    ):
        print(f"El endpoint ya sirve {nombre_modelo} versión {version}: no hay nada que actualizar.")
        return "sin cambios"

    print(f"El endpoint existe (sirve: {', '.join(servidas) or 'ninguna versión'}); se actualiza a la versión {version}.")
    status, datos = llamar_api("PUT", f"{ruta}/config", {"served_entities": [entidad]})
    _exigir_ok(status, datos, "actualizar el endpoint")
    return "actualizado"


def esperar_endpoint(nombre_endpoint, minutos=45, segundos_entre_consultas=30, version=None):
    """Consulta el estado cada `segundos_entre_consultas` hasta que el endpoint quede listo.

    Listo = state.ready READY + state.config_update NOT_UPDATING (+ sirviendo `version`, si se da).
    """
    ruta = f"/api/2.0/serving-endpoints/{nombre_endpoint}"
    donde = f"en Databricks abre Serving > {nombre_endpoint} y revisa las pestañas Events y Logs"
    inicio = time.time()
    raros_seguidos = 0
    while True:
        status, datos = llamar_api("GET", ruta)
        _exigir_ok(status, datos, "consultar el estado del endpoint")
        estado = datos.get("state") or {}
        listo, actualizacion = estado.get("ready", "?"), estado.get("config_update", "?")
        servidas = _versiones_servidas(datos.get("config"))
        transcurrido = time.time() - inicio
        minuto, segundo = divmod(int(transcurrido), 60)
        print(
            f"[{minuto:02d}:{segundo:02d}] ready={listo} | config_update={actualizacion} | "
            f"versiones servidas={', '.join(servidas) or '-'}"
        )

        if actualizacion in ("UPDATE_FAILED", "UPDATE_CANCELED"):
            raise RuntimeError(
                f"El endpoint {nombre_endpoint} no pudo desplegar la nueva configuración ({actualizacion}). "
                f"Para ver la causa, {donde}. Lo más común: una librería que no se pudo instalar o un "
                "error al cargar el modelo."
            )
        if listo == "READY" and actualizacion == "NOT_UPDATING":
            if version is None or str(version) in servidas:
                print(f"Endpoint listo después de {transcurrido / 60:.1f} minutos.")
                return datos
            raros_seguidos += 1  # listo, pero con otra versión
        elif listo != "READY" and actualizacion == "NOT_UPDATING":
            raros_seguidos += 1  # ni listo ni actualizándose
        else:
            raros_seguidos = 0
        if raros_seguidos >= 3:
            raise RuntimeError(
                f"El endpoint {nombre_endpoint} no se está actualizando y no quedó como se esperaba "
                f"(ready={listo}, versiones={servidas}, se esperaba la versión {version}). Para ver qué pasó, {donde}."
            )
        if transcurrido > minutos * 60:
            raise TimeoutError(
                f"Pasaron {minutos} minutos y el endpoint {nombre_endpoint} sigue sin estar listo "
                f"(ready={listo}, config_update={actualizacion}). Puede seguir desplegándose: {donde}, "
                "y vuelve a correr esta celda más tarde."
            )
        time.sleep(segundos_entre_consultas)


def _valor_json(valor):
    """Convierte valores de numpy/pandas a tipos que JSON entiende (NaN pasa a null)."""
    if hasattr(valor, "item") and not isinstance(valor, (str, bytes)):
        valor = valor.item()
    if valor is None or valor is pd.NA:
        return None
    if isinstance(valor, float) and not np.isfinite(valor):
        return None
    return valor


def consultar_endpoint(nombre_endpoint, filas, timeout=120):
    """Manda filas (lista de dicts o DataFrame) al endpoint. Devuelve (código_http, cuerpo, segundos)."""
    if isinstance(filas, pd.DataFrame):
        filas = filas.to_dict(orient="records")
    if isinstance(filas, dict):
        filas = [filas]
    claves = []
    for fila in filas:
        for clave in fila:
            if clave not in claves:
                claves.append(clave)
    columnas = [c for c in COLUMNAS_MODELO if c in claves] + [c for c in claves if c not in COLUMNAS_MODELO]
    cuerpo = {
        "dataframe_split": {
            "columns": columnas,
            "data": [[_valor_json(fila.get(c)) for c in columnas] for fila in filas],
        }
    }
    host, encabezados = credenciales()
    inicio = time.time()
    respuesta = requests.post(
        f"{host}/serving-endpoints/{nombre_endpoint}/invocations",
        headers={**encabezados, "Content-Type": "application/json"},
        json=cuerpo,
        timeout=timeout,
    )
    segundos = round(time.time() - inicio, 2)
    try:
        cuerpo_respuesta = respuesta.json()
    except ValueError:
        cuerpo_respuesta = respuesta.text
    return respuesta.status_code, cuerpo_respuesta, segundos


def obtener_openapi(nombre_endpoint):
    """Esquema OpenAPI del endpoint (requiere que esté READY y que el modelo tenga firma)."""
    status, datos = llamar_api("GET", f"/api/2.0/serving-endpoints/{nombre_endpoint}/openapi")
    _exigir_ok(status, datos, "obtener el esquema OpenAPI")
    if isinstance(datos, str):
        datos = json.loads(datos)
    return datos


print("Modelo y funciones del endpoint cargados (_modelo).")
