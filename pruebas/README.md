# Pruebas automáticas de la aplicación y de la API (plan B)

Estas pruebas demuestran, de forma repetible, que la aplicación **maneja errores, tiempos de espera y respuestas inválidas**
(rúbrica, criterio 4).

| Archivo | Qué cubre |
|---|---|
| `test_cliente.py` | El cliente `app/cliente_endpoint.py` contra un servidor HTTP falso. **Errores**: cada fila de la tabla de errores (400, 401/403, 404, 429, 500/502/503/504), mensaje legible de la API y reintentos con espera creciente. **Tiempos de espera**: lectura lenta y conexión rechazada. **Respuestas inválidas**: 200 que no es JSON, falta `predictions`, probabilidad fuera de rango, NaN o texto, número de filas incorrecto. También la validación de la entrada. |
| `test_app.py` | La interfaz Streamlit (`app/app.py`) con `streamlit.testing`: modo normal y los cuatro modos de demostración de fallas; verifica el mensaje que ve la persona y que nunca aparece un error sin controlar. |
| `test_api.py` | La API FastAPI del plan B con un modelo falso: validación (422), formato de Model Serving, errores 400/404/500/503, conversión de tipos, esquema OpenAPI; y una prueba donde el cliente real consulta la API servida por `uvicorn`. |
| `conftest.py`, `stub_modelo.py`, `servir_api_stub.py` | Apoyo: servidor falso (puertos 5201 a 5210), modelo falso y arranque de la API con ese modelo. |

## Cómo correrlas

Desde la raíz del repositorio.

**PowerShell (Windows)**

```powershell
python -m venv .venv-pruebas
.venv-pruebas\Scripts\Activate.ps1
pip install -r pruebas/requirements.txt
$env:PYTHONDONTWRITEBYTECODE = "1"
python -m pytest pruebas -q -p no:cacheprovider
```

**bash (Linux, macOS o Git Bash)**

```bash
python -m venv .venv-pruebas
source .venv-pruebas/bin/activate   # en Git Bash de Windows: source .venv-pruebas/Scripts/activate
pip install -r pruebas/requirements.txt
PYTHONDONTWRITEBYTECODE=1 python -m pytest pruebas -q -p no:cacheprovider
```

Si falta un paquete, los archivos que dependen de él se omiten solos (ver `requirements.txt`). Las pruebas usan los puertos
locales 5201 a 5210; si alguno está ocupado, ciérralo antes.

## Qué NO prueban

Corren contra un **servidor falso local**, no contra Databricks: no comprueban Model Serving, Databricks Apps, los permisos ni los
tiempos reales de despertar de un endpoint. Para eso están las pruebas en vivo del notebook `05_publicar_endpoint` y los modos de
demostración de fallas de la app.
