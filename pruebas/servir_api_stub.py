"""Arranca la API FastAPI con el modelo falso en el puerto indicado (lo usa test_api.py)."""

import os
import sys

import uvicorn

AQUI = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, AQUI)
sys.path.insert(0, sys.argv[2])  # carpeta api_fastapi

import main  # noqa: E402
from stub_modelo import ModeloFalso  # noqa: E402

main.fijar_modelo(ModeloFalso())
uvicorn.run(main.app, host="127.0.0.1", port=int(sys.argv[1]), log_level="warning")
