"""Utilidades compartidas: servidor HTTP falso (puertos 5201-5210) y rutas del proyecto."""

import json
import os
import socket
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import pytest

PROYECTO = Path(__file__).resolve().parent.parent  # raíz del repositorio
DIR_APP = str(PROYECTO / "app")
DIR_API = str(PROYECTO / "api_fastapi")
for ruta in (DIR_APP, DIR_API):
    if ruta not in sys.path:
        sys.path.insert(0, ruta)

PUERTOS_MOCK = list(range(5201, 5209))  # 5201-5208; 5209 = sin servidor; 5210 = API FastAPI
PUERTO_SIN_SERVIDOR = 5209
PUERTO_API = 5210

VUELO = {
    "mes": 7, "dia_semana": 5, "hora_salida_prog": 18, "hora_llegada_prog": 21,
    "distancia_millas": 1947.0, "duracion_prog_min": 285.0,
    "aerolinea": "DL", "origen": "ATL", "destino": "LAX",
}
OK = {"predictions": [{"probabilidad_retraso": 0.3124, "retraso_predicho": 0, "umbral_decision": 0.41}]}


class Respuesta:
    """Una respuesta programada del servidor falso: estado, cuerpo (dict -> JSON, str/bytes tal cual), retraso."""

    def __init__(self, status=200, cuerpo=None, retraso=0.0):
        self.status, self.cuerpo, self.retraso = status, cuerpo, retraso


class ServidorFalso:
    def __init__(self, puerto):
        self.puerto = puerto
        self.guion = []
        self.peticiones = []  # (ruta, encabezados, cuerpo)
        self.responder = None  # opcional: función (método, ruta, cuerpo) -> Respuesta
        servidor = self

        class Manejador(BaseHTTPRequestHandler):
            def _atender(self):
                longitud = int(self.headers.get("Content-Length") or 0)
                cuerpo = self.rfile.read(longitud).decode("utf-8") if longitud else ""
                servidor.peticiones.append((self.path, dict(self.headers), cuerpo))
                if servidor.responder is not None:
                    r = servidor.responder(self.command, self.path, cuerpo)
                elif len(servidor.guion) > 1:
                    r = servidor.guion.pop(0)
                else:
                    r = servidor.guion[0] if servidor.guion else Respuesta(200, OK)
                if r.retraso:
                    time.sleep(r.retraso)
                datos = r.cuerpo
                if isinstance(datos, (dict, list)):
                    datos = json.dumps(datos)
                datos = (datos or "").encode("utf-8") if not isinstance(datos, bytes) else datos
                try:
                    self.send_response(r.status)
                    self.send_header("Content-Type", "application/json")
                    self.send_header("Content-Length", str(len(datos)))
                    self.end_headers()
                    self.wfile.write(datos)
                except (BrokenPipeError, ConnectionResetError, ConnectionAbortedError):
                    pass  # el cliente ya cortó (prueba de timeout)

            do_POST = _atender
            do_GET = _atender

            def log_message(self, *args):
                pass

        ThreadingHTTPServer.daemon_threads = True
        self.http = ThreadingHTTPServer(("127.0.0.1", puerto), Manejador)
        self.hilo = threading.Thread(target=self.http.serve_forever, kwargs={"poll_interval": 0.02}, daemon=True)
        self.hilo.start()

    @property
    def url(self):
        return f"http://127.0.0.1:{self.puerto}"

    def programar(self, *respuestas):
        self.guion = list(respuestas)
        self.peticiones.clear()

    def cerrar(self):
        self.http.shutdown()
        self.http.server_close()


def _puerto_libre():
    for puerto in PUERTOS_MOCK:
        with socket.socket() as s:
            try:
                s.bind(("127.0.0.1", puerto))
            except OSError:
                continue
            return puerto
    raise RuntimeError("No hay puertos libres entre 5201 y 5208")


@pytest.fixture
def servidor():
    s = ServidorFalso(_puerto_libre())
    yield s
    s.cerrar()


class Reloj:
    """Sustituye a time.sleep: guarda las esperas pedidas sin dormir de verdad."""

    def __init__(self):
        self.esperas = []

    def __call__(self, segundos):
        self.esperas.append(segundos)


@pytest.fixture
def reloj():
    return Reloj()
