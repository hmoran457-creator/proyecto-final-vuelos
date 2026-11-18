"""Modelo falso con la misma interfaz que mlflow.pyfunc: predict(DataFrame) -> DataFrame."""

import pandas as pd
from mlflow.exceptions import MlflowException
from mlflow.protos.databricks_pb2 import INVALID_PARAMETER_VALUE


class ModeloFalso:
    """Probabilidad fija por hora de salida; rechaza meses fuera de rango como lo haría el modelo real."""

    def __init__(self):
        self.entradas = []

    def predict(self, df):
        self.entradas.append(df.copy())
        if (df["mes"] > 12).any() or (df["mes"] < 1).any():
            raise MlflowException("Entrada inválida: fila 0: mes fuera de rango (1-12)", error_code=INVALID_PARAMETER_VALUE)
        probabilidades = [0.25 if hora < 12 else 0.6 for hora in df["hora_salida_prog"]]
        return pd.DataFrame(
            {
                "probabilidad_retraso": probabilidades,
                "retraso_predicho": [int(p >= 0.41) for p in probabilidades],
                "umbral_decision": [0.41] * len(probabilidades),
            }
        )
