# =========================================================
# 05 - SIMULADOR DE PRESUPUESTO DE VIAJE
# =========================================================
# Objetivo:
# Utilizar el modelo entrenado para estimar el coste total
# de hospedaje de un viaje y determinar si el presupuesto
# del usuario es suficiente.
# =========================================================

import os
import re
import time
import unicodedata
from datetime import date, timedelta
import requests
from pathlib import Path

import pandas as pd
import pickle
from pathlib import Path
from datetime import datetime, timedelta


# ---------------------------------------------------------
# 05.1 Definición de rutas
# ---------------------------------------------------------

BASE_DIR = Path(__file__).resolve().parents[2]

RAW_DIR = BASE_DIR / "data" / "raw"
INTERIM_DIR = BASE_DIR / "data" / "interim"   # ✅ NUEVO
CLEAN_DIR = BASE_DIR / "data" / "clean"
OUTPUTS_DIR = BASE_DIR / "outputs"
EXPERIMENTAL_DIR = BASE_DIR / "data" / "Experimental"
ML_DIR = BASE_DIR / "data" / "Machine Learning"

RAW_DIR.mkdir(parents=True, exist_ok=True)
INTERIM_DIR.mkdir(parents=True, exist_ok=True)  # ✅ NUEVO
CLEAN_DIR.mkdir(parents=True, exist_ok=True)
OUTPUTS_DIR.mkdir(parents=True, exist_ok=True)
EXPERIMENTAL_DIR.mkdir(parents=True, exist_ok=True)

model_dir = OUTPUTS_DIR / "regresion_precios" / "modelos"

model_path = model_dir / "random_forest_precio.pkl"
features_path = model_dir / "features_modelo_precio.pkl"

# ---------------------------------------------------------
# 05.2 Cargar modelo entrenado
# ---------------------------------------------------------

with open(model_path, "rb") as archivo_modelo:
    modelo = pickle.load(archivo_modelo)

with open(features_path, "rb") as archivo_features:
    features = pickle.load(archivo_features)

print("Modelo cargado correctamente.")


# ---------------------------------------------------------
# 05.3 Función para contar noches
# ---------------------------------------------------------

def calcular_noches(fecha_inicio, fecha_fin):

    fecha_inicio = datetime.strptime(fecha_inicio, "%Y-%m-%d")
    fecha_fin = datetime.strptime(fecha_fin, "%Y-%m-%d")

    noches = []

    fecha_actual = fecha_inicio

    while fecha_actual < fecha_fin:
        noches.append(fecha_actual)
        fecha_actual += timedelta(days=1)

    return noches


# ---------------------------------------------------------
# 05.4 Separar noches entre semana y fin de semana
# ---------------------------------------------------------

def separar_noches(noches):

    noches_semana = 0
    noches_fin_semana = 0

    for fecha in noches:

        if fecha.weekday() >= 4:
            noches_fin_semana += 1
        else:
            noches_semana += 1

    return noches_semana, noches_fin_semana


# ---------------------------------------------------------
# 05.5 Simular predicción de precio
# ---------------------------------------------------------

def predecir_precio(base_input):

    df_input = pd.DataFrame([base_input])

    return modelo.predict(df_input)[0]


# ---------------------------------------------------------
# 05.6 Ejemplo de usuario
# ---------------------------------------------------------

usuario = {

    "presupuesto": 800,

    "fecha_inicio": "2025-07-10",
    "fecha_fin": "2025-07-15",

    "id_ccaa": 1,
    "id_provincia": 29,

    "mes": 7,
    "temporada_cod": 3,

    "categoria_alojamiento_cod": 2,

    "periodo_antelacion_cod": 3,

    "valoraciones_norm": 4.2,
    "tiene_valoraciones": True
}


# ---------------------------------------------------------
# 05.7 Calcular noches
# ---------------------------------------------------------

noches = calcular_noches(
    usuario["fecha_inicio"],
    usuario["fecha_fin"]
)

noches_semana, noches_fin_semana = separar_noches(noches)

print("Noches semana:", noches_semana)
print("Noches fin de semana:", noches_fin_semana)


# ---------------------------------------------------------
# 05.8 Preparar inputs del modelo
# ---------------------------------------------------------

input_semana = {
    "id_ccaa": usuario["id_ccaa"],
    "id_provincia": usuario["id_provincia"],
    "mes": usuario["mes"],
    "temporada_cod": usuario["temporada_cod"],
    "categoria_alojamiento_cod": usuario["categoria_alojamiento_cod"],
    "periodo_antelacion_cod": usuario["periodo_antelacion_cod"],
    "valoraciones_norm": usuario["valoraciones_norm"],
    "tiene_valoraciones": usuario["tiene_valoraciones"],
    "tipo_dia_cod": 0
}

input_fin_semana = input_semana.copy()
input_fin_semana["tipo_dia_cod"] = 1


# ---------------------------------------------------------
# 05.9 Predicción de precios
# ---------------------------------------------------------

precio_semana = predecir_precio(input_semana)
precio_fin_semana = predecir_precio(input_fin_semana)

print("Precio estimado semana:", round(precio_semana,2))
print("Precio estimado fin semana:", round(precio_fin_semana,2))


# ---------------------------------------------------------
# 05.10 Calcular coste total
# ---------------------------------------------------------

coste_total = (
    noches_semana * precio_semana +
    noches_fin_semana * precio_fin_semana
)

print("\nCoste total estimado:", round(coste_total,2))


# ---------------------------------------------------------
# 05.11 Evaluar presupuesto
# ---------------------------------------------------------

if usuario["presupuesto"] >= coste_total:
    print("\nEl presupuesto ALCANZA para este viaje.")
else:
    print("\nEl presupuesto NO alcanza para este viaje.")