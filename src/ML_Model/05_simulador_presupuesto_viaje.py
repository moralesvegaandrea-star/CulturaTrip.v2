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
# 05.2 Cargar modelo optimizado y lista de variables
# ---------------------------------------------------------

with open(model_path, "rb") as archivo_modelo:
    modelo = pickle.load(archivo_modelo)

with open(features_path, "rb") as archivo_features:
    features = pickle.load(archivo_features)

print("Modelo optimizado cargado correctamente.")


# ---------------------------------------------------------
# 05.3 Función para clasificar temporada
# ---------------------------------------------------------

def obtener_temporada_cod(mes):
    if mes in [12, 1, 2]:
        return 1   # baja
    elif mes in [3, 4, 5, 10, 11]:
        return 2   # media
    elif mes in [6, 7, 8, 9]:
        return 3   # alta
    else:
        raise ValueError("Mes inválido para clasificar temporada.")


# ---------------------------------------------------------
# 05.4 Función para generar lista de noches
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
# 05.5 Separar noches entre semana y fin de semana
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
# 05.6 Función para predecir precio
# ---------------------------------------------------------

def predecir_precio(base_input):

    df_input = pd.DataFrame([base_input])
    df_input = df_input[features]

    return modelo.predict(df_input)[0]


# ---------------------------------------------------------
# 05.7 Definir caso de prueba del usuario
# ---------------------------------------------------------

usuario = {
    "presupuesto": 8000.00,
    "fecha_inicio": "2025-07-10",
    "fecha_fin": "2025-07-15",
    "id_ccaa": 13,
    "id_provincia": 29,
    "categoria_alojamiento_cod": 2,
    "periodo_antelacion_cod": 3,
    "valoraciones_norm": 4.2,
    "tiene_valoraciones": True
}


# ---------------------------------------------------------
# 05.8 Derivar variables del viaje
# ---------------------------------------------------------

fecha_inicio_dt = datetime.strptime(usuario["fecha_inicio"], "%Y-%m-%d")
mes = fecha_inicio_dt.month
temporada_cod = obtener_temporada_cod(mes)

noches = calcular_noches(
    usuario["fecha_inicio"],
    usuario["fecha_fin"]
)

noches_semana, noches_fin_semana = separar_noches(noches)

print("\nNoches semana:", noches_semana)
print("Noches fin de semana:", noches_fin_semana)


# ---------------------------------------------------------
# 05.9 Preparar inputs para predicción
# ---------------------------------------------------------

input_semana = {
    "id_ccaa": usuario["id_ccaa"],
    "id_provincia": usuario["id_provincia"],
    "mes": mes,
    "temporada_cod": temporada_cod,
    "categoria_alojamiento_cod": usuario["categoria_alojamiento_cod"],
    "periodo_antelacion_cod": usuario["periodo_antelacion_cod"],
    "valoraciones_norm": usuario["valoraciones_norm"],
    "tiene_valoraciones": usuario["tiene_valoraciones"],
    "tipo_dia_cod": 0
}

input_fin_semana = input_semana.copy()
input_fin_semana["tipo_dia_cod"] = 1


# ---------------------------------------------------------
# 05.10 Predicción de precios por tipo de día
# ---------------------------------------------------------

precio_semana = predecir_precio(input_semana)
precio_fin_semana = predecir_precio(input_fin_semana)

print("\nPrecio estimado entre semana:", round(precio_semana, 2))
print("Precio estimado fin de semana:", round(precio_fin_semana, 2))


# ---------------------------------------------------------
# 05.11 Calcular coste total estimado
# ---------------------------------------------------------

coste_total = (
    noches_semana * precio_semana +
    noches_fin_semana * precio_fin_semana
)

print("\nCoste total estimado del alojamiento:", round(coste_total, 2))


# ---------------------------------------------------------
# 05.12 Evaluar presupuesto del usuario (CORRECTO)
# ---------------------------------------------------------

# Distribución INE (alojamiento 35%)
pct_alojamiento = 0.35

budget_alojamiento = usuario["presupuesto"] * pct_alojamiento

print("\nPresupuesto total del usuario:", usuario["presupuesto"])
print("Presupuesto disponible para alojamiento (35%):", round(budget_alojamiento, 2))

# Comparación correcta
diferencia = budget_alojamiento - coste_total

if budget_alojamiento >= coste_total:
    print("\nEl presupuesto de alojamiento ALCANZA.")
    print("Margen disponible:", round(diferencia, 2))
else:
    print("\nEl presupuesto de alojamiento NO alcanza.")
    print("Monto faltante:", round(abs(diferencia), 2))