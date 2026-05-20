# =========================================================
# 05 - SIMULADOR DE PRESUPUESTO DE ALOJAMIENTO
# =========================================================
# Objetivo:
# Integrar el modelo de ML con reglas de negocio para
# determinar si el presupuesto del usuario es suficiente.
#
# Importante:
# - El modelo predice el precio del alojamiento
# - La distribución del presupuesto proviene del TFM
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

model_path = model_dir / "random_forest_precio_v2.pkl"
features_path = model_dir / "features_modelo_precio_v2.pkl"

# ---------------------------------------------------------
# 05.2 Cargar modelo y features
# ---------------------------------------------------------

with open(model_path, "rb") as f:
    modelo = pickle.load(f)

with open(features_path, "rb") as f:
    features_modelo = pickle.load(f)


# ---------------------------------------------------------
# 05.3 Inputs del usuario (ejemplo)
# ---------------------------------------------------------

presupuesto_total = 1000  # €
noches = 3

mes = 8
valoraciones_norm = 4.2
tiene_valoraciones = 1
antelacion_dias = 30
tipo_dia_cod = 1  # fin de semana

categoria = "hotel 4 estrellas"


# ---------------------------------------------------------
# 05.4 Distribución del presupuesto (según TFM)
# ---------------------------------------------------------
# Fuente: distribución de gasto turístico definida en el TFM
# Alojamiento = 35%
# ---------------------------------------------------------

porcentaje_alojamiento = 0.35

presupuesto_alojamiento = presupuesto_total * porcentaje_alojamiento


# ---------------------------------------------------------
# 05.5 Crear input para el modelo
# ---------------------------------------------------------

input_dict = {col: 0 for col in features_modelo}

# variables numéricas
input_dict["mes"] = mes
input_dict["valoraciones_norm"] = valoraciones_norm
input_dict["tiene_valoraciones"] = tiene_valoraciones
input_dict["antelacion_dias"] = antelacion_dias
input_dict["tipo_dia_cod"] = tipo_dia_cod

# categoría (one-hot)
col_categoria = f"categoria_alojamiento_{categoria}"

if col_categoria in input_dict:
    input_dict[col_categoria] = 1


# convertir a DataFrame
input_df = pd.DataFrame([input_dict])


# ---------------------------------------------------------
# 05.6 Predicción
# ---------------------------------------------------------

precio_noche = modelo.predict(input_df)[0]

costo_total = precio_noche * noches


# ---------------------------------------------------------
# 05.7 Evaluación de presupuesto
# ---------------------------------------------------------

alcanza = costo_total <= presupuesto_alojamiento


# ---------------------------------------------------------
# 05.8 Resultados
# ---------------------------------------------------------

print("\n=== RESULTADOS SIMULACIÓN ===")

print(f"Presupuesto total: {presupuesto_total} €")
print(f"Presupuesto alojamiento (35%): {presupuesto_alojamiento:.2f} €")

print(f"\nPrecio estimado por noche: {precio_noche:.2f} €")
print(f"Noches: {noches}")
print(f"Costo total alojamiento: {costo_total:.2f} €")

print("\n¿Alcanza el presupuesto?")
print("✅ SÍ" if alcanza else "❌ NO")


# ---------------------------------------------------------
# 05.9 Conclusión
# ---------------------------------------------------------

print("\n=== CONCLUSIÓN ===")
print("El modelo estima el costo del alojamiento utilizando variables turísticas.")
print("Posteriormente, se compara contra el presupuesto disponible definido")
print("por la distribución porcentual del gasto turístico establecida en el TFM.")