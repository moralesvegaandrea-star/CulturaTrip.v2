# =========================================================
# 04 - GUARDAR MODELO DE REGRESIÓN DE PRECIOS
# =========================================================
# Objetivo:
# Entrenar nuevamente el mejor modelo seleccionado
# (Random Forest Regressor) y guardarlo en disco para su
# posterior uso en el simulador de presupuesto de viaje.
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
from sklearn.ensemble import RandomForestRegressor


# ---------------------------------------------------------
# 04.1 Definición de rutas
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


input_path =  os.path.join(ML_DIR,"df_precios_features.csv")
model_dir = BASE_DIR / "outputs" / "regresion_precios" / "modelos"
model_path = model_dir / "random_forest_precio.pkl"
features_path = model_dir / "features_modelo_precio.pkl"

model_dir.mkdir(parents=True, exist_ok=True)

# ---------------------------------------------------------
# 04.2 Cargar dataset con features
# ---------------------------------------------------------

print("Leyendo dataset desde:")
print(input_path)

df = pd.read_csv(input_path)

print("\nDataset cargado correctamente.")
print("Dimensiones:", df.shape)


# ---------------------------------------------------------
# 04.3 Definir variables predictoras y variable objetivo
# ---------------------------------------------------------

features = [
    "id_ccaa",
    "id_provincia",
    "mes",
    "temporada_cod",
    "categoria_alojamiento_cod",
    "periodo_antelacion_cod",
    "valoraciones_norm",
    "tiene_valoraciones",
    "tipo_dia_cod"
]

X = df[features]
y = df["precio"]


# ---------------------------------------------------------
# 04.4 Crear y entrenar modelo final
# ---------------------------------------------------------

modelo_final = RandomForestRegressor(
    n_estimators=200,
    random_state=42,
    n_jobs=-1
)

modelo_final.fit(X, y)

print("\nModelo entrenado correctamente.")


# ---------------------------------------------------------
# 04.5 Guardar modelo entrenado
# ---------------------------------------------------------

with open(model_path, "wb") as archivo_modelo:
    pickle.dump(modelo_final, archivo_modelo)

print("\nModelo guardado en:")
print(model_path)


# ---------------------------------------------------------
# 04.6 Guardar lista de features
# ---------------------------------------------------------

with open(features_path, "wb") as archivo_features:
    pickle.dump(features, archivo_features)

print("\nLista de variables guardada en:")
print(features_path)