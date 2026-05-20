# =========================================================
# 02 - FEATURE ENGINEERING (MODELO V2 CORREGIDO)
# =========================================================
# Objetivo:
# Transformar las variables del dataset limpio en variables
# numéricas aptas para modelos de regresión.
#
# Decisiones metodológicas:
# - Se evita el uso de IDs como predictores.
# - Las categorías nominales se codifican con One-Hot Encoding.
# - La antelación se transforma a días para mejorar interpretación.
# - El precio se mantiene como variable objetivo en escala original.
# =========================================================

import os
import re
import time
import unicodedata
from datetime import date, timedelta
import requests
import pandas as pd
from pathlib import Path


# ---------------------------------------------------------
# 02.1 Definición de rutas
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

input_path = os.path.join(ML_DIR, "df_precios_limpio_v2.csv")
output_path = os.path.join(ML_DIR, "df_precios_features_v2.csv")

# ---------------------------------------------------------
# 02.2 Cargar dataset limpio
# ---------------------------------------------------------

print("Leyendo dataset desde:")
print(input_path)

df = pd.read_csv(input_path)

print("\nDataset cargado correctamente.")
print("Dimensiones iniciales:", df.shape)


# ---------------------------------------------------------
# 02.3 Crear variable de antelación en días
# ---------------------------------------------------------

map_antelacion_dias = {
    "1 semana": 7,
    "2 semanas": 14,
    "1 mes": 30,
    "2-3 meses": 75,
    "3 meses": 90
}

df["antelacion_dias"] = df["periodo_antelacion"].map(map_antelacion_dias)


# ---------------------------------------------------------
# 02.4 Codificar tipo de día
# ---------------------------------------------------------

map_tipo_dia = {
    "semana": 0,
    "fin_semana": 1
}

df["tipo_dia_cod"] = df["tipo_dia"].map(map_tipo_dia)


# ---------------------------------------------------------
# 02.5 Convertir booleanos a enteros
# ---------------------------------------------------------

df["tiene_valoraciones"] = df["tiene_valoraciones"].astype(int)


# ---------------------------------------------------------
# 02.6 One-Hot Encoding para categoría de alojamiento
# ---------------------------------------------------------
# La categoría es nominal, por lo que no se representa con
# códigos ordinales. Esto evita imponer relaciones numéricas
# artificiales entre tipos de alojamiento.
# ---------------------------------------------------------

df_features = pd.get_dummies(
    df,
    columns=["categoria_alojamiento"],
    drop_first=False,
    dtype=int
)


# ---------------------------------------------------------
# 02.7 Eliminar columnas textuales ya transformadas
# ---------------------------------------------------------

columnas_eliminar = [
    "periodo_antelacion",
    "tipo_dia"
]

df_features = df_features.drop(columns=columnas_eliminar, errors="ignore")


# ---------------------------------------------------------
# 02.8 Validar nulos después de transformar
# ---------------------------------------------------------

print("\n=== NULOS POR COLUMNA DESPUÉS DE FEATURE ENGINEERING ===")
print(df_features.isnull().sum())


# ---------------------------------------------------------
# 02.9 Validar columnas finales
# ---------------------------------------------------------

print("\nColumnas finales:")
print(df_features.columns.tolist())

print("\nDimensiones finales:", df_features.shape)


# ---------------------------------------------------------
# 02.10 Guardar dataset con features
# ---------------------------------------------------------

df_features.to_csv(output_path, index=False, encoding="utf-8")

print("\nDataset con features guardado correctamente en:")
print(output_path)


# ---------------------------------------------------------
# 02.11 Vista previa
# ---------------------------------------------------------

print("\n=== PRIMERAS 10 FILAS DEL DATASET CON FEATURES ===")
print(df_features.head(10))


# ---------------------------------------------------------
# 02.12 Conclusión del paso
# ---------------------------------------------------------

print("\n=== CONCLUSIÓN PASO 02 ===")
print("- Se codificó correctamente la categoría de alojamiento.")
print("- Se transformó la antelación a días.")
print("- Se codificó tipo de día como variable binaria.")
print("- El dataset queda listo para entrenamiento del modelo.")
