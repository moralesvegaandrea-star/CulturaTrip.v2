# =========================================================
# 01 - PREPARACIÓN DEL DATASET DE PRECIOS
# =========================================================
# Objetivo:
# Transformar el dataset de alojamientos desde formato ancho
# a formato largo, para construir una base adecuada para
# modelos de regresión que predigan precios turísticos.
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
# 01.1 Definición de rutas
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

input_path = os.path.join(CLEAN_DIR, "df_alojamientos.csv")
output_dir = os.path.join(ML_DIR, "regresion_precios")
output_path = os.path.join(ML_DIR,"df_precios_largo.csv")

# ---------------------------------------------------------
# 01.2 Cargar dataset base
# ---------------------------------------------------------

print("Leyendo dataset desde:")
print(input_path)

df = pd.read_csv(input_path)

print("\nDataset cargado correctamente.")
print("Dimensiones originales:", df.shape)


# ---------------------------------------------------------
# 01.3 Transformación a formato largo
# ---------------------------------------------------------
# Se convierten las columnas de precio entre semana y fin
# de semana en una sola variable de precio, junto con una
# variable indicadora del tipo de día.

df_largo = df.melt(
    id_vars=[
        "id_alojamiento",
        "id_pais",
        "id_ccaa",
        "id_provincia",
        "mes",
        "categoria_alojamiento",
        "periodo_antelacion",
        "tiene_valoraciones",
        "fuente",
        "granularidad_origen",
        "es_dato_replicado",
        "nivel_geografico",
        "valoraciones_norm"
    ],
    value_vars=[
        "precio_checkin_entre_semana",
        "precio_checkin_fin_semana"
    ],
    var_name="tipo_dia_original",
    value_name="precio"
)


# ---------------------------------------------------------
# 01.4 Crear variable limpia tipo_dia
# ---------------------------------------------------------

map_tipo_dia = {
    "precio_checkin_entre_semana": "semana",
    "precio_checkin_fin_semana": "fin_semana"
}

df_largo["tipo_dia"] = df_largo["tipo_dia_original"].map(map_tipo_dia)


# ---------------------------------------------------------
# 01.5 Seleccionar y ordenar columnas finales
# ---------------------------------------------------------

columnas_finales = [
    "id_alojamiento",
    "id_pais",
    "id_ccaa",
    "id_provincia",
    "mes",
    "categoria_alojamiento",
    "periodo_antelacion",
    "valoraciones_norm",
    "tiene_valoraciones",
    "tipo_dia",
    "precio"
]

df_largo = df_largo[columnas_finales].copy()


# ---------------------------------------------------------
# 01.6 Guardar dataset transformado
# ---------------------------------------------------------

df_largo.to_csv(output_path, index=False, encoding="utf-8")

print("\nDataset en formato largo generado correctamente.")
print("Archivo guardado en:")
print(output_path)


# ---------------------------------------------------------
# 01.7 Vista previa del resultado
# ---------------------------------------------------------

print("\n=== PRIMERAS 5 FILAS DEL DATASET EN FORMATO LARGO ===")
print(df_largo.head())

print("\n=== DIMENSIONES DEL DATASET EN FORMATO LARGO ===")
print(df_largo.shape)

print("\n=== DISTRIBUCIÓN DE tipo_dia ===")
print(df_largo["tipo_dia"].value_counts())