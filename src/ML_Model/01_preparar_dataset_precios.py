# =========================================================
# 01 - PREPARACIÓN DEL DATASET (MODELO V2 CORREGIDO)
# =========================================================
# Objetivo:
# Preparar el dataset base para el modelo de regresión,
# eliminando identificadores, metadatos técnicos y columnas
# que no aportan valor predictivo.
#
# Decisiones metodológicas:
# - No se utilizan IDs como variables predictoras.
# - Se conserva únicamente información explicativa del precio.
# - El precio se mantiene en escala original para facilitar
#   interpretación económica.
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

input_path = os.path.join(ML_DIR, "df_precios_largo.csv")
output_path = os.path.join(ML_DIR,"df_precios_limpio_v2.csv")

# ---------------------------------------------------------
# 01.2 Cargar dataset
# ---------------------------------------------------------

print("Leyendo dataset desde:")
print(input_path)

df = pd.read_csv(input_path)

print("\nDataset cargado correctamente.")
print("Dimensiones originales:", df.shape)

print("\nColumnas originales:")
print(df.columns.tolist())


# ---------------------------------------------------------
# 01.3 Eliminar columnas no predictivas
# ---------------------------------------------------------
# Se eliminan identificadores y variables técnicas.
#
# Los IDs no tienen significado numérico para el modelo.
# Su uso puede inducir aprendizaje espurio o memorización.
# ---------------------------------------------------------

columnas_eliminar = [
    "id_alojamiento",
    "id_pais",
    "id_ccaa",
    "id_provincia",
    "fuente",
    "granularidad_origen",
    "es_dato_replicado",
    "nivel_geografico"
]

df_limpio = df.drop(columns=columnas_eliminar, errors="ignore")


# ---------------------------------------------------------
# 01.4 Validar columnas finales
# ---------------------------------------------------------

columnas_esperadas = [
    "mes",
    "categoria_alojamiento",
    "periodo_antelacion",
    "valoraciones_norm",
    "tiene_valoraciones",
    "tipo_dia",
    "precio"
]

df_limpio = df_limpio[columnas_esperadas].copy()

print("\nColumnas finales:")
print(df_limpio.columns.tolist())

print("\nDimensiones finales:", df_limpio.shape)


# ---------------------------------------------------------
# 01.5 Validar nulos
# ---------------------------------------------------------

print("\n=== NULOS POR COLUMNA ===")
print(df_limpio.isnull().sum())


# ---------------------------------------------------------
# 01.6 Validar valores únicos principales
# ---------------------------------------------------------

print("\n=== VALORES ÚNICOS: CATEGORÍA ===")
print(df_limpio["categoria_alojamiento"].unique())

print("\n=== VALORES ÚNICOS: PERIODO ANTELACIÓN ===")
print(df_limpio["periodo_antelacion"].unique())

print("\n=== VALORES ÚNICOS: TIPO DÍA ===")
print(df_limpio["tipo_dia"].unique())

print("\n=== MESES DISPONIBLES ===")
print(sorted(df_limpio["mes"].unique()))


# ---------------------------------------------------------
# 01.7 Guardar dataset limpio
# ---------------------------------------------------------

df_limpio.to_csv(output_path, index=False, encoding="utf-8")

print("\nDataset limpio guardado correctamente en:")
print(output_path)


# ---------------------------------------------------------
# 01.8 Vista previa
# ---------------------------------------------------------

print("\n=== PRIMERAS 10 FILAS DEL DATASET LIMPIO ===")
print(df_limpio.head(10))


# ---------------------------------------------------------
# 01.9 Conclusión del paso
# ---------------------------------------------------------

print("\n=== CONCLUSIÓN PASO 01 ===")
print("- Se eliminaron IDs y metadatos técnicos.")
print("- Se conservaron variables explicativas del precio.")
print("- El dataset queda listo para feature engineering.")