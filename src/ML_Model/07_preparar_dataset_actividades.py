# =========================================================
# 07 - PREPARACIÓN DEL DATASET DE ACTIVIDADES
# =========================================================
# Objetivo:
# Cargar el dataset de actividades enriquecido, validar su
# estructura, revisar calidad de datos (nulos, duplicados,
# tipos), y generar una base limpia inicial para análisis
# exploratorio y modelado de precios de actividades.
# =========================================================

import os
import pandas as pd
from pathlib import Path


# ---------------------------------------------------------
# 07.1 Definición de rutas
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


input_path = os.path.join(CLEAN_DIR, "fact_actividades_provincia_enriquecida.csv")
output_path = os.path.join(ML_DIR, "df_actividades_base.csv")


# ---------------------------------------------------------
# 07.2 Cargar dataset base
# ---------------------------------------------------------

print("Leyendo dataset desde:")
print(input_path)

df = pd.read_csv(input_path)

print("\nDataset cargado correctamente.")
print("Dimensiones:", df.shape)


# ---------------------------------------------------------
# 07.3 Vista general del dataset
# ---------------------------------------------------------

print("\n=== COLUMNAS DEL DATASET ===")
print(df.columns.tolist())

print("\n=== TIPOS DE DATOS ===")
print(df.dtypes)


# ---------------------------------------------------------
# 07.4 Revisión de valores nulos
# ---------------------------------------------------------

print("\n=== VALORES NULOS POR COLUMNA ===")
print(df.isnull().sum())


# ---------------------------------------------------------
# 07.5 Revisión de duplicados
# ---------------------------------------------------------

duplicados = df.duplicated().sum()

print("\n=== REGISTROS DUPLICADOS ===")
print(duplicados)


# ---------------------------------------------------------
# 07.6 Validación de variable objetivo
# ---------------------------------------------------------

print("\n=== ESTADÍSTICAS DE precio_medio_entrada_promedio ===")
print(df["precio_medio_entrada_promedio"].describe())


# ---------------------------------------------------------
# 07.7 Revisión de valores extremos básicos
# ---------------------------------------------------------

print("\n=== VALORES MÍNIMOS Y MÁXIMOS ===")
print("Min precio:", df["precio_medio_entrada_promedio"].min())
print("Max precio:", df["precio_medio_entrada_promedio"].max())


# ---------------------------------------------------------
# 07.8 Filtrado básico de datos inválidos
# ---------------------------------------------------------
# Eliminamos precios negativos o cero si existen

df = df[df["precio_medio_entrada_promedio"] > 0]

print("\nDimensiones después de filtrar precios inválidos:", df.shape)


# ---------------------------------------------------------
# 07.9 Guardar dataset base limpio
# ---------------------------------------------------------

df.to_csv(output_path, index=False, encoding="utf-8")

print("\nDataset base de actividades guardado en:")
print(output_path)


# ---------------------------------------------------------
# 07.10 Vista previa final
# ---------------------------------------------------------

print("\n=== PRIMERAS 5 FILAS ===")
print(df.head())

print("\n=== DATASET LISTO PARA EDA ===")
print("Filas:", df.shape[0])
print("Columnas:", df.shape[1])