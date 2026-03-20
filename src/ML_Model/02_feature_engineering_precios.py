# =========================================================
# 02 - FEATURE ENGINEERING PARA REGRESIÓN DE PRECIOS
# =========================================================
# Objetivo:
# Crear variables derivadas y codificaciones a partir del
# dataset en formato largo, para preparar una base adecuada
# para modelos de regresión de precios turísticos.
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

input_path = os.path.join(ML_DIR, "df_precios_largo.csv")
output_path = os.path.join(ML_DIR, "df_precios_features.csv")


# ---------------------------------------------------------
# 02.2 Cargar dataset en formato largo
# ---------------------------------------------------------

print("Leyendo dataset desde:")
print(input_path)

df = pd.read_csv(input_path)

print("\nDataset cargado correctamente.")
print("Dimensiones:", df.shape)


# ---------------------------------------------------------
# 02.3 Crear variable de temporada
# ---------------------------------------------------------

def clasificar_temporada(mes):
    if mes in [12, 1, 2]:
        return "baja"
    elif mes in [3, 4, 5, 10, 11]:
        return "media"
    elif mes in [6, 7, 8, 9]:
        return "alta"
    else:
        return "desconocida"

df["temporada"] = df["mes"].apply(clasificar_temporada)


# ---------------------------------------------------------
# 02.4 Codificar tipo_dia
# ---------------------------------------------------------

map_tipo_dia = {
    "semana": 0,
    "fin_semana": 1
}

df["tipo_dia_cod"] = df["tipo_dia"].map(map_tipo_dia)


# ---------------------------------------------------------
# 02.5 Codificar categoría de alojamiento
# ---------------------------------------------------------

map_categoria = {
    "hotel 3 estrellas": 1,
    "hotel 4 estrellas": 2,
    "hotel 5 estrellas": 3,
    "apartamento": 4,
    "casa entera": 5,
    "habitacion privada": 6,
    "habitacion compartida": 7,
    "alternativo": 8
}

df["categoria_alojamiento_cod"] = df["categoria_alojamiento"].map(map_categoria)


# ---------------------------------------------------------
# 02.6 Codificar periodo de antelación
# ---------------------------------------------------------

map_antelacion = {
    "1 semana": 1,
    "2 semanas": 2,
    "1 mes": 3,
    "2-3 meses": 4,
    "3 meses": 5
}

df["periodo_antelacion_cod"] = df["periodo_antelacion"].map(map_antelacion)


# ---------------------------------------------------------
# 02.7 Codificar temporada
# ---------------------------------------------------------

map_temporada = {
    "baja": 1,
    "media": 2,
    "alta": 3
}

df["temporada_cod"] = df["temporada"].map(map_temporada)


# ---------------------------------------------------------
# 02.8 Crear indicador de fin de semana premium
# ---------------------------------------------------------
# Esta variable refleja cuánto cambia el precio según el tipo de día.

df["es_fin_de_semana"] = df["tipo_dia_cod"]


# ---------------------------------------------------------
# 02.9 Seleccionar columnas finales
# ---------------------------------------------------------

columnas_finales = [
    "id_alojamiento",
    "id_pais",
    "id_ccaa",
    "id_provincia",
    "mes",
    "temporada",
    "temporada_cod",
    "categoria_alojamiento",
    "categoria_alojamiento_cod",
    "periodo_antelacion",
    "periodo_antelacion_cod",
    "valoraciones_norm",
    "tiene_valoraciones",
    "tipo_dia",
    "tipo_dia_cod",
    "es_fin_de_semana",
    "precio"
]

df = df[columnas_finales].copy()


# ---------------------------------------------------------
# 02.10 Guardar dataset con features
# ---------------------------------------------------------

df.to_csv(output_path, index=False, encoding="utf-8")

print("\nDataset con features generado correctamente.")
print("Archivo guardado en:")
print(output_path)


# ---------------------------------------------------------
# 02.11 Vista previa
# ---------------------------------------------------------

print("\n=== PRIMERAS 5 FILAS DEL DATASET CON FEATURES ===")
print(df.head())

print("\n=== DIMENSIONES DEL DATASET CON FEATURES ===")
print(df.shape)

print("\n=== NULOS POR COLUMNA ===")
print(df.isnull().sum())
