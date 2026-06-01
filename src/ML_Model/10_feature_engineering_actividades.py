# =========================================================
# 09 - FEATURE ENGINEERING PARA ACTIVIDADES
# =========================================================
# Objetivo:
# Preparar el dataset de actividades para modelado,
# incluyendo:
# - transformación logarítmica del precio
# - codificación de variables categóricas
# - creación de variables derivadas
# =========================================================

import os
import numpy as np
import pandas as pd
from pathlib import Path


# ---------------------------------------------------------
# 09.1 Definición de rutas
# ---------------------------------------------------------

BASE_DIR = Path(__file__).resolve().parents[2]

RAW_DIR = BASE_DIR / "data" / "raw"
INTERIM_DIR = BASE_DIR / "data" / "interim"
CLEAN_DIR = BASE_DIR / "data" / "clean"
OUTPUTS_DIR = BASE_DIR / "outputs"
ML_DIR = BASE_DIR / "data" / "Machine Learning"

RAW_DIR.mkdir(parents=True, exist_ok=True)
INTERIM_DIR.mkdir(parents=True, exist_ok=True)
CLEAN_DIR.mkdir(parents=True, exist_ok=True)
OUTPUTS_DIR.mkdir(parents=True, exist_ok=True)

input_path = os.path.join(ML_DIR, "df_actividades_base.csv")
output_path = os.path.join(ML_DIR, "df_actividades_features.csv")


# ---------------------------------------------------------
# 09.2 Cargar dataset base
# ---------------------------------------------------------

print("Leyendo dataset desde:")
print(input_path)

df = pd.read_csv(input_path)

print("\nDataset cargado correctamente.")
print("Dimensiones:", df.shape)


# ---------------------------------------------------------
# 09.3 Transformación logarítmica del target
# ---------------------------------------------------------

df["log_precio"] = np.log1p(df["precio_medio_entrada_promedio"])

print("\nTransformación log aplicada correctamente.")


# ---------------------------------------------------------
# 09.4 Crear variable de temporada
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
# 09.5 Codificar temporada
# ---------------------------------------------------------

map_temporada = {
    "baja": 1,
    "media": 2,
    "alta": 3
}

df["temporada_cod"] = df["temporada"].map(map_temporada)


# ---------------------------------------------------------
# 09.6 Codificar categoria (label encoding simple)
# ---------------------------------------------------------

categorias = sorted(df["categoria"].unique())
map_categoria = {cat: i for i, cat in enumerate(categorias)}

df["categoria_cod"] = df["categoria"].map(map_categoria)

print("\nMapa de categoría:")
print(map_categoria)


# ---------------------------------------------------------
# 09.7 Codificar subcategoria
# ---------------------------------------------------------

subcategorias = sorted(df["subcategoria"].unique())
map_subcategoria = {sub: i for i, sub in enumerate(subcategorias)}

df["subcategoria_cod"] = df["subcategoria"].map(map_subcategoria)

print("\nMapa de subcategoría:")
print(map_subcategoria)


# ---------------------------------------------------------
# 09.8 Transformación log de opiniones (opcional pero recomendable)
# ---------------------------------------------------------

df["log_opiniones"] = np.log1p(df["total_opiniones_categoria_promedio"])


# ---------------------------------------------------------
# 09.9 Selección de columnas finales
# ---------------------------------------------------------

columnas_finales = [
    "id_ccaa",
    "id_provincia",
    "mes",
    "temporada_cod",
    "categoria_cod",
    "subcategoria_cod",
    "valoracion_general_promedio",
    "valoracion_por_categoria_promedio",
    "log_opiniones",
    "log_precio"
]

df = df[columnas_finales].copy()


# ---------------------------------------------------------
# 09.10 Guardar dataset final
# ---------------------------------------------------------

df.to_csv(output_path, index=False, encoding="utf-8")

print("\nDataset de features guardado en:")
print(output_path)


# ---------------------------------------------------------
# 09.11 Vista previa
# ---------------------------------------------------------

print("\n=== PRIMERAS 5 FILAS ===")
print(df.head())

print("\n=== DIMENSIONES ===")
print(df.shape)

print("\n=== NULOS ===")
print(df.isnull().sum())