# =========================================================
# 01 - PREPARAR DATASET (V5)
# =========================================================
# Objetivo:
# Preparar dataset final para entrenamiento del modelo V5.
#
# Incluye:
# - Variables geográficas interpretables
# - Split train/test
# - Eliminación de IDs
# - Validaciones metodológicas
#
# IMPORTANTE:
# NO se utilizan variables derivadas del precio.
# =========================================================

import os
import pandas as pd

from pathlib import Path
from sklearn.model_selection import train_test_split


# ---------------------------------------------------------
# 01.1 Rutas
# ---------------------------------------------------------

BASE_DIR = Path(__file__).resolve().parents[2]

ML_DIR = BASE_DIR / "data" / "Machine Learning"

input_path = os.path.join(ML_DIR, "df_precios_largo.csv")

OUTPUT_DIR = ML_DIR / "modelo_precios_v5"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

train_output = os.path.join(OUTPUT_DIR, "train_v5.csv")
test_output = os.path.join(OUTPUT_DIR, "test_v5.csv")


# ---------------------------------------------------------
# 01.2 Cargar dataset
# ---------------------------------------------------------

print("Leyendo dataset desde:")
print(input_path)

df = pd.read_csv(input_path)

print("\nDataset cargado correctamente.")
print("Dimensiones originales:", df.shape)


# ---------------------------------------------------------
# 01.3 Crear variables geográficas
# ---------------------------------------------------------

provincias_insulares = [
    7,   # Illes Balears
    35,  # Las Palmas
    38   # Santa Cruz de Tenerife
]

provincias_costa = [
    15, 27, 36, 33, 39, 48, 20,
    17, 8, 43, 12, 46, 3, 30,
    4, 18, 29, 11, 21, 51, 52
]

grandes_ciudades = [
    28,  # Madrid
    8    # Barcelona
]


def clasificar_zona(id_provincia):

    if id_provincia in provincias_insulares:
        return "insular"

    elif id_provincia in provincias_costa:
        return "costa"

    else:
        return "interior"


def clasificar_gran_ciudad(id_provincia):

    if id_provincia in grandes_ciudades:
        return 1

    else:
        return 0


df["tipo_zona_turistica"] = df["id_provincia"].apply(clasificar_zona)

df["gran_ciudad"] = df["id_provincia"].apply(
    clasificar_gran_ciudad
)


# ---------------------------------------------------------
# 01.4 Seleccionar variables finales
# ---------------------------------------------------------
# IMPORTANTE:
# id_provincia NO se utilizará para entrenar.
# Solo se usó para crear variables interpretables.
# ---------------------------------------------------------

columnas_modelo = [

    # Variables geográficas interpretables
    "tipo_zona_turistica",
    "gran_ciudad",

    # Variables originales V2
    "mes",
    "categoria_alojamiento",
    "periodo_antelacion",
    "valoraciones_norm",
    "tiene_valoraciones",
    "tipo_dia",

    # Target
    "precio"
]

df = df[columnas_modelo]

print("\nColumnas seleccionadas:")
print(df.columns.tolist())

print("\nDimensiones después de selección:")
print(df.shape)


# ---------------------------------------------------------
# 01.5 Validación de nulos
# ---------------------------------------------------------

print("\n=== NULOS ===")
print(df.isnull().sum())


# ---------------------------------------------------------
# 01.6 Split Train/Test
# ---------------------------------------------------------
# Muy importante:
# El split ocurre ANTES del feature engineering.
# Esto evita leakage entre train y test.
# ---------------------------------------------------------

train_df, test_df = train_test_split(

    df,
    test_size=0.20,
    random_state=42
)

print("\nTrain:", train_df.shape)
print("Test:", test_df.shape)


# ---------------------------------------------------------
# 01.7 Validaciones geográficas
# ---------------------------------------------------------

print("\n=== DISTRIBUCIÓN ZONAS TRAIN ===")
print(train_df["tipo_zona_turistica"].value_counts())

print("\n=== DISTRIBUCIÓN ZONAS TEST ===")
print(test_df["tipo_zona_turistica"].value_counts())


print("\n=== DISTRIBUCIÓN GRAN_CIUDAD TRAIN ===")
print(train_df["gran_ciudad"].value_counts())

print("\n=== DISTRIBUCIÓN GRAN_CIUDAD TEST ===")
print(test_df["gran_ciudad"].value_counts())


# ---------------------------------------------------------
# 01.8 Guardar datasets
# ---------------------------------------------------------

train_df.to_csv(
    train_output,
    index=False,
    encoding="utf-8"
)

test_df.to_csv(
    test_output,
    index=False,
    encoding="utf-8"
)

print("\nTrain guardado en:")
print(train_output)

print("\nTest guardado en:")
print(test_output)


# ---------------------------------------------------------
# 01.9 Conclusión
# ---------------------------------------------------------

print("\n=== CONCLUSIÓN PASO 01 V5 ===")

print("- Se incorporaron variables geográficas interpretables.")
print("- id_provincia NO se utiliza para entrenamiento.")
print("- El dataset no contiene variables derivadas del target.")
print("- Se mantiene separación correcta entre X e y.")
print("- Se realizó split train/test antes del feature engineering.")
print("- Dataset listo para transformación y entrenamiento.")