# =========================================================
# 02 - FEATURE ENGINEERING (MODELO V5)
# =========================================================
# Objetivo:
# Transformar train y test a formato numérico para entrenar
# modelos de regresión.
#
# Transformaciones:
# - periodo_antelacion -> antelacion_dias
# - tipo_dia -> tipo_dia_cod
# - tiene_valoraciones -> entero
# - mes -> One-Hot Encoding
# - categoria_alojamiento -> One-Hot Encoding
# - tipo_zona_turistica -> One-Hot Encoding
#
# Importante:
# - precio se mantiene como target.
# - No se usan IDs.
# - No se usan variables derivadas del precio.
# - Se utiliza drop_first=True para evitar multicolinealidad.
# =========================================================

import os
import pandas as pd
from pathlib import Path


# ---------------------------------------------------------
# 02.1 Rutas
# ---------------------------------------------------------

BASE_DIR = Path(__file__).resolve().parents[2]

ML_DIR = BASE_DIR / "data" / "Machine Learning" / "modelo_precios_v5"

train_path = os.path.join(ML_DIR, "train_v5.csv")
test_path = os.path.join(ML_DIR, "test_v5.csv")

output_train_path = os.path.join(ML_DIR, "train_features_v5.csv")
output_test_path = os.path.join(ML_DIR, "test_features_v5.csv")


# ---------------------------------------------------------
# 02.2 Cargar datasets
# ---------------------------------------------------------

print("Leyendo train desde:")
print(train_path)

train = pd.read_csv(train_path)

print("\nLeyendo test desde:")
print(test_path)

test = pd.read_csv(test_path)

print("\nTrain inicial:", train.shape)
print("Test inicial:", test.shape)


# ---------------------------------------------------------
# 02.3 Transformar periodo_antelacion a días
# ---------------------------------------------------------
# Esta transformación mantiene sentido ordinal real:
# mayor anticipación = más días antes de reservar.
# ---------------------------------------------------------

map_antelacion = {
    "1 semana": 7,
    "2 semanas": 14,
    "1 mes": 30,
    "2-3 meses": 75,
    "3 meses": 90
}

train["antelacion_dias"] = train["periodo_antelacion"].map(map_antelacion)
test["antelacion_dias"] = test["periodo_antelacion"].map(map_antelacion)


# ---------------------------------------------------------
# 02.4 Codificar tipo_dia
# ---------------------------------------------------------

map_tipo_dia = {
    "semana": 0,
    "fin_semana": 1
}

train["tipo_dia_cod"] = train["tipo_dia"].map(map_tipo_dia)
test["tipo_dia_cod"] = test["tipo_dia"].map(map_tipo_dia)


# ---------------------------------------------------------
# 02.5 Convertir booleanos a enteros
# ---------------------------------------------------------

train["tiene_valoraciones"] = train["tiene_valoraciones"].astype(int)
test["tiene_valoraciones"] = test["tiene_valoraciones"].astype(int)

train["gran_ciudad"] = train["gran_ciudad"].astype(int)
test["gran_ciudad"] = test["gran_ciudad"].astype(int)


# ---------------------------------------------------------
# 02.6 Tratar mes como categórico
# ---------------------------------------------------------
# Aunque mes es numérico, se trata como categoría porque:
# diciembre no es "más grande" que enero.
# ---------------------------------------------------------

train["mes"] = train["mes"].astype(str)
test["mes"] = test["mes"].astype(str)


# ---------------------------------------------------------
# 02.7 One-Hot Encoding
# ---------------------------------------------------------
# Se concatenan train y test únicamente para asegurar
# mismas columnas en ambos datasets.
#
# drop_first=True evita multicolinealidad perfecta.
# ---------------------------------------------------------

train["dataset"] = "train"
test["dataset"] = "test"

df_total = pd.concat(
    [train, test],
    axis=0,
    ignore_index=True
)

df_total = pd.get_dummies(
    df_total,
    columns=[
        "categoria_alojamiento",
        "mes",
        "tipo_zona_turistica"
    ],
    drop_first=True,
    dtype=int
)


# ---------------------------------------------------------
# 02.8 Separar nuevamente train y test
# ---------------------------------------------------------

train_features = df_total[
    df_total["dataset"] == "train"
].copy()

test_features = df_total[
    df_total["dataset"] == "test"
].copy()

train_features = train_features.drop(columns=["dataset"])
test_features = test_features.drop(columns=["dataset"])


# ---------------------------------------------------------
# 02.9 Eliminar columnas textuales transformadas
# ---------------------------------------------------------

columnas_eliminar = [
    "periodo_antelacion",
    "tipo_dia"
]

train_features = train_features.drop(
    columns=columnas_eliminar,
    errors="ignore"
)

test_features = test_features.drop(
    columns=columnas_eliminar,
    errors="ignore"
)


# ---------------------------------------------------------
# 02.10 Validaciones
# ---------------------------------------------------------

print("\n=== NULOS TRAIN ===")
print(train_features.isnull().sum())

print("\n=== NULOS TEST ===")
print(test_features.isnull().sum())

print("\nColumnas train:", train_features.shape[1])
print("Columnas test:", test_features.shape[1])

columnas_iguales = list(train_features.columns) == list(test_features.columns)

print("\n¿Train y test tienen las mismas columnas?")
print(columnas_iguales)

if not columnas_iguales:

    print("\nColumnas en train pero no en test:")
    print(set(train_features.columns) - set(test_features.columns))

    print("\nColumnas en test pero no en train:")
    print(set(test_features.columns) - set(train_features.columns))


# ---------------------------------------------------------
# 02.11 Guardar datasets con features
# ---------------------------------------------------------

train_features.to_csv(
    output_train_path,
    index=False,
    encoding="utf-8"
)

test_features.to_csv(
    output_test_path,
    index=False,
    encoding="utf-8"
)

print("\nTrain features guardado en:")
print(output_train_path)

print("\nTest features guardado en:")
print(output_test_path)


# ---------------------------------------------------------
# 02.12 Vista previa
# ---------------------------------------------------------

print("\n=== PRIMERAS 5 FILAS TRAIN FEATURES ===")
print(train_features.head())


# ---------------------------------------------------------
# 02.13 Variables finales del modelo
# ---------------------------------------------------------

print("\n=== VARIABLES FINALES DEL MODELO ===")

variables_modelo = [
    col for col in train_features.columns
    if col != "precio"
]

for var in variables_modelo:
    print(var)

print("\nCantidad total variables predictoras:")
print(len(variables_modelo))


# ---------------------------------------------------------
# 02.14 Conclusión
# ---------------------------------------------------------

print("\n=== CONCLUSIÓN PASO 02 V5 ===")

print("- Se transformó periodo_antelacion a antelacion_dias.")
print("- Se codificó tipo_dia como variable binaria.")
print("- Se aplicó One-Hot Encoding a mes.")
print("- Se aplicó One-Hot Encoding a categoria_alojamiento.")
print("- Se aplicó One-Hot Encoding a tipo_zona_turistica.")
print("- Se utilizó drop_first=True para evitar multicolinealidad.")
print("- gran_ciudad se mantiene como variable binaria.")
print("- precio se conserva como target, pero no será predictor.")
print("- Train y test quedan listos para modelado.")
