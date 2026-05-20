# =========================================================
# 03 - MODELO DE REGRESIÓN (MODELO V2 CORREGIDO)
# =========================================================
# Objetivo:
# Entrenar y evaluar modelos de regresión para predecir
# el precio del alojamiento utilizando variables limpias.
#
# Decisiones metodológicas:
# - No se utilizan IDs como variables predictoras.
# - Se comparan múltiples modelos.
# - Se evalúa capacidad de generalización.
# =========================================================

import os
import re
import time
import unicodedata
from datetime import date, timedelta
import requests
import pandas as pd
from pathlib import Path

from sklearn.model_selection import train_test_split, GridSearchCV
from sklearn.linear_model import LinearRegression
from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score


# ---------------------------------------------------------
# 03.1 Definición de rutas
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

input_path = os.path.join(ML_DIR,"df_precios_features_v2.csv")

# ---------------------------------------------------------
# 03.2 Cargar dataset
# ---------------------------------------------------------

df = pd.read_csv(input_path)

print("Dataset cargado:", df.shape)


# ---------------------------------------------------------
# 03.3 Definir X e y
# ---------------------------------------------------------

X = df.drop(columns=["precio"])
y = df["precio"]

print("\nNúmero de variables:", X.shape[1])


# ---------------------------------------------------------
# 03.4 Train / Test Split
# ---------------------------------------------------------

X_train, X_test, y_train, y_test = train_test_split(
    X,
    y,
    test_size=0.2,
    random_state=42
)

print("\nTrain:", X_train.shape)
print("Test:", X_test.shape)


# ---------------------------------------------------------
# 03.5 Modelos a comparar
# ---------------------------------------------------------

modelos = {
    "LinearRegression": LinearRegression(),
    "RandomForest": RandomForestRegressor(
        n_estimators=200,
        random_state=42,
        n_jobs=-1
    ),
    "GradientBoosting": GradientBoostingRegressor(
        n_estimators=200,
        random_state=42
    )
}


# ---------------------------------------------------------
# 03.6 Entrenamiento y evaluación
# ---------------------------------------------------------

resultados = []

for nombre, modelo in modelos.items():
    print(f"\nEntrenando: {nombre}")

    modelo.fit(X_train, y_train)
    y_pred = modelo.predict(X_test)

    mae = mean_absolute_error(y_test, y_pred)
    rmse = mean_squared_error(y_test, y_pred) ** 0.5
    r2 = r2_score(y_test, y_pred)

    resultados.append({
        "modelo": nombre,
        "MAE": round(mae, 2),
        "RMSE": round(rmse, 2),
        "R2": round(r2, 4)
    })


# ---------------------------------------------------------
# 03.7 Resultados
# ---------------------------------------------------------

df_resultados = pd.DataFrame(resultados).sort_values(by="R2", ascending=False)

print("\n=== RESULTADOS ===")
print(df_resultados)


# ---------------------------------------------------------
# 03.8 Mejor modelo
# ---------------------------------------------------------

mejor_modelo_nombre = df_resultados.iloc[0]["modelo"]

print("\nMejor modelo:", mejor_modelo_nombre)


# ---------------------------------------------------------
# 03.9 Importancia de variables (solo RF)
# ---------------------------------------------------------

rf = RandomForestRegressor(
    n_estimators=200,
    random_state=42,
    n_jobs=-1
)

rf.fit(X_train, y_train)

importancias = pd.DataFrame({
    "variable": X.columns,
    "importancia": rf.feature_importances_
}).sort_values(by="importancia", ascending=False)

print("\n=== IMPORTANCIA DE VARIABLES ===")
print(importancias.head(10))

# =========================================================
# 03.10 OPTIMIZACIÓN DE HIPERPARÁMETROS - RANDOM FOREST
# =========================================================

from sklearn.model_selection import GridSearchCV

print("\n=== OPTIMIZACIÓN DE HIPERPARÁMETROS ===")

param_grid = {
    "n_estimators": [100, 200],
    "max_depth": [10, 20, None],
    "min_samples_split": [2, 5],
    "min_samples_leaf": [1, 2]
}

rf = RandomForestRegressor(
    random_state=42,
    n_jobs=-1
)

grid_search = GridSearchCV(
    estimator=rf,
    param_grid=param_grid,
    cv=5,
    scoring="r2",
    n_jobs=-1,
    verbose=1
)

grid_search.fit(X_train, y_train)

best_rf = grid_search.best_estimator_

print("\nMejores hiperparámetros:")
print(grid_search.best_params_)

print("\nMejor R2 en validación cruzada:")
print(round(grid_search.best_score_, 4))


# ---------------------------------------------------------
# Evaluación final del modelo optimizado
# ---------------------------------------------------------

y_pred_rf = best_rf.predict(X_test)

mae_rf = mean_absolute_error(y_test, y_pred_rf)
rmse_rf = mean_squared_error(y_test, y_pred_rf) ** 0.5
r2_rf = r2_score(y_test, y_pred_rf)

print("\n=== RESULTADOS RANDOM FOREST OPTIMIZADO ===")
print("MAE:", round(mae_rf, 2))
print("RMSE:", round(rmse_rf, 2))
print("R2:", round(r2_rf, 4))