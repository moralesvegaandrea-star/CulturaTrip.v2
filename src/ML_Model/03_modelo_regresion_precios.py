# =========================================================
# 03 - MODELOS DE REGRESIÓN PARA PREDICCIÓN DE PRECIOS
# =========================================================
# Objetivo:
# Entrenar y comparar distintos modelos de regresión para
# predecir el precio del alojamiento a partir de variables
# turísticas y contextuales.
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

input_path = os.path.join(ML_DIR,"df_precios_features.csv")
output_dir = os.path.join(ML_DIR,"regresion_precios")
output_metricas = os.path.join(ML_DIR, "metricas_modelos_regresion.csv")
output_metricas_rf = os.path.join(ML_DIR, "metricas_random_forest_optimizado.csv")


# ---------------------------------------------------------
# 03.2 Cargar dataset con features
# ---------------------------------------------------------

print("Leyendo dataset desde:")
print(input_path)

df = pd.read_csv(input_path)

print("\nDataset cargado correctamente.")
print("Dimensiones:", df.shape)


# ---------------------------------------------------------
# 03.3 Definir variable objetivo y variables predictoras
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
# 03.4 Dividir dataset en entrenamiento y prueba
# ---------------------------------------------------------

X_train, X_test, y_train, y_test = train_test_split(
    X,
    y,
    test_size=0.2,
    random_state=42
)

print("\nTamaño entrenamiento:", X_train.shape)
print("Tamaño prueba:", X_test.shape)


# ---------------------------------------------------------
# 03.5 Definir modelos base a comparar
# ---------------------------------------------------------

modelos = {
    "LinearRegression": LinearRegression(),
    "RandomForestRegressor": RandomForestRegressor(
        n_estimators=200,
        random_state=42,
        n_jobs=-1
    ),
    "GradientBoostingRegressor": GradientBoostingRegressor(
        n_estimators=200,
        random_state=42
    )
}


# ---------------------------------------------------------
# 03.6 Entrenar y evaluar cada modelo base
# ---------------------------------------------------------

resultados = []

for nombre_modelo, modelo in modelos.items():
    print(f"\nEntrenando modelo: {nombre_modelo}")

    modelo.fit(X_train, y_train)
    y_pred = modelo.predict(X_test)

    mae = mean_absolute_error(y_test, y_pred)
    rmse = mean_squared_error(y_test, y_pred) ** 0.5
    r2 = r2_score(y_test, y_pred)

    resultados.append({
        "modelo": nombre_modelo,
        "MAE": round(mae, 4),
        "RMSE": round(rmse, 4),
        "R2": round(r2, 4)
    })


# ---------------------------------------------------------
# 03.7 Crear tabla de resultados de modelos base
# ---------------------------------------------------------

df_resultados = pd.DataFrame(resultados)
df_resultados = df_resultados.sort_values(by="R2", ascending=False)

print("\n=== RESULTADOS DE LOS MODELOS BASE ===")
print(df_resultados)


# ---------------------------------------------------------
# 03.8 Guardar métricas de comparación de modelos base
# ---------------------------------------------------------

df_resultados.to_csv(output_metricas, index=False, encoding="utf-8")

print("\nMétricas de modelos base guardadas en:")
print(output_metricas)


# ---------------------------------------------------------
# 03.9 Seleccionar mejor modelo base
# ---------------------------------------------------------

mejor_modelo_nombre = df_resultados.iloc[0]["modelo"]
print("\nMejor modelo base seleccionado:", mejor_modelo_nombre)


# ---------------------------------------------------------
# 03.10 Optimización de hiperparámetros del Random Forest
# ---------------------------------------------------------
# Se optimiza únicamente Random Forest por haber sido el
# mejor modelo base en la comparación inicial.
# ---------------------------------------------------------

print("\n=== OPTIMIZACIÓN DE HIPERPARÁMETROS: RANDOM FOREST ===")

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

print("\nMejores hiperparámetros encontrados:")
print(grid_search.best_params_)

print("\nMejor R2 en validación cruzada:")
print(round(grid_search.best_score_, 4))


# ---------------------------------------------------------
# 03.11 Evaluar modelo Random Forest optimizado
# ---------------------------------------------------------

y_pred_rf = best_rf.predict(X_test)

mae_rf = mean_absolute_error(y_test, y_pred_rf)
rmse_rf = mean_squared_error(y_test, y_pred_rf) ** 0.5
r2_rf = r2_score(y_test, y_pred_rf)

print("\n=== RESULTADOS RANDOM FOREST OPTIMIZADO ===")
print("MAE:", round(mae_rf, 4))
print("RMSE:", round(rmse_rf, 4))
print("R2:", round(r2_rf, 4))


# ---------------------------------------------------------
# 03.12 Guardar métricas del modelo optimizado
# ---------------------------------------------------------

df_resultado_rf = pd.DataFrame([{
    "modelo": "RandomForestRegressor_Optimizado",
    "MAE": round(mae_rf, 4),
    "RMSE": round(rmse_rf, 4),
    "R2": round(r2_rf, 4),
    "best_params": str(grid_search.best_params_),
    "best_cv_r2": round(grid_search.best_score_, 4)
}])

df_resultado_rf.to_csv(output_metricas_rf, index=False, encoding="utf-8")

print("\nMétricas del Random Forest optimizado guardadas en:")
print(output_metricas_rf)


# ---------------------------------------------------------
# 03.13 Importancia de variables del modelo optimizado
# ---------------------------------------------------------

importancias = pd.DataFrame({
    "variable": features,
    "importancia": best_rf.feature_importances_
}).sort_values(by="importancia", ascending=False)

print("\n=== IMPORTANCIA DE VARIABLES DEL RANDOM FOREST OPTIMIZADO ===")
print(importancias)