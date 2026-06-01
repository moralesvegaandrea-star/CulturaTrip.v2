# =========================================================
# 10 - MODELOS DE REGRESIÓN PARA ACTIVIDADES
# =========================================================
# Objetivo:
# Entrenar y comparar distintos modelos para predecir
# el precio esperado de actividades turísticas,
# utilizando la variable logarítmica del precio.
# =========================================================

import os
import numpy as np
import pandas as pd
from pathlib import Path

from sklearn.model_selection import train_test_split
from sklearn.linear_model import LinearRegression, Ridge
from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score


# ---------------------------------------------------------
# 10.1 Rutas
# ---------------------------------------------------------

BASE_DIR = Path(__file__).resolve().parents[2]

ML_DIR = BASE_DIR / "data" / "Machine Learning"
OUTPUTS_DIR = BASE_DIR / "outputs"

input_path = os.path.join(ML_DIR, "df_actividades_features.csv")
output_metricas = os.path.join(ML_DIR, "metricas_modelos_actividades.csv")


# ---------------------------------------------------------
# 10.2 Cargar dataset
# ---------------------------------------------------------

print("Leyendo dataset desde:")
print(input_path)

df = pd.read_csv(input_path)

print("\nDataset cargado correctamente.")
print("Dimensiones:", df.shape)


# ---------------------------------------------------------
# 10.3 Definir variables
# ---------------------------------------------------------

features = [
    "id_ccaa",
    "id_provincia",
    "mes",
    "temporada_cod",
    "categoria_cod",
    "subcategoria_cod",
    "valoracion_general_promedio",
    "valoracion_por_categoria_promedio",
    "log_opiniones"
]

X = df[features]
y = df["log_precio"]


# ---------------------------------------------------------
# 10.4 Train / Test split
# ---------------------------------------------------------

X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=42
)

print("\nTrain:", X_train.shape)
print("Test:", X_test.shape)


# ---------------------------------------------------------
# 10.5 Modelos a comparar
# ---------------------------------------------------------

modelos = {
    "LinearRegression": LinearRegression(),
    "Ridge": Ridge(alpha=1.0),
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
# 10.6 Entrenamiento y evaluación
# ---------------------------------------------------------

resultados = []

for nombre, modelo in modelos.items():
    print(f"\nEntrenando: {nombre}")

    modelo.fit(X_train, y_train)

    y_pred_log = modelo.predict(X_test)

    # 🔥 Convertimos a escala real
    y_pred = np.expm1(y_pred_log)
    y_real = np.expm1(y_test)

    mae = mean_absolute_error(y_real, y_pred)
    rmse = np.sqrt(mean_squared_error(y_real, y_pred))
    r2 = r2_score(y_real, y_pred)

    resultados.append({
        "modelo": nombre,
        "MAE": round(mae, 2),
        "RMSE": round(rmse, 2),
        "R2": round(r2, 4)
    })


# ---------------------------------------------------------
# 10.7 Resultados
# ---------------------------------------------------------

df_resultados = pd.DataFrame(resultados)
df_resultados = df_resultados.sort_values(by="R2", ascending=False)

print("\n=== RESULTADOS ===")
print(df_resultados)


# ---------------------------------------------------------
# 10.8 Guardar resultados
# ---------------------------------------------------------

df_resultados.to_csv(output_metricas, index=False)

print("\nMétricas guardadas en:")
print(output_metricas)