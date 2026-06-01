# =========================================================
# 03 - MODELO REGRESIÓN (V5)
# =========================================================
# Objetivo:
# Entrenar y comparar modelos de regresión utilizando
# variables geográficas interpretables.
#
# Modelos:
# - Linear Regression
# - Random Forest
# - Gradient Boosting
#
# Métricas:
# - MAE
# - RMSE
# - R2
#
# Además:
# - tabla comparativa
# - gráfico comparativo
# - importancia de variables
# =========================================================

import os
import pandas as pd
import matplotlib.pyplot as plt

from pathlib import Path

from sklearn.linear_model import LinearRegression
from sklearn.ensemble import RandomForestRegressor
from sklearn.ensemble import GradientBoostingRegressor

from sklearn.metrics import (
    mean_absolute_error,
    mean_squared_error,
    r2_score
)


# ---------------------------------------------------------
# 03.1 Rutas
# ---------------------------------------------------------

BASE_DIR = Path(__file__).resolve().parents[2]

ML_DIR = (
    BASE_DIR
    / "data"
    / "Machine Learning"
    / "modelo_precios_v5"
)

train_path = os.path.join(ML_DIR, "train_features_v5.csv")
test_path = os.path.join(ML_DIR, "test_features_v5.csv")

metricas_path = os.path.join(ML_DIR, "metricas_modelos_v5.csv")
grafico_path = os.path.join(ML_DIR, "comparacion_modelos_v5.png")

importancia_path = os.path.join(
    ML_DIR,
    "importancia_variables_v5.csv"
)

grafico_importancia_path = os.path.join(
    ML_DIR,
    "importancia_variables_v5.png"
)


# ---------------------------------------------------------
# 03.2 Cargar datasets
# ---------------------------------------------------------

print("Cargando train...")
train = pd.read_csv(train_path)

print("Cargando test...")
test = pd.read_csv(test_path)

print("\nTrain:", train.shape)
print("Test:", test.shape)


# ---------------------------------------------------------
# 03.3 Separar X e y
# ---------------------------------------------------------

X_train = train.drop(columns=["precio"])
y_train = train["precio"]

X_test = test.drop(columns=["precio"])
y_test = test["precio"]

print("\nNúmero de variables predictoras:")
print(X_train.shape[1])

print("\nVariables predictoras:")
print(X_train.columns.tolist())


# ---------------------------------------------------------
# 03.4 Modelos
# ---------------------------------------------------------

modelos = {

    "LinearRegression": LinearRegression(),

    "RandomForest": RandomForestRegressor(
        n_estimators=100,
        random_state=42
    ),

    "GradientBoosting": GradientBoostingRegressor(
        random_state=42
    )
}


# ---------------------------------------------------------
# 03.5 Entrenamiento y evaluación
# ---------------------------------------------------------

resultados = []

mejor_modelo = None
mejor_r2 = -999

for nombre, modelo in modelos.items():

    print(f"\nEntrenando modelo: {nombre}")

    modelo.fit(X_train, y_train)

    predicciones = modelo.predict(X_test)

    mae = mean_absolute_error(y_test, predicciones)

    rmse = mean_squared_error(
        y_test,
        predicciones
    ) ** 0.5

    r2 = r2_score(y_test, predicciones)

    resultados.append({
        "modelo": nombre,
        "MAE": round(mae, 2),
        "RMSE": round(rmse, 2),
        "R2": round(r2, 4)
    })

    if r2 > mejor_r2:
        mejor_r2 = r2
        mejor_modelo = modelo


# ---------------------------------------------------------
# 03.6 Tabla resultados
# ---------------------------------------------------------

df_resultados = pd.DataFrame(resultados)

df_resultados = df_resultados.sort_values(
    by="R2",
    ascending=False
)

print("\n=== RESULTADOS MODELOS V5 ===")
print(df_resultados)

df_resultados.to_csv(
    metricas_path,
    index=False,
    encoding="utf-8"
)

print("\nTabla comparativa guardada en:")
print(metricas_path)


# ---------------------------------------------------------
# 03.7 Gráfico comparativo
# ---------------------------------------------------------

fig, ax = plt.subplots(figsize=(10, 6))

x = range(len(df_resultados))

ax.bar(
    [i - 0.25 for i in x],
    df_resultados["MAE"],
    width=0.25,
    label="MAE"
)

ax.bar(
    x,
    df_resultados["RMSE"],
    width=0.25,
    label="RMSE"
)

ax.bar(
    [i + 0.25 for i in x],
    df_resultados["R2"] * 100,
    width=0.25,
    label="R2 x100"
)

ax.set_xticks(list(x))
ax.set_xticklabels(df_resultados["modelo"])

ax.set_title("Comparación de Modelos V5")

ax.legend()

plt.tight_layout()

plt.savefig(grafico_path)

print("\nGráfico comparativo guardado en:")
print(grafico_path)


# ---------------------------------------------------------
# 03.8 Importancia variables
# ---------------------------------------------------------

if hasattr(mejor_modelo, "feature_importances_"):

    importancia = pd.DataFrame({

        "variable": X_train.columns,
        "importancia": mejor_modelo.feature_importances_
    })

    importancia = importancia.sort_values(
        by="importancia",
        ascending=False
    )

    print("\n=== IMPORTANCIA VARIABLES ===")
    print(importancia.head(20))

    importancia.to_csv(
        importancia_path,
        index=False,
        encoding="utf-8"
    )

    print("\nTabla importancia guardada en:")
    print(importancia_path)

    # gráfico

    top15 = importancia.head(15)

    plt.figure(figsize=(12, 8))

    plt.barh(
        top15["variable"],
        top15["importancia"]
    )

    plt.gca().invert_yaxis()

    plt.title("Top 15 Variables Más Importantes - V5")

    plt.tight_layout()

    plt.savefig(grafico_importancia_path)

    print("\nGráfico importancia guardado en:")
    print(grafico_importancia_path)


# ---------------------------------------------------------
# 03.9 Conclusión
# ---------------------------------------------------------

print("\n=== CONCLUSIÓN PASO 03 V5 ===")

print("- Se entrenaron múltiples modelos de regresión.")
print("- Se evaluaron usando MAE, RMSE y R2.")
print("- Se incorporaron variables geográficas interpretables.")
print("- No se utilizaron IDs ni variables derivadas del precio.")
print("- Se generaron tablas y gráficos comparativos.")
print("- Se analizaron las variables más importantes.")
