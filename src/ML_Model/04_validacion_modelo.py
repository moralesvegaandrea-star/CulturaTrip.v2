# =========================================================
# 04 - VALIDACIÓN DEL MODELO FINAL (V5)
# =========================================================
# Objetivo:
# Validar si el modelo Random Forest V5 generaliza bien o
# si presenta señales de overfitting.
#
# Validaciones:
# - Comparación Train vs Test
# - Validación cruzada
# - Análisis de residuos
# - Error por rangos de precio
# =========================================================

import os
import pandas as pd
import matplotlib.pyplot as plt

from pathlib import Path

from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import cross_val_score, KFold
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score


# ---------------------------------------------------------
# 04.1 Rutas
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

output_validacion_path = os.path.join(
    ML_DIR,
    "validacion_modelo_v5.csv"
)

output_errores_path = os.path.join(
    ML_DIR,
    "errores_por_rango_precio_v5.csv"
)

grafico_real_pred_path = os.path.join(
    ML_DIR,
    "real_vs_predicho_v5.png"
)

grafico_residuos_path = os.path.join(
    ML_DIR,
    "residuos_v5.png"
)


# ---------------------------------------------------------
# 04.2 Cargar datasets
# ---------------------------------------------------------

print("Cargando train...")
train = pd.read_csv(train_path)

print("Cargando test...")
test = pd.read_csv(test_path)

print("\nTrain:", train.shape)
print("Test:", test.shape)


# ---------------------------------------------------------
# 04.3 Separar X e y
# ---------------------------------------------------------

X_train = train.drop(columns=["precio"])
y_train = train["precio"]

X_test = test.drop(columns=["precio"])
y_test = test["precio"]


# ---------------------------------------------------------
# 04.4 Entrenar Random Forest final
# ---------------------------------------------------------

modelo = RandomForestRegressor(
    n_estimators=100,
    random_state=42,
    n_jobs=-1
)

modelo.fit(X_train, y_train)


# ---------------------------------------------------------
# 04.5 Predicciones Train y Test
# ---------------------------------------------------------

pred_train = modelo.predict(X_train)
pred_test = modelo.predict(X_test)


def calcular_metricas(y_real, y_pred, nombre_dataset):

    mae = mean_absolute_error(y_real, y_pred)
    rmse = mean_squared_error(y_real, y_pred) ** 0.5
    r2 = r2_score(y_real, y_pred)

    return {
        "dataset": nombre_dataset,
        "MAE": round(mae, 2),
        "RMSE": round(rmse, 2),
        "R2": round(r2, 4)
    }


metricas_train = calcular_metricas(
    y_train,
    pred_train,
    "Train"
)

metricas_test = calcular_metricas(
    y_test,
    pred_test,
    "Test"
)

df_metricas = pd.DataFrame([
    metricas_train,
    metricas_test
])

print("\n=== MÉTRICAS TRAIN VS TEST ===")
print(df_metricas)


# ---------------------------------------------------------
# 04.6 Brecha de generalización
# ---------------------------------------------------------

r2_gap = metricas_train["R2"] - metricas_test["R2"]
mae_gap = metricas_test["MAE"] - metricas_train["MAE"]

print("\n=== BRECHA TRAIN VS TEST ===")
print("Diferencia R2 Train-Test:", round(r2_gap, 4))
print("Diferencia MAE Test-Train:", round(mae_gap, 2))


if r2_gap > 0.10:
    print("\nALERTA: Posible overfitting. La diferencia de R2 es alta.")
else:
    print("\nOK: La diferencia de R2 no parece excesiva.")

if mae_gap > 10:
    print("ALERTA: El error en test aumenta bastante respecto a train.")
else:
    print("OK: El error en test es razonablemente estable.")


# ---------------------------------------------------------
# 04.7 Validación cruzada sobre train
# ---------------------------------------------------------

print("\n=== VALIDACIÓN CRUZADA ===")

cv = KFold(
    n_splits=5,
    shuffle=True,
    random_state=42
)

cv_scores = cross_val_score(
    modelo,
    X_train,
    y_train,
    cv=cv,
    scoring="r2",
    n_jobs=-1
)

print("R2 por fold:")
print(cv_scores)

print("R2 CV promedio:", round(cv_scores.mean(), 4))
print("R2 CV desviación:", round(cv_scores.std(), 4))


# ---------------------------------------------------------
# 04.8 Guardar validación general
# ---------------------------------------------------------

df_metricas["R2_CV_promedio"] = round(cv_scores.mean(), 4)
df_metricas["R2_CV_std"] = round(cv_scores.std(), 4)
df_metricas["R2_gap_train_test"] = round(r2_gap, 4)
df_metricas["MAE_gap_test_train"] = round(mae_gap, 2)

df_metricas.to_csv(
    output_validacion_path,
    index=False,
    encoding="utf-8"
)

print("\nValidación guardada en:")
print(output_validacion_path)


# ---------------------------------------------------------
# 04.9 Gráfico Real vs Predicho
# ---------------------------------------------------------

plt.figure(figsize=(8, 6))
plt.scatter(y_test, pred_test, alpha=0.5)

min_val = min(y_test.min(), pred_test.min())
max_val = max(y_test.max(), pred_test.max())

plt.plot([min_val, max_val], [min_val, max_val], linestyle="--")

plt.title("Precio real vs precio predicho - V5")
plt.xlabel("Precio real")
plt.ylabel("Precio predicho")
plt.tight_layout()
plt.savefig(grafico_real_pred_path)
plt.show()

print("\nGráfico real vs predicho guardado en:")
print(grafico_real_pred_path)


# ---------------------------------------------------------
# 04.10 Análisis de residuos
# ---------------------------------------------------------

residuos = y_test - pred_test

plt.figure(figsize=(8, 6))
plt.scatter(pred_test, residuos, alpha=0.5)
plt.axhline(0, linestyle="--")

plt.title("Residuos del modelo - V5")
plt.xlabel("Precio predicho")
plt.ylabel("Error / Residuo")
plt.tight_layout()
plt.savefig(grafico_residuos_path)
plt.show()

print("\nGráfico residuos guardado en:")
print(grafico_residuos_path)


# ---------------------------------------------------------
# 04.11 Error por rango de precio
# ---------------------------------------------------------

df_error = pd.DataFrame({
    "precio_real": y_test,
    "precio_predicho": pred_test
})

df_error["error_abs"] = (
    df_error["precio_real"] - df_error["precio_predicho"]
).abs()

df_error["rango_precio"] = pd.cut(
    df_error["precio_real"],
    bins=[0, 100, 200, 300, 500, 1000, 2000],
    labels=[
        "0-100",
        "100-200",
        "200-300",
        "300-500",
        "500-1000",
        "1000+"
    ]
)

errores_rango = (
    df_error
    .groupby("rango_precio", observed=True)
    .agg(
        registros=("precio_real", "count"),
        precio_real_medio=("precio_real", "mean"),
        precio_predicho_medio=("precio_predicho", "mean"),
        MAE=("error_abs", "mean")
    )
    .reset_index()
)

errores_rango["precio_real_medio"] = errores_rango["precio_real_medio"].round(2)
errores_rango["precio_predicho_medio"] = errores_rango["precio_predicho_medio"].round(2)
errores_rango["MAE"] = errores_rango["MAE"].round(2)

print("\n=== ERROR POR RANGO DE PRECIO ===")
print(errores_rango)

errores_rango.to_csv(
    output_errores_path,
    index=False,
    encoding="utf-8"
)

print("\nErrores por rango guardados en:")
print(output_errores_path)


# ---------------------------------------------------------
# 04.12 Conclusión
# ---------------------------------------------------------

print("\n=== CONCLUSIÓN PASO 04 V5 ===")
print("- Se comparó desempeño en train y test.")
print("- Se calculó brecha de generalización.")
print("- Se aplicó validación cruzada.")
print("- Se analizaron residuos.")
print("- Se evaluó error por rango de precio.")
print("- Esta validación permite decidir si V5 es defendible como modelo final.")
