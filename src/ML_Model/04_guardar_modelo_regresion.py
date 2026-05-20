# =========================================================
# 04 - GUARDAR MODELO DE REGRESIÓN (MODELO V2 CORREGIDO)
# =========================================================
# Objetivo:
# Entrenar el modelo final seleccionado y guardar los archivos
# necesarios para su uso posterior en el prototipo.
#
# Decisión metodológica:
# - Se guarda Random Forest base, ya que obtuvo mejor desempeño
#   que el modelo optimizado en el conjunto de prueba.
# - Se guardan también las features usadas para garantizar
#   consistencia en futuras predicciones.
# =========================================================

import os
import pickle
import pandas as pd
from pathlib import Path

from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score

# ---------------------------------------------------------
# 04.1 Definición de rutas
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


input_path =  os.path.join(ML_DIR,"df_precios_features_v2.csv")
model_dir = BASE_DIR / "outputs" / "regresion_precios" / "modelos"
model_path = model_dir / "random_forest_precio_v2.pkl"
features_path = model_dir / "features_modelo_precio_v2.pkl"
metricas_rf_path = os.path.join(ML_DIR, "metricas_modelo_precio_v2.csv")

model_dir.mkdir(parents=True, exist_ok=True)

# ---------------------------------------------------------
# 04.2 Cargar dataset
# ---------------------------------------------------------

print("Leyendo dataset desde:")
print(input_path)

df = pd.read_csv(input_path)

print("\nDataset cargado correctamente.")
print("Dimensiones:", df.shape)


# ---------------------------------------------------------
# 04.3 Separar variables predictoras y variable objetivo
# ---------------------------------------------------------

X = df.drop(columns=["precio"])
y = df["precio"]

features = X.columns.tolist()

print("\nVariables del modelo:")
print(features)


# ---------------------------------------------------------
# 04.4 Train / Test Split
# ---------------------------------------------------------

X_train, X_test, y_train, y_test = train_test_split(
    X,
    y,
    test_size=0.2,
    random_state=42
)


# ---------------------------------------------------------
# 04.5 Entrenar modelo final
# ---------------------------------------------------------

modelo_final = RandomForestRegressor(
    n_estimators=200,
    random_state=42,
    n_jobs=-1
)

modelo_final.fit(X_train, y_train)


# ---------------------------------------------------------
# 04.6 Evaluar modelo final
# ---------------------------------------------------------

y_pred = modelo_final.predict(X_test)

mae = mean_absolute_error(y_test, y_pred)
rmse = mean_squared_error(y_test, y_pred) ** 0.5
r2 = r2_score(y_test, y_pred)

print("\n=== MÉTRICAS MODELO FINAL ===")
print("MAE:", round(mae, 2))
print("RMSE:", round(rmse, 2))
print("R2:", round(r2, 4))


# ---------------------------------------------------------
# 04.7 Guardar métricas
# ---------------------------------------------------------

df_metricas = pd.DataFrame([{
    "modelo": "RandomForestRegressor_base_v2",
    "MAE": round(mae, 2),
    "RMSE": round(rmse, 2),
    "R2": round(r2, 4),
    "n_estimators": 200,
    "random_state": 42,
    "usa_ids": False
}])

df_metricas.to_csv(metricas_rf_path, index=False, encoding="utf-8")


# ---------------------------------------------------------
# 04.8 Guardar modelo
# ---------------------------------------------------------

with open(model_path, "wb") as file:
    pickle.dump(modelo_final, file)

print("\nModelo guardado en:")
print(model_path)


# ---------------------------------------------------------
# 04.9 Guardar lista de features
# ---------------------------------------------------------

with open(features_path, "wb") as file:
    pickle.dump(features, file)

print("\nFeatures guardadas en:")
print(features_path)


# ---------------------------------------------------------
# 04.10 Conclusión del paso
# ---------------------------------------------------------

print("\n=== CONCLUSIÓN PASO 04 ===")
print("- Se entrenó y guardó el modelo Random Forest base.")
print("- Se guardó la lista de variables predictoras.")
print("- Se guardaron las métricas finales del modelo.")
print("- El modelo queda listo para ser usado en simulaciones.")