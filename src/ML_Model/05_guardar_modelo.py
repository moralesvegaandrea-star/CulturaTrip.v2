# =========================================================
# 05 - GUARDAR MODELO FINAL (V5)
# =========================================================
# Objetivo:
# Entrenar el modelo Random Forest final V5 y guardar:
# - modelo entrenado
# - lista de variables predictoras
# - métricas finales
# - importancia de variables
#
# Este modelo será el candidato para producción / Streamlit.
# =========================================================

import os
import joblib
import pandas as pd

from pathlib import Path

from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score


# ---------------------------------------------------------
# 05.1 Rutas
# ---------------------------------------------------------

BASE_DIR = Path(__file__).resolve().parents[2]

ML_DIR = BASE_DIR / "data" / "Machine Learning" / "modelo_precios_v5"

train_path = os.path.join(ML_DIR, "train_features_v5.csv")
test_path = os.path.join(ML_DIR, "test_features_v5.csv")

modelo_path = os.path.join(ML_DIR, "random_forest_precio_v5.pkl")
features_path = os.path.join(ML_DIR, "features_modelo_precio_v5.pkl")
metricas_path = os.path.join(ML_DIR, "metricas_modelo_final_v5.csv")
importancias_path = os.path.join(ML_DIR, "importancia_variables_modelo_final_v5.csv")


# ---------------------------------------------------------
# 05.2 Cargar datasets
# ---------------------------------------------------------

print("Leyendo train desde:")
print(train_path)

train = pd.read_csv(train_path)

print("\nLeyendo test desde:")
print(test_path)

test = pd.read_csv(test_path)

print("\nTrain:", train.shape)
print("Test:", test.shape)


# ---------------------------------------------------------
# 05.3 Separar X e y
# ---------------------------------------------------------

X_train = train.drop(columns=["precio"])
y_train = train["precio"]

X_test = test.drop(columns=["precio"])
y_test = test["precio"]

features = X_train.columns.tolist()

print("\nVariables finales del modelo:")
print(features)

print("\nCantidad de variables predictoras:")
print(len(features))


# ---------------------------------------------------------
# 05.4 Entrenar modelo final
# ---------------------------------------------------------

modelo = RandomForestRegressor(
    n_estimators=100,
    random_state=42,
    n_jobs=-1
)

modelo.fit(X_train, y_train)


# ---------------------------------------------------------
# 05.5 Evaluar modelo final
# ---------------------------------------------------------

pred_test = modelo.predict(X_test)

mae = mean_absolute_error(y_test, pred_test)
rmse = mean_squared_error(y_test, pred_test) ** 0.5
r2 = r2_score(y_test, pred_test)

print("\n=== MÉTRICAS MODELO FINAL V5 ===")
print("MAE:", round(mae, 2))
print("RMSE:", round(rmse, 2))
print("R2:", round(r2, 4))


# ---------------------------------------------------------
# 05.6 Guardar métricas
# ---------------------------------------------------------

df_metricas = pd.DataFrame([{
    "modelo": "RandomForestRegressor_V5",
    "MAE": round(mae, 2),
    "RMSE": round(rmse, 2),
    "R2_holdout": round(r2, 4),
    "observacion": "Modelo V5 con variables geográficas interpretables, sin IDs ni variables derivadas del target."
}])

df_metricas.to_csv(metricas_path, index=False, encoding="utf-8")

print("\nMétricas guardadas en:")
print(metricas_path)


# ---------------------------------------------------------
# 05.7 Guardar importancia de variables
# ---------------------------------------------------------

df_importancias = pd.DataFrame({
    "variable": features,
    "importancia": modelo.feature_importances_
}).sort_values(by="importancia", ascending=False)

df_importancias.to_csv(importancias_path, index=False, encoding="utf-8")

print("\nImportancia de variables guardada en:")
print(importancias_path)

print("\n=== TOP 15 VARIABLES ===")
print(df_importancias.head(15))


# ---------------------------------------------------------
# 05.8 Guardar modelo y features
# ---------------------------------------------------------

joblib.dump(modelo, modelo_path)
joblib.dump(features, features_path)

print("\nModelo guardado en:")
print(modelo_path)

print("\nFeatures guardadas en:")
print(features_path)


# ---------------------------------------------------------
# 05.9 Conclusión
# ---------------------------------------------------------

print("\n=== CONCLUSIÓN PASO 05 V5 ===")
print("- Se entrenó el modelo final Random Forest V5.")
print("- Se guardó el modelo en formato .pkl.")
print("- Se guardó la lista exacta de variables predictoras.")
print("- Se guardaron métricas finales del modelo.")
print("- Se guardó la importancia de variables.")
print("- El modelo queda listo para ser usado en simulación o Streamlit.")