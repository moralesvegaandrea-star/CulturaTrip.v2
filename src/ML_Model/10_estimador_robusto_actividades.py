# =========================================================
# 10 - ESTIMADOR ROBUSTO DE ACTIVIDADES
# =========================================================
# Objetivo:
# Construir una alternativa robusta al modelo supervisado
# para estimar el precio esperado de actividades turísticas,
# utilizando estadísticas agregadas por contexto.
#
# En lugar de predecir el precio mediante regresión, este
# enfoque estima el coste unitario esperado a partir de la
# mediana del precio por provincia, mes y categoría, con
# reglas de fallback para contextos con menor cobertura.
# =========================================================

import os
import numpy as np
import pandas as pd
from pathlib import Path
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score


# ---------------------------------------------------------
# 10.1 Definición de rutas
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
output_metricas = os.path.join(ML_DIR, "metricas_estimador_robusto_actividades.csv")
output_tabla_contexto = os.path.join(ML_DIR, "tabla_contexto_robusto_actividades.csv")


# ---------------------------------------------------------
# 10.2 Cargar dataset base
# ---------------------------------------------------------

print("Leyendo dataset desde:")
print(input_path)

df = pd.read_csv(input_path)

print("\nDataset cargado correctamente.")
print("Dimensiones:", df.shape)


# ---------------------------------------------------------
# 10.3 Definir variable objetivo
# ---------------------------------------------------------

target = "precio_medio_entrada_promedio"


# ---------------------------------------------------------
# 10.4 Train / Test split
# ---------------------------------------------------------
# Separamos antes de construir tablas agregadas para evitar
# contaminar la evaluación con información del conjunto test.

train_df, test_df = train_test_split(
    df,
    test_size=0.2,
    random_state=42
)

print("\nTrain:", train_df.shape)
print("Test:", test_df.shape)


# ---------------------------------------------------------
# 10.5 Construcción de tablas robustas por niveles
# ---------------------------------------------------------
# Nivel 1: provincia + mes + categoria
# Nivel 2: provincia + categoria
# Nivel 3: categoria
# Nivel 4: mediana global

tabla_nivel_1 = (
    train_df.groupby(["id_provincia", "mes", "categoria"])[target]
    .median()
    .reset_index()
    .rename(columns={target: "precio_mediano_n1"})
)

tabla_nivel_2 = (
    train_df.groupby(["id_provincia", "categoria"])[target]
    .median()
    .reset_index()
    .rename(columns={target: "precio_mediano_n2"})
)

tabla_nivel_3 = (
    train_df.groupby(["categoria"])[target]
    .median()
    .reset_index()
    .rename(columns={target: "precio_mediano_n3"})
)

mediana_global = train_df[target].median()

print("\nMediana global del train:", round(mediana_global, 2))


# ---------------------------------------------------------
# 10.6 Guardar tabla principal de contexto
# ---------------------------------------------------------

tabla_nivel_1.to_csv(output_tabla_contexto, index=False, encoding="utf-8")

print("\nTabla de contexto robusto guardada en:")
print(output_tabla_contexto)


# ---------------------------------------------------------
# 10.7 Función de estimación robusta con fallback
# ---------------------------------------------------------

def estimar_precio_robusto(fila):
    id_provincia = fila["id_provincia"]
    mes = fila["mes"]
    categoria = fila["categoria"]

    # Nivel 1: provincia + mes + categoria
    match_n1 = tabla_nivel_1[
        (tabla_nivel_1["id_provincia"] == id_provincia) &
        (tabla_nivel_1["mes"] == mes) &
        (tabla_nivel_1["categoria"] == categoria)
    ]

    if not match_n1.empty:
        return match_n1.iloc[0]["precio_mediano_n1"], "nivel_1"

    # Nivel 2: provincia + categoria
    match_n2 = tabla_nivel_2[
        (tabla_nivel_2["id_provincia"] == id_provincia) &
        (tabla_nivel_2["categoria"] == categoria)
    ]

    if not match_n2.empty:
        return match_n2.iloc[0]["precio_mediano_n2"], "nivel_2"

    # Nivel 3: categoria
    match_n3 = tabla_nivel_3[
        (tabla_nivel_3["categoria"] == categoria)
    ]

    if not match_n3.empty:
        return match_n3.iloc[0]["precio_mediano_n3"], "nivel_3"

    # Nivel 4: fallback global
    return mediana_global, "global"


# ---------------------------------------------------------
# 10.8 Aplicar estimación al conjunto de prueba
# ---------------------------------------------------------

predicciones = test_df.apply(estimar_precio_robusto, axis=1)

test_df["precio_estimado"] = [x[0] for x in predicciones]
test_df["nivel_fallback"] = [x[1] for x in predicciones]


# ---------------------------------------------------------
# 10.9 Evaluación del estimador robusto
# ---------------------------------------------------------

y_true = test_df[target]
y_pred = test_df["precio_estimado"]

mae = mean_absolute_error(y_true, y_pred)
rmse = np.sqrt(mean_squared_error(y_true, y_pred))
r2 = r2_score(y_true, y_pred)

print("\n=== RESULTADOS DEL ESTIMADOR ROBUSTO ===")
print("MAE:", round(mae, 2))
print("RMSE:", round(rmse, 2))
print("R2:", round(r2, 4))


# ---------------------------------------------------------
# 10.10 Distribución de niveles de fallback
# ---------------------------------------------------------

print("\n=== USO DE NIVELES DE FALLBACK ===")
print(test_df["nivel_fallback"].value_counts())


# ---------------------------------------------------------
# 10.11 Guardar métricas
# ---------------------------------------------------------

df_resultado = pd.DataFrame([{
    "modelo": "EstimadorRobustoMediana",
    "MAE": round(mae, 2),
    "RMSE": round(rmse, 2),
    "R2": round(r2, 4),
    "mediana_global_train": round(mediana_global, 2)
}])

df_resultado.to_csv(output_metricas, index=False, encoding="utf-8")

print("\nMétricas guardadas en:")
print(output_metricas)


# ---------------------------------------------------------
# 10.12 Vista previa de predicciones
# ---------------------------------------------------------

print("\n=== EJEMPLO DE PREDICCIONES ===")
print(
    test_df[
        [
            "id_provincia",
            "mes",
            "categoria",
            target,
            "precio_estimado",
            "nivel_fallback"
        ]
    ].head(10)
)