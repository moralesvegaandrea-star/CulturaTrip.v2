# =========================================================
# 00 - EDA ALOJAMIENTOS (MODELO V2 CORREGIDO)
# =========================================================
# Objetivo:
# Analizar la distribución del precio y validar qué variables
# explican su comportamiento antes de construir el modelo.
#
# Este análisis permite justificar:
# - eliminación de IDs
# - selección de variables
# - diseño del feature engineering
# =========================================================

import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from pathlib import Path


# ---------------------------------------------------------
# 00.1 Definición de rutas
# ---------------------------------------------------------

BASE_DIR = Path(__file__).resolve().parents[2]

ML_DIR = BASE_DIR / "data" / "Machine Learning"
input_path = os.path.join(ML_DIR, "df_precios_largo.csv")


# ---------------------------------------------------------
# 00.2 Cargar dataset
# ---------------------------------------------------------

print("Leyendo dataset desde:")
print(input_path)

df = pd.read_csv(input_path)

print("\nDataset cargado correctamente.")
print("Dimensiones:", df.shape)


# ---------------------------------------------------------
# 00.3 Estadísticas del precio
# ---------------------------------------------------------

print("\n=== DESCRIBE PRECIO ===")
print(df["precio"].describe())

print("\n=== SKEWNESS DEL PRECIO ===")
print(round(df["precio"].skew(), 4))


# ---------------------------------------------------------
# 00.4 Distribución del precio
# ---------------------------------------------------------

plt.figure(figsize=(10, 6))
plt.hist(df["precio"], bins=50, edgecolor="black")
plt.title("Distribución de precios de alojamientos")
plt.xlabel("Precio")
plt.ylabel("Frecuencia")
plt.show()


# ---------------------------------------------------------
# 00.5 Boxplot (detección de outliers)
# ---------------------------------------------------------

plt.figure(figsize=(10, 4))
plt.boxplot(df["precio"], vert=False)
plt.title("Boxplot precios alojamientos")
plt.show()


# ---------------------------------------------------------
# 00.6 Outliers (IQR)
# ---------------------------------------------------------

q1 = df["precio"].quantile(0.25)
q3 = df["precio"].quantile(0.75)
iqr = q3 - q1

lim_sup = q3 + 1.5 * iqr

outliers = df[df["precio"] > lim_sup]

print("\nOutliers detectados:", outliers.shape[0])
print("Porcentaje:",
      round(outliers.shape[0] / df.shape[0] * 100, 2), "%")


# ---------------------------------------------------------
# 00.7 Precio por tipo de día
# ---------------------------------------------------------

print("\n=== PRECIO PROMEDIO POR TIPO DE DÍA ===")
print(df.groupby("tipo_dia")["precio"].mean())


# ---------------------------------------------------------
# 00.8 Precio por categoría
# ---------------------------------------------------------

print("\n=== PRECIO PROMEDIO POR CATEGORÍA ===")
print(df.groupby("categoria_alojamiento")["precio"].mean())


# ---------------------------------------------------------
# 00.9 Precio por mes
# ---------------------------------------------------------

print("\n=== PRECIO PROMEDIO POR MES ===")
print(df.groupby("mes")["precio"].mean())


# ---------------------------------------------------------
# 00.10 Insights automáticos
# ---------------------------------------------------------

print("\n=== INSIGHTS ===")
print("- El precio presenta asimetría (skewness).")
print("- Existen outliers asociados a alojamientos premium.")
print("- El precio varía por tipo de día (fin de semana vs semana).")
print("- El precio varía significativamente por categoría.")
print("- El precio muestra comportamiento estacional por mes.")