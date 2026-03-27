# =========================================================
# 08 - ANÁLISIS EXPLORATORIO DE ACTIVIDADES
# =========================================================
# Objetivo:
# Analizar la distribución de la variable objetivo
# precio_medio_entrada_promedio, detectar outliers,
# revisar el comportamiento por categoría y provincia,
# y evaluar si conviene aplicar transformación logarítmica
# antes del modelado.
# =========================================================

import os
from pathlib import Path

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt


# ---------------------------------------------------------
# 08.1 Definición de rutas
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
output_dir = os.path.join(OUTPUTS_DIR, "eda_actividades")
os.makedirs(output_dir, exist_ok=True)


# ---------------------------------------------------------
# 08.2 Cargar dataset base
# ---------------------------------------------------------

print("Leyendo dataset desde:")
print(input_path)

df = pd.read_csv(input_path)

print("\nDataset cargado correctamente.")
print("Dimensiones:", df.shape)


# ---------------------------------------------------------
# 08.3 Estadísticas descriptivas del target
# ---------------------------------------------------------

target = "precio_medio_entrada_promedio"

print(f"\n=== DESCRIBE DE {target} ===")
print(df[target].describe())

print("\n=== SKEWNESS DEL TARGET ===")
print(round(df[target].skew(), 4))

print("\n=== CUANTILES DEL TARGET ===")
print(df[target].quantile([0.01, 0.05, 0.25, 0.50, 0.75, 0.90, 0.95, 0.99]))


# ---------------------------------------------------------
# 08.4 Histograma del target original
# ---------------------------------------------------------

plt.figure(figsize=(10, 6))
plt.hist(df[target], bins=50, edgecolor="black")
plt.title("Distribución de precio_medio_entrada_promedio")
plt.xlabel("Precio")
plt.ylabel("Frecuencia")
plt.tight_layout()
plt.savefig(os.path.join(output_dir, "01_histograma_precio_original.png"))
plt.show()


# ---------------------------------------------------------
# 08.5 Boxplot del target original
# ---------------------------------------------------------

plt.figure(figsize=(10, 4))
plt.boxplot(df[target], vert=False)
plt.title("Boxplot de precio_medio_entrada_promedio")
plt.xlabel("Precio")
plt.tight_layout()
plt.savefig(os.path.join(output_dir, "02_boxplot_precio_original.png"))
plt.show()


# ---------------------------------------------------------
# 08.6 Detección básica de outliers con IQR
# ---------------------------------------------------------

q1 = df[target].quantile(0.25)
q3 = df[target].quantile(0.75)
iqr = q3 - q1

limite_inferior = q1 - 1.5 * iqr
limite_superior = q3 + 1.5 * iqr

outliers = df[(df[target] < limite_inferior) | (df[target] > limite_superior)]

print("\n=== DETECCIÓN DE OUTLIERS (IQR) ===")
print("Q1:", round(q1, 2))
print("Q3:", round(q3, 2))
print("IQR:", round(iqr, 2))
print("Límite inferior:", round(limite_inferior, 2))
print("Límite superior:", round(limite_superior, 2))
print("Cantidad de outliers:", outliers.shape[0])
print("Porcentaje de outliers:", round((outliers.shape[0] / df.shape[0]) * 100, 2), "%")


# ---------------------------------------------------------
# 08.7 Transformación logarítmica del target
# ---------------------------------------------------------

df["log_precio"] = np.log1p(df[target])

print("\n=== DESCRIBE DE log_precio ===")
print(df["log_precio"].describe())

print("\n=== SKEWNESS DE log_precio ===")
print(round(df["log_precio"].skew(), 4))


# ---------------------------------------------------------
# 08.8 Histograma del target transformado
# ---------------------------------------------------------

plt.figure(figsize=(10, 6))
plt.hist(df["log_precio"], bins=50, edgecolor="black")
plt.title("Distribución logarítmica de precio_medio_entrada_promedio")
plt.xlabel("log(1 + precio)")
plt.ylabel("Frecuencia")
plt.tight_layout()
plt.savefig(os.path.join(output_dir, "03_histograma_log_precio.png"))
plt.show()


# ---------------------------------------------------------
# 08.9 Boxplot del target transformado
# ---------------------------------------------------------

plt.figure(figsize=(10, 4))
plt.boxplot(df["log_precio"], vert=False)
plt.title("Boxplot de log_precio")
plt.xlabel("log(1 + precio)")
plt.tight_layout()
plt.savefig(os.path.join(output_dir, "04_boxplot_log_precio.png"))
plt.show()


# ---------------------------------------------------------
# 08.10 Precio promedio por categoría
# ---------------------------------------------------------

df_categoria = (
    df.groupby("categoria")[target]
    .agg(["count", "mean", "median"])
    .sort_values(by="mean", ascending=False)
    .reset_index()
)

print("\n=== PRECIO PROMEDIO POR CATEGORÍA ===")
print(df_categoria)

plt.figure(figsize=(12, 6))
plt.bar(df_categoria["categoria"], df_categoria["mean"])
plt.title("Precio promedio por categoría")
plt.xlabel("Categoría")
plt.ylabel("Precio promedio")
plt.xticks(rotation=45, ha="right")
plt.tight_layout()
plt.savefig(os.path.join(output_dir, "05_precio_promedio_por_categoria.png"))
plt.show()


# ---------------------------------------------------------
# 08.11 Top 15 provincias por precio promedio
# ---------------------------------------------------------

df_provincia = (
    df.groupby("provincia")[target]
    .agg(["count", "mean", "median"])
    .sort_values(by="mean", ascending=False)
    .reset_index()
)

top15_provincias = df_provincia.head(15)

print("\n=== TOP 15 PROVINCIAS POR PRECIO PROMEDIO ===")
print(top15_provincias)

plt.figure(figsize=(12, 6))
plt.bar(top15_provincias["provincia"], top15_provincias["mean"])
plt.title("Top 15 provincias por precio promedio")
plt.xlabel("Provincia")
plt.ylabel("Precio promedio")
plt.xticks(rotation=45, ha="right")
plt.tight_layout()
plt.savefig(os.path.join(output_dir, "06_top15_provincias_precio_promedio.png"))
plt.show()


# ---------------------------------------------------------
# 08.12 Subcategorías con más observaciones
# ---------------------------------------------------------

df_subcategoria = (
    df.groupby("subcategoria")[target]
    .agg(["count", "mean"])
    .sort_values(by="count", ascending=False)
    .reset_index()
)

top15_subcategorias = df_subcategoria.head(15)

print("\n=== TOP 15 SUBCATEGORÍAS POR NÚMERO DE OBSERVACIONES ===")
print(top15_subcategorias)

plt.figure(figsize=(12, 6))
plt.bar(top15_subcategorias["subcategoria"], top15_subcategorias["count"])
plt.title("Top 15 subcategorías por número de observaciones")
plt.xlabel("Subcategoría")
plt.ylabel("Número de observaciones")
plt.xticks(rotation=45, ha="right")
plt.tight_layout()
plt.savefig(os.path.join(output_dir, "07_top15_subcategorias_observaciones.png"))
plt.show()


# ---------------------------------------------------------
# 08.13 Relación entre opiniones y precio
# ---------------------------------------------------------

plt.figure(figsize=(10, 6))
plt.scatter(df["total_opiniones_categoria_promedio"], df[target], alpha=0.4)
plt.title("Relación entre total_opiniones_categoria_promedio y precio")
plt.xlabel("Total opiniones")
plt.ylabel("Precio")
plt.tight_layout()
plt.savefig(os.path.join(output_dir, "08_scatter_opiniones_vs_precio.png"))
plt.show()


# ---------------------------------------------------------
# 08.14 Relación entre valoración general y precio
# ---------------------------------------------------------

plt.figure(figsize=(10, 6))
plt.scatter(df["valoracion_general_promedio"], df[target], alpha=0.4)
plt.title("Relación entre valoracion_general_promedio y precio")
plt.xlabel("Valoración general promedio")
plt.ylabel("Precio")
plt.tight_layout()
plt.savefig(os.path.join(output_dir, "09_scatter_valoracion_vs_precio.png"))
plt.show()


# ---------------------------------------------------------
# 08.15 Conclusión operativa para modelado
# ---------------------------------------------------------

print("\n=== CONCLUSIÓN OPERATIVA ===")
print("1. Revisar visualmente histogramas y boxplots.")
print("2. Si log_precio reduce claramente la asimetría, conviene modelar con log_precio.")
print("3. Revisar si provincias o categorías dominan precios extremos.")
print("4. Evaluar si total_opiniones_categoria_promedio requiere transformación logarítmica.")
print("5. Confirmar si se mantendrán outliers o si se tratarán antes del modelado.")