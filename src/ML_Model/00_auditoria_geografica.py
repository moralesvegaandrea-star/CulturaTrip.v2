# =========================================================
# 00 - AUDITORÍA GEOGRÁFICA (MODELO V5)
# =========================================================
# Objetivo:
# Validar que podemos construir variables geográficas
# interpretables a partir de id_provincia, sin usar IDs
# como variables predictoras y sin utilizar precio.
#
# Variables nuevas propuestas:
# - tipo_zona_turistica: costa / interior / insular
# - gran_ciudad: 1 si es Madrid o Barcelona, 0 en caso contrario
#
# Importante:
# id_provincia se usa SOLO como variable auxiliar para mapear.
# No se usará como predictor del modelo.
# =========================================================

import os
import pandas as pd
from pathlib import Path


# ---------------------------------------------------------
# 00.1 Rutas
# ---------------------------------------------------------

BASE_DIR = Path(__file__).resolve().parents[2]

ML_DIR = BASE_DIR / "data" / "Machine Learning"
input_path = os.path.join(ML_DIR, "df_precios_largo.csv")

OUTPUT_DIR = ML_DIR / "modelo_precios_v5"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

output_path = os.path.join(OUTPUT_DIR, "df_auditoria_geografica_v5.csv")


# ---------------------------------------------------------
# 00.2 Cargar dataset
# ---------------------------------------------------------

print("Leyendo dataset desde:")
print(input_path)

df = pd.read_csv(input_path)

print("\nDataset cargado correctamente.")
print("Dimensiones:", df.shape)

print("\nColumnas disponibles:")
print(df.columns.tolist())


# ---------------------------------------------------------
# 00.3 Validar columna id_provincia
# ---------------------------------------------------------

if "id_provincia" not in df.columns:
    raise ValueError("No existe la columna id_provincia en el dataset.")

print("\nProvincias únicas en dataset:")
print(sorted(df["id_provincia"].unique()))

print("\nCantidad de provincias únicas:")
print(df["id_provincia"].nunique())


# ---------------------------------------------------------
# 00.4 Definir clasificación territorial
# ---------------------------------------------------------
# Clasificación basada en códigos INE de provincia.
#
# Costa:
# Provincias peninsulares con litoral, incluyendo Ceuta y Melilla
# por su condición territorial costera.
#
# Insular:
# Baleares, Las Palmas y Santa Cruz de Tenerife.
#
# Interior:
# Resto de provincias.
# ---------------------------------------------------------

provincias_insulares = [
    7,   # Illes Balears
    35,  # Las Palmas
    38   # Santa Cruz de Tenerife
]

provincias_costa = [
    15,  # A Coruña
    27,  # Lugo
    36,  # Pontevedra
    33,  # Asturias
    39,  # Cantabria
    48,  # Bizkaia
    20,  # Gipuzkoa
    17,  # Girona
    8,   # Barcelona
    43,  # Tarragona
    12,  # Castellón
    46,  # Valencia
    3,   # Alicante
    30,  # Murcia
    4,   # Almería
    18,  # Granada
    29,  # Málaga
    11,  # Cádiz
    21,  # Huelva
    51,  # Ceuta
    52   # Melilla
]

grandes_ciudades = [
    28,  # Madrid
    8    # Barcelona
]


def clasificar_zona(id_provincia):
    if id_provincia in provincias_insulares:
        return "insular"
    elif id_provincia in provincias_costa:
        return "costa"
    else:
        return "interior"


def clasificar_gran_ciudad(id_provincia):
    if id_provincia in grandes_ciudades:
        return 1
    else:
        return 0


# ---------------------------------------------------------
# 00.5 Crear variables geográficas interpretables
# ---------------------------------------------------------

df["tipo_zona_turistica"] = df["id_provincia"].apply(clasificar_zona)
df["gran_ciudad"] = df["id_provincia"].apply(clasificar_gran_ciudad)


# ---------------------------------------------------------
# 00.6 Validar cobertura del mapeo
# ---------------------------------------------------------

print("\n=== DISTRIBUCIÓN tipo_zona_turistica ===")
print(df["tipo_zona_turistica"].value_counts())

print("\n=== DISTRIBUCIÓN gran_ciudad ===")
print(df["gran_ciudad"].value_counts())

print("\n=== VALIDACIÓN POR PROVINCIA ===")

df_validacion = (
    df.groupby("id_provincia")
    .agg(
        registros=("precio", "count"),
        tipo_zona_turistica=("tipo_zona_turistica", "first"),
        gran_ciudad=("gran_ciudad", "first"),
        precio_medio=("precio", "mean")
    )
    .reset_index()
    .sort_values(by="id_provincia")
)

print(df_validacion)


# ---------------------------------------------------------
# 00.7 Guardar auditoría
# ---------------------------------------------------------

df_validacion.to_csv(output_path, index=False, encoding="utf-8")

print("\nAuditoría geográfica guardada en:")
print(output_path)


# ---------------------------------------------------------
# 00.8 Conclusión del paso
# ---------------------------------------------------------

print("\n=== CONCLUSIÓN PASO 00 V5 ===")
print("- Se construyeron variables geográficas interpretables.")
print("- id_provincia se utilizó únicamente como variable auxiliar.")
print("- El modelo NO entrenará con id_provincia.")
print("- tipo_zona_turistica captura costa/interior/insular.")
print("- gran_ciudad identifica Madrid y Barcelona.")
print("- No se utilizó precio para construir las variables geográficas.")
