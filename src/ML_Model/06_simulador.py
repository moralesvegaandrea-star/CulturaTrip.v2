# =========================================================
# 06 - SIMULADOR DE PREDICCIÓN (V5)
# =========================================================
# Objetivo:
# Cargar el modelo final V5 guardado y simular una predicción
# de precio por noche con datos similares a los que ingresaría
# un usuario en Streamlit.
#
# También calcula:
# - costo total de alojamiento
# - presupuesto disponible para alojamiento según regla 35%
# - si el presupuesto alcanza o no
# =========================================================

import os
import joblib
import pandas as pd

from pathlib import Path


# ---------------------------------------------------------
# 06.1 Rutas
# ---------------------------------------------------------

BASE_DIR = Path(__file__).resolve().parents[2]

ML_DIR = BASE_DIR / "data" / "Machine Learning" / "modelo_precios_v5"

modelo_path = os.path.join(
    ML_DIR,
    "random_forest_precio_v5.pkl"
)

features_path = os.path.join(
    ML_DIR,
    "features_modelo_precio_v5.pkl"
)


# ---------------------------------------------------------
# 06.2 Cargar modelo y features
# ---------------------------------------------------------

print("Cargando modelo desde:")
print(modelo_path)

modelo = joblib.load(modelo_path)

print("\nCargando features desde:")
print(features_path)

features_modelo = joblib.load(features_path)

print("\nModelo y features cargados correctamente.")

print("\nFeatures esperadas por el modelo:")
print(features_modelo)


# ---------------------------------------------------------
# 06.3 Funciones geográficas V5
# ---------------------------------------------------------

provincias_insulares = [
    7,   # Illes Balears
    35,  # Las Palmas
    38   # Santa Cruz de Tenerife
]

provincias_costa = [
    15, 27, 36, 33, 39, 48, 20,
    17, 8, 43, 12, 46, 3, 30,
    4, 18, 29, 11, 21, 51, 52
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
# 06.4 Función preparar input usuario
# ---------------------------------------------------------

def preparar_input_usuario(
    id_provincia,
    mes,
    categoria_alojamiento,
    periodo_antelacion,
    valoraciones_norm,
    tiene_valoraciones,
    tipo_dia
):

    # ---------------------------------------------
    # Variables geográficas
    # ---------------------------------------------

    tipo_zona_turistica = clasificar_zona(
        id_provincia
    )

    gran_ciudad = clasificar_gran_ciudad(
        id_provincia
    )

    # ---------------------------------------------
    # Antelación
    # ---------------------------------------------

    map_antelacion = {

        "1 semana": 7,
        "2 semanas": 14,
        "1 mes": 30,
        "2-3 meses": 75,
        "3 meses": 90
    }

    antelacion_dias = map_antelacion[
        periodo_antelacion
    ]

    # ---------------------------------------------
    # Tipo día
    # ---------------------------------------------

    map_tipo_dia = {

        "semana": 0,
        "fin_semana": 1
    }

    tipo_dia_cod = map_tipo_dia[
        tipo_dia
    ]

    # ---------------------------------------------
    # Crear dataframe base
    # IMPORTANTE:
    # Se crean floats para evitar warnings de pandas
    # ---------------------------------------------

    input_modelo = pd.DataFrame(

        data=[[0.0] * len(features_modelo)],

        columns=features_modelo
    )

    # ---------------------------------------------
    # Variables numéricas / binarias
    # ---------------------------------------------

    input_modelo.loc[
        0,
        "gran_ciudad"
    ] = gran_ciudad

    input_modelo.loc[
        0,
        "valoraciones_norm"
    ] = valoraciones_norm

    input_modelo.loc[
        0,
        "tiene_valoraciones"
    ] = int(tiene_valoraciones)

    input_modelo.loc[
        0,
        "antelacion_dias"
    ] = antelacion_dias

    input_modelo.loc[
        0,
        "tipo_dia_cod"
    ] = tipo_dia_cod

    # ---------------------------------------------
    # One-Hot categoria alojamiento
    # ---------------------------------------------

    col_categoria = (
        f"categoria_alojamiento_{categoria_alojamiento}"
    )

    if col_categoria in input_modelo.columns:

        input_modelo.loc[
            0,
            col_categoria
        ] = 1

    # ---------------------------------------------
    # One-Hot mes
    # ---------------------------------------------

    col_mes = f"mes_{mes}"

    if col_mes in input_modelo.columns:

        input_modelo.loc[
            0,
            col_mes
        ] = 1

    # ---------------------------------------------
    # One-Hot zona turística
    # ---------------------------------------------

    col_zona = (
        f"tipo_zona_turistica_{tipo_zona_turistica}"
    )

    if col_zona in input_modelo.columns:

        input_modelo.loc[
            0,
            col_zona
        ] = 1

    return (
        input_modelo,
        tipo_zona_turistica,
        gran_ciudad
    )


# ---------------------------------------------------------
# 06.5 Simulación ejemplo
# ---------------------------------------------------------

presupuesto_total = 1000

porcentaje_alojamiento = 0.35

noches = 3

# ---------------------------------------------
# Ejemplo provincias:
# Barcelona = 8
# Madrid = 28
# Baleares = 7
# Málaga = 29
# Salamanca = 37
# ---------------------------------------------

id_provincia = 8

mes = 8

categoria_alojamiento = "hotel 5 estrellas"

periodo_antelacion = "1 mes"

valoraciones_norm = 4.5

tiene_valoraciones = 1

tipo_dia = "fin_semana"


# ---------------------------------------------------------
# 06.6 Preparar input y predecir
# ---------------------------------------------------------

(
    input_modelo,
    tipo_zona,
    gran_ciudad

) = preparar_input_usuario(

    id_provincia=id_provincia,

    mes=mes,

    categoria_alojamiento=categoria_alojamiento,

    periodo_antelacion=periodo_antelacion,

    valoraciones_norm=valoraciones_norm,

    tiene_valoraciones=tiene_valoraciones,

    tipo_dia=tipo_dia
)

precio_estimado_noche = modelo.predict(
    input_modelo
)[0]

costo_total_alojamiento = (
    precio_estimado_noche * noches
)

presupuesto_alojamiento = (
    presupuesto_total
    * porcentaje_alojamiento
)

alcanza = (
    costo_total_alojamiento
    <= presupuesto_alojamiento
)


# ---------------------------------------------------------
# 06.7 Resultados
# ---------------------------------------------------------

print("\n=== INPUT TRANSFORMADO PARA EL MODELO ===")
print(input_modelo)

print("\n=== VARIABLES GEOGRÁFICAS GENERADAS ===")

print(
    "Tipo zona turística:",
    tipo_zona
)

print(
    "Gran ciudad:",
    gran_ciudad
)

print("\n=== RESULTADOS SIMULACIÓN V5 ===")

print(
    f"Presupuesto total: {presupuesto_total:.2f} €"
)

print(
    f"Presupuesto alojamiento (35%): {presupuesto_alojamiento:.2f} €"
)

print(
    f"\nPrecio estimado por noche: {precio_estimado_noche:.2f} €"
)

print(f"Noches: {noches}")

print(
    f"Costo total alojamiento: {costo_total_alojamiento:.2f} €"
)

print("\n¿Alcanza el presupuesto?")

if alcanza:

    print("✅ SÍ")

else:

    print("❌ NO")


# ---------------------------------------------------------
# 06.8 Conclusión
# ---------------------------------------------------------

print("\n=== CONCLUSIÓN PASO 06 V5 ===")

print(
    "- El modelo V5 fue cargado correctamente desde archivo .pkl."
)

print(
    "- Se generaron las variables necesarias a partir de inputs del usuario."
)

print(
    "- id_provincia se utilizó únicamente para construir variables geográficas interpretables."
)

print(
    "- El modelo NO utiliza IDs directamente."
)

print(
    "- El precio estimado se compara contra el presupuesto de alojamiento definido en el TFM."
)

print(
    "- El pipeline queda listo para integración en Streamlit."
)


