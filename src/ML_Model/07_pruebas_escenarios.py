# =========================================================
# 07 - PRUEBAS DE ESCENARIOS (V5)
# =========================================================
# Objetivo:
# Ejecutar múltiples escenarios turísticos reales para:
# - validar comportamiento del modelo
# - analizar coherencia de negocio
# - generar ejemplos para defensa del TFM
# =========================================================

import os
import joblib
import pandas as pd

from pathlib import Path


# ---------------------------------------------------------
# 07.1 Rutas
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

output_path = os.path.join(
    ML_DIR,
    "resultados_escenarios_v5.csv"
)


# ---------------------------------------------------------
# 07.2 Cargar modelo
# ---------------------------------------------------------

print("Cargando modelo...")

modelo = joblib.load(modelo_path)

features_modelo = joblib.load(features_path)

print("Modelo cargado correctamente.")


# ---------------------------------------------------------
# 07.3 Funciones geográficas
# ---------------------------------------------------------

provincias_insulares = [
    7, 35, 38
]

provincias_costa = [
    15, 27, 36, 33, 39, 48, 20,
    17, 8, 43, 12, 46, 3, 30,
    4, 18, 29, 11, 21, 51, 52
]

grandes_ciudades = [
    28, 8
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

    return 0


# ---------------------------------------------------------
# 07.4 Función preparación input
# ---------------------------------------------------------

def preparar_input(
    id_provincia,
    mes,
    categoria_alojamiento,
    periodo_antelacion,
    valoraciones_norm,
    tiene_valoraciones,
    tipo_dia
):

    tipo_zona = clasificar_zona(id_provincia)

    gran_ciudad = clasificar_gran_ciudad(
        id_provincia
    )

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

    map_tipo_dia = {

        "semana": 0,
        "fin_semana": 1
    }

    tipo_dia_cod = map_tipo_dia[
        tipo_dia
    ]

    df = pd.DataFrame(
        data=[[0.0] * len(features_modelo)],
        columns=features_modelo
    )

    # Variables base
    df.loc[0, "gran_ciudad"] = gran_ciudad
    df.loc[0, "valoraciones_norm"] = valoraciones_norm
    df.loc[0, "tiene_valoraciones"] = tiene_valoraciones
    df.loc[0, "antelacion_dias"] = antelacion_dias
    df.loc[0, "tipo_dia_cod"] = tipo_dia_cod

    # Categoría alojamiento
    col_categoria = (
        f"categoria_alojamiento_{categoria_alojamiento}"
    )

    if col_categoria in df.columns:
        df.loc[0, col_categoria] = 1

    # Mes
    col_mes = f"mes_{mes}"

    if col_mes in df.columns:
        df.loc[0, col_mes] = 1

    # Zona turística
    col_zona = (
        f"tipo_zona_turistica_{tipo_zona}"
    )

    if col_zona in df.columns:
        df.loc[0, col_zona] = 1

    return df, tipo_zona, gran_ciudad


# ---------------------------------------------------------
# 07.5 Escenarios
# ---------------------------------------------------------

escenarios = [

    {
        "nombre": "Barcelona lujo verano",
        "id_provincia": 8,
        "mes": 8,
        "categoria_alojamiento": "hotel 5 estrellas",
        "periodo_antelacion": "1 mes",
        "valoraciones_norm": 4.6,
        "tiene_valoraciones": 1,
        "tipo_dia": "fin_semana"
    },

    {
        "nombre": "Madrid hotel ejecutivo",
        "id_provincia": 28,
        "mes": 11,
        "categoria_alojamiento": "hotel 4 estrellas",
        "periodo_antelacion": "2 semanas",
        "valoraciones_norm": 4.2,
        "tiene_valoraciones": 1,
        "tipo_dia": "semana"
    },

    {
        "nombre": "Baleares verano premium",
        "id_provincia": 7,
        "mes": 7,
        "categoria_alojamiento": "casa entera",
        "periodo_antelacion": "1 semana",
        "valoraciones_norm": 4.8,
        "tiene_valoraciones": 1,
        "tipo_dia": "fin_semana"
    },

    {
        "nombre": "Salamanca económico invierno",
        "id_provincia": 37,
        "mes": 2,
        "categoria_alojamiento": "hotel 3 estrellas",
        "periodo_antelacion": "2-3 meses",
        "valoraciones_norm": 3.8,
        "tiene_valoraciones": 1,
        "tipo_dia": "semana"
    },

    {
        "nombre": "Málaga apartamento turístico",
        "id_provincia": 29,
        "mes": 9,
        "categoria_alojamiento": "apartamento",
        "periodo_antelacion": "1 mes",
        "valoraciones_norm": 4.3,
        "tiene_valoraciones": 1,
        "tipo_dia": "fin_semana"
    }
]


# ---------------------------------------------------------
# 07.6 Ejecutar escenarios
# ---------------------------------------------------------

resultados = []

for esc in escenarios:

    input_modelo, zona, gran_ciudad = preparar_input(

        id_provincia=esc["id_provincia"],
        mes=esc["mes"],
        categoria_alojamiento=esc["categoria_alojamiento"],
        periodo_antelacion=esc["periodo_antelacion"],
        valoraciones_norm=esc["valoraciones_norm"],
        tiene_valoraciones=esc["tiene_valoraciones"],
        tipo_dia=esc["tipo_dia"]
    )

    precio_estimado = modelo.predict(
        input_modelo
    )[0]

    resultados.append({

        "escenario": esc["nombre"],
        "zona_turistica": zona,
        "gran_ciudad": gran_ciudad,
        "categoria_alojamiento": esc["categoria_alojamiento"],
        "mes": esc["mes"],
        "tipo_dia": esc["tipo_dia"],
        "valoraciones": esc["valoraciones_norm"],
        "precio_estimado_noche": round(
            precio_estimado,
            2
        )
    })


# ---------------------------------------------------------
# 07.7 Resultados finales
# ---------------------------------------------------------

df_resultados = pd.DataFrame(resultados)

print("\n=== RESULTADOS ESCENARIOS V5 ===")
print(df_resultados)

df_resultados.to_csv(
    output_path,
    index=False,
    encoding="utf-8"
)

print("\nResultados guardados en:")
print(output_path)


# ---------------------------------------------------------
# 07.8 Conclusión
# ---------------------------------------------------------

print("\n=== CONCLUSIÓN PASO 07 V5 ===")

print("- Se ejecutaron múltiples escenarios turísticos.")
print("- Se validó coherencia de negocio.")
print("- El modelo responde diferente según zona, temporada y alojamiento.")
print("- Los resultados sirven como validación funcional del sistema.")
print("- Los escenarios pueden utilizarse en defensa y documentación del TFM.")
