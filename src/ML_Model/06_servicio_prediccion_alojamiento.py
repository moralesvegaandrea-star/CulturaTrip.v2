# =========================================================
# 06 - SERVICIO DE PREDICCIÓN DE ALOJAMIENTO
# =========================================================
# coste total de alojamiento de un viaje utilizando el
# modelo optimizado de regresión, integrando nombre de
# provincia, fechas, categoría de alojamiento y comparándolo
# contra el presupuesto disponible para alojamiento según la
# distribución presupuestaria definida en el proyecto.
# =========================================================
import os
import pandas as pd
import pickle
from pathlib import Path
from datetime import datetime, timedelta
from sqlalchemy import create_engine


print("\n==============================")
print("SERVICIO ML CULTURATRIP")
print("==============================\n")


# ---------------------------------------------------------
# 06.1 Definición de rutas
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

model_path = OUTPUTS_DIR/ "regresion_precios"/"modelos" / "random_forest_precio.pkl"
features_path = OUTPUTS_DIR / "regresion_precios"/"modelos" / "features_modelo_precio.pkl"


# ---------------------------------------------------------
# Constante de distribución presupuestaria
# ---------------------------------------------------------

PCT_ALOJAMIENTO_STANDARD = 0.35



# ---------------------------------------------------------
# 06.2 Cargar modelo optimizado y lista de variables
# ---------------------------------------------------------

print("Cargando modelo optimizado...")

with open(model_path, "rb") as archivo_modelo:
    modelo = pickle.load(archivo_modelo)

with open(features_path, "rb") as archivo_features:
    features_modelo = pickle.load(archivo_features)

print("Modelo cargado correctamente.\n")


# ---------------------------------------------------------
# 06.3 Conexión a PostgreSQL
# ---------------------------------------------------------

print("Conectando a PostgreSQL...")

engine = create_engine(
    "postgresql+psycopg2://culturatrip:culturatrip@localhost:5433/culturatrip"
)

query_provincias = """
SELECT DISTINCT
    f.id_pais,
    f.id_ccaa,
    f.id_provincia,
    p.provincia_nombre
FROM culturatrip.fact_alojamientos f
JOIN culturatrip.dim_provincia p
    ON f.id_provincia = p.id_provincia
ORDER BY f.id_provincia
"""

df_provincias = pd.read_sql(query_provincias, engine)
df_provincias["provincia_nombre"] = df_provincias["provincia_nombre"].str.lower().str.strip()

print("Provincias cargadas correctamente.")
print(df_provincias.head(), "\n")


# ---------------------------------------------------------
# 06.4 Función para clasificar temporada
# ---------------------------------------------------------

def obtener_temporada_cod(mes):
    if mes in [12, 1, 2]:
        return 1   # baja
    elif mes in [3, 4, 5, 10, 11]:
        return 2   # media
    elif mes in [6, 7, 8, 9]:
        return 3   # alta
    else:
        raise ValueError("Mes inválido para clasificar temporada.")


# ---------------------------------------------------------
# 06.5 Función para clasificar periodo de antelación
# ---------------------------------------------------------

def obtener_periodo_antelacion_cod(fecha_ida):
    hoy = datetime.today()
    dias_antelacion = (fecha_ida - hoy).days

    if dias_antelacion <= 7:
        return 1   # 1 semana
    elif dias_antelacion <= 14:
        return 2   # 2 semanas
    elif dias_antelacion <= 31:
        return 3   # 1 mes
    elif dias_antelacion <= 90:
        return 4   # 2-3 meses
    else:
        return 5   # 3 meses o más


# ---------------------------------------------------------
# 06.6 Mapeo de categoría de alojamiento
# ---------------------------------------------------------

def obtener_categoria_alojamiento_cod(categoria_alojamiento):
    mapa_categoria = {
        "hotel 3 estrellas": 1,
        "hotel 4 estrellas": 2,
        "hotel 5 estrellas": 3,
        "apartamento": 4,
        "casa entera": 5,
        "habitacion privada": 6,
        "habitacion compartida": 7,
        "alternativo": 8
    }

    categoria_normalizada = categoria_alojamiento.lower().strip()

    if categoria_normalizada not in mapa_categoria:
        raise ValueError(f"Categoría de alojamiento no reconocida: {categoria_alojamiento}")

    return mapa_categoria[categoria_normalizada]


# ---------------------------------------------------------
# 06.7 Función para contar noches
# ---------------------------------------------------------

def calcular_tipo_noches(fecha_inicio, fecha_fin):

    fecha_actual = fecha_inicio
    noches_semana = 0
    noches_fin_semana = 0

    while fecha_actual < fecha_fin:
        if fecha_actual.weekday() in [4, 5]:
            noches_fin_semana += 1
        else:
            noches_semana += 1

        fecha_actual += timedelta(days=1)

    return noches_semana, noches_fin_semana


# ---------------------------------------------------------
# 06.8 Obtener ids geográficos desde nombre provincia
# ---------------------------------------------------------

def obtener_datos_provincia(nombre_provincia):

    nombre_provincia = nombre_provincia.lower().strip()

    fila = df_provincias[df_provincias["provincia_nombre"] == nombre_provincia]

    if fila.empty:
        raise ValueError(f"Provincia no encontrada: {nombre_provincia}")

    return {
        "id_pais": fila.iloc[0]["id_pais"],
        "id_ccaa": int(fila.iloc[0]["id_ccaa"]),
        "id_provincia": int(fila.iloc[0]["id_provincia"])
    }


# ---------------------------------------------------------
# 06.9 Función principal del servicio
# ---------------------------------------------------------

# ---------------------------------------------------------
# 06.9 Función principal del servicio
# ---------------------------------------------------------

def estimar_coste_alojamiento(
    provincia_nombre,
    categoria_alojamiento,
    fecha_ida,
    fecha_regreso,
    presupuesto,
    valoraciones_norm=4.2,
    tiene_valoraciones=True
):

    fecha_ida_dt = datetime.strptime(fecha_ida, "%Y-%m-%d")
    fecha_regreso_dt = datetime.strptime(fecha_regreso, "%Y-%m-%d")

    if fecha_regreso_dt <= fecha_ida_dt:
        raise ValueError("La fecha de regreso debe ser posterior a la fecha de ida.")

    datos_provincia = obtener_datos_provincia(provincia_nombre)

    mes = fecha_ida_dt.month
    temporada_cod = obtener_temporada_cod(mes)
    periodo_antelacion_cod = obtener_periodo_antelacion_cod(fecha_ida_dt)
    categoria_alojamiento_cod = obtener_categoria_alojamiento_cod(categoria_alojamiento)

    noches_semana, noches_fin_semana = calcular_tipo_noches(fecha_ida_dt, fecha_regreso_dt)

    base_features = {
        "id_ccaa": datos_provincia["id_ccaa"],
        "id_provincia": datos_provincia["id_provincia"],
        "mes": mes,
        "temporada_cod": temporada_cod,
        "categoria_alojamiento_cod": categoria_alojamiento_cod,
        "periodo_antelacion_cod": periodo_antelacion_cod,
        "valoraciones_norm": valoraciones_norm,
        "tiene_valoraciones": tiene_valoraciones
    }

    input_semana = base_features.copy()
    input_semana["tipo_dia_cod"] = 0

    input_fin_semana = base_features.copy()
    input_fin_semana["tipo_dia_cod"] = 1

    X_semana = pd.DataFrame([input_semana])[features_modelo]
    X_fin_semana = pd.DataFrame([input_fin_semana])[features_modelo]

    precio_semana = modelo.predict(X_semana)[0]
    precio_fin_semana = modelo.predict(X_fin_semana)[0]

    coste_total = (
        precio_semana * noches_semana +
        precio_fin_semana * noches_fin_semana
    )

    # ============================================
    # Comparación correcta contra presupuesto de alojamiento
    # ============================================
    pct_alojamiento = PCT_ALOJAMIENTO_STANDARD
    budget_alojamiento = presupuesto * pct_alojamiento

    diferencia_alojamiento = budget_alojamiento - coste_total
    alcanza_presupuesto_alojamiento = diferencia_alojamiento >= 0

    resultado = {
        "provincia": str(provincia_nombre),
        "categoria_alojamiento": str(categoria_alojamiento),
        "mes": int(mes),
        "temporada_cod": int(temporada_cod),
        "periodo_antelacion_cod": int(periodo_antelacion_cod),
        "id_ccaa": int(datos_provincia["id_ccaa"]),
        "id_provincia": int(datos_provincia["id_provincia"]),
        "noches_semana": int(noches_semana),
        "noches_fin_semana": int(noches_fin_semana),
        "precio_semana": float(round(precio_semana, 2)),
        "precio_fin_semana": float(round(precio_fin_semana, 2)),
        "coste_total": float(round(coste_total, 2)),
        "presupuesto_total": float(round(presupuesto, 2)),
        "pct_alojamiento": float(pct_alojamiento),
        "budget_alojamiento": float(round(budget_alojamiento, 2)),
        "diferencia_alojamiento": float(round(diferencia_alojamiento, 2)),
        "alcanza_presupuesto_alojamiento": bool(alcanza_presupuesto_alojamiento)
    }

    return resultado

# ---------------------------------------------------------
# 06.10 Test automático del servicio
# ---------------------------------------------------------

print("Ejecutando prueba del servicio...\n")

resultado = estimar_coste_alojamiento(
    provincia_nombre="albacete",
    categoria_alojamiento="hotel 3 estrellas",
    fecha_ida="2026-03-26",
    fecha_regreso="2026-04-09",
    presupuesto=5000
)

print("RESULTADO SIMULACIÓN\n")
print("Provincia:", resultado["provincia"])
print("Categoría alojamiento:", resultado["categoria_alojamiento"])
print("Mes:", resultado["mes"])
print("ID CCAA:", resultado["id_ccaa"])
print("ID Provincia:", resultado["id_provincia"])
print("Noches semana:", resultado["noches_semana"])
print("Noches fin de semana:", resultado["noches_fin_semana"])
print("Precio estimado semana:", resultado["precio_semana"])
print("Precio estimado fin de semana:", resultado["precio_fin_semana"])
print("Coste total estimado alojamiento:", resultado["coste_total"])
print("Presupuesto total:", resultado["presupuesto_total"])
print("Porcentaje asignado a alojamiento:", resultado["pct_alojamiento"])
print("Presupuesto disponible para alojamiento:", resultado["budget_alojamiento"])

if resultado["alcanza_presupuesto_alojamiento"]:
    print("El presupuesto de alojamiento ALCANZA para este viaje.")
    print("Margen disponible:", resultado["diferencia_alojamiento"])
else:
    print("El presupuesto de alojamiento NO alcanza para este viaje.")
    print("Monto faltante:", abs(resultado["diferencia_alojamiento"]))