"""
CulturaTrip — Generador de dimensión temporal (dim_tiempo)
==========================================================
Genera un CSV con 12 filas (una por mes) que sirve como
tabla de dimensión temporal en el modelo estrella de Tableau.

Convenciones del proyecto:
  - BASE_DIR = Path(__file__).resolve().parents[2]
  - dtype=str en cargas CSV
  - QA summary al final

Uso:
  python generar_dim_tiempo.py

Salida:
  data/dim/dim_tiempo.csv
"""

from pathlib import Path
import pandas as pd
import itertools

# ─────────────────────────────────────────────
# 1. Configuración de rutas
# ─────────────────────────────────────────────
BASE_DIR = Path(__file__).resolve().parents[2]

RAW_DIR = BASE_DIR / "data" / "raw"
INTERIM_DIR = BASE_DIR / "data" / "interim"
CLEAN_DIR = BASE_DIR / "data" / "clean"
OUTPUTS_DIR = BASE_DIR / "outputs"

RAW_DIR.mkdir(parents=True, exist_ok=True)
INTERIM_DIR.mkdir(parents=True, exist_ok=True)
CLEAN_DIR.mkdir(parents=True, exist_ok=True)
OUTPUTS_DIR.mkdir(parents=True, exist_ok=True)

OUTPUT_PATH = CLEAN_DIR /  "dim_tiempo.csv"

# ─────────────────────────────────────────────
# 2. Parámetros
# ─────────────────────────────────────────────
AÑOS = [2023, 2024, 2025]
MESES = list(range(1, 13))

# ─────────────────────────────────────────────
# 3. Datos base por mes
# ─────────────────────────────────────────────
nombres_es = {
    1: "Enero", 2: "Febrero", 3: "Marzo", 4: "Abril",
    5: "Mayo", 6: "Junio", 7: "Julio", 8: "Agosto",
    9: "Septiembre", 10: "Octubre", 11: "Noviembre", 12: "Diciembre"
}

nombres_corto = {
    1: "Ene", 2: "Feb", 3: "Mar", 4: "Abr",
    5: "May", 6: "Jun", 7: "Jul", 8: "Ago",
    9: "Sep", 10: "Oct", 11: "Nov", 12: "Dic"
}

trimestres = {
    1: 1, 2: 1, 3: 1, 4: 2, 5: 2, 6: 2,
    7: 3, 8: 3, 9: 3, 10: 4, 11: 4, 12: 4
}

nombres_trimestre = {
    1: "Q1 (Ene-Mar)", 2: "Q2 (Abr-Jun)",
    3: "Q3 (Jul-Sep)", 4: "Q4 (Oct-Dic)"
}

semestres = {
    1: 1, 2: 1, 3: 1, 4: 1, 5: 1, 6: 1,
    7: 2, 8: 2, 9: 2, 10: 2, 11: 2, 12: 2
}

nombres_semestre = {1: "S1 (Ene-Jun)", 2: "S2 (Jul-Dic)"}

estaciones = {
    1: "Invierno", 2: "Invierno", 3: "Primavera", 4: "Primavera",
    5: "Primavera", 6: "Verano", 7: "Verano", 8: "Verano",
    9: "Otoño", 10: "Otoño", 11: "Otoño", 12: "Invierno"
}

# Basado en el análisis del dashboard:
#   - Alta: Jul-Sep (precios > 150 €/noche)
#   - Media: Mar-Jun, Oct (transición, Semana Santa)
#   - Baja: Ene-Feb, Nov-Dic (valle tarifario)
temporadas = {
    1: "Baja", 2: "Baja", 3: "Media", 4: "Media",
    5: "Media", 6: "Media", 7: "Alta", 8: "Alta",
    9: "Alta", 10: "Media", 11: "Baja", 12: "Baja"
}

temporada_orden = {"Baja": 1, "Media": 2, "Alta": 3}

# ─────────────────────────────────────────────
# 4. Construir DataFrame (año × mes)
# ─────────────────────────────────────────────
filas = []
for año, mes in itertools.product(AÑOS, MESES):
    trim = trimestres[mes]
    sem = semestres[mes]
    temp = temporadas[mes]

    filas.append({
        "año":                    año,
        "mes":                    mes,
        "año_mes":                f"{año}-{mes:02d}",
        "mes_nombre":             nombres_es[mes],
        "mes_nombre_corto":       nombres_corto[mes],
        "trimestre":              trim,
        "trimestre_nombre":       nombres_trimestre[trim],
        "año_trimestre":          f"{año}-Q{trim}",
        "semestre":               sem,
        "semestre_nombre":        nombres_semestre[sem],
        "estacion":               estaciones[mes],
        "temporada_turistica":    temp,
        "temporada_orden":        temporada_orden[temp],
        "es_temporada_alta":      str(temp == "Alta"),
        "es_temporada_baja":      str(temp == "Baja"),
        "es_periodo_vacacional":  str(mes in [7, 8, 12]),
    })

df = pd.DataFrame(filas)

# ─────────────────────────────────────────────
# 5. Exportar
# ─────────────────────────────────────────────
df.to_csv(OUTPUT_PATH, index=False, encoding="utf-8-sig")

# ─────────────────────────────────────────────
# 6. QA Summary
# ─────────────────────────────────────────────
print("=" * 60)
print("  dim_tiempo v2 — QA Summary")
print("=" * 60)
print(f"  Filas generadas:       {len(df)}")
print(f"  Columnas:              {len(df.columns)}")
print(f"  Años cubiertos:        {AÑOS}")
print(f"  Archivo de salida:     {OUTPUT_PATH}")
print(f"  Encoding:              utf-8-sig (BOM)")
print(f"  Temporadas alta:       {(df['es_temporada_alta']=='True').sum()} filas")
print(f"  Temporadas baja:       {(df['es_temporada_baja']=='True').sum()} filas")
print(f"  Trimestres únicos:     {df['año_trimestre'].nunique()}")
print()
print("  Preview (primeros 6 + últimos 6):")
print(df.head(6).to_string(index=False))
print("  ...")
print(df.tail(6).to_string(index=False))
print()
print("  ✅ dim_tiempo v2 generada correctamente")
print("=" * 60)
