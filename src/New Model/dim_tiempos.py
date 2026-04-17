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
# 2. Datos base
# ─────────────────────────────────────────────
meses = list(range(1, 13))

nombres_es = [
    "Enero", "Febrero", "Marzo", "Abril", "Mayo", "Junio",
    "Julio", "Agosto", "Septiembre", "Octubre", "Noviembre", "Diciembre"
]

nombres_corto = [
    "Ene", "Feb", "Mar", "Abr", "May", "Jun",
    "Jul", "Ago", "Sep", "Oct", "Nov", "Dic"
]

trimestres = [1, 1, 1, 2, 2, 2, 3, 3, 3, 4, 4, 4]

nombres_trimestre = [
    "Q1 (Ene-Mar)", "Q1 (Ene-Mar)", "Q1 (Ene-Mar)",
    "Q2 (Abr-Jun)", "Q2 (Abr-Jun)", "Q2 (Abr-Jun)",
    "Q3 (Jul-Sep)", "Q3 (Jul-Sep)", "Q3 (Jul-Sep)",
    "Q4 (Oct-Dic)", "Q4 (Oct-Dic)", "Q4 (Oct-Dic)"
]

semestres = [1, 1, 1, 1, 1, 1, 2, 2, 2, 2, 2, 2]

nombres_semestre = [
    "S1 (Ene-Jun)", "S1 (Ene-Jun)", "S1 (Ene-Jun)",
    "S1 (Ene-Jun)", "S1 (Ene-Jun)", "S1 (Ene-Jun)",
    "S2 (Jul-Dic)", "S2 (Jul-Dic)", "S2 (Jul-Dic)",
    "S2 (Jul-Dic)", "S2 (Jul-Dic)", "S2 (Jul-Dic)"
]

# ─────────────────────────────────────────────
# 3. Temporada turística (criterio CulturaTrip)
# ─────────────────────────────────────────────
# Basado en el análisis de estacionalidad del dashboard:
#   - Temporada alta: Jul-Sep (precios > 150 €/noche)
#   - Temporada media: Mar-Jun, Oct (transición, Semana Santa)
#   - Temporada baja: Ene-Feb, Nov-Dic (valle tarifario)

temporada_turistica = [
    "Baja", "Baja",               # Ene, Feb
    "Media", "Media", "Media",    # Mar, Abr, May
    "Media",                       # Jun
    "Alta", "Alta", "Alta",       # Jul, Ago, Sep
    "Media",                       # Oct
    "Baja", "Baja"                # Nov, Dic
]

# Orden numérico para ordenar temporadas en Tableau (Baja=1, Media=2, Alta=3)
temporada_orden = [
    1, 1, 2, 2, 2, 2, 3, 3, 3, 2, 1, 1
]

# ─────────────────────────────────────────────
# 4. Estación del año (hemisferio norte)
# ─────────────────────────────────────────────
estacion = [
    "Invierno", "Invierno", "Primavera", "Primavera", "Primavera",
    "Verano", "Verano", "Verano", "Otoño", "Otoño", "Otoño", "Invierno"
]

# ─────────────────────────────────────────────
# 5. Indicadores booleanos útiles
# ─────────────────────────────────────────────
es_temporada_alta = [m in [7, 8, 9] for m in meses]
es_temporada_baja = [m in [1, 2, 11, 12] for m in meses]
es_periodo_vacacional = [m in [7, 8, 12] for m in meses]  # verano + Navidad

# ─────────────────────────────────────────────
# 6. Construir DataFrame
# ─────────────────────────────────────────────
df = pd.DataFrame({
    "mes":                    meses,
    "mes_nombre":             nombres_es,
    "mes_nombre_corto":       nombres_corto,
    "trimestre":              trimestres,
    "trimestre_nombre":       nombres_trimestre,
    "semestre":               semestres,
    "semestre_nombre":        nombres_semestre,
    "estacion":               estacion,
    "temporada_turistica":    temporada_turistica,
    "temporada_orden":        temporada_orden,
    "es_temporada_alta":      es_temporada_alta,
    "es_temporada_baja":      es_temporada_baja,
    "es_periodo_vacacional":  es_periodo_vacacional,
})

# Convertir booleanos a texto para compatibilidad CSV/Tableau
df["es_temporada_alta"] = df["es_temporada_alta"].map({True: "True", False: "False"})
df["es_temporada_baja"] = df["es_temporada_baja"].map({True: "True", False: "False"})
df["es_periodo_vacacional"] = df["es_periodo_vacacional"].map({True: "True", False: "False"})

# ─────────────────────────────────────────────
# 7. Exportar
# ─────────────────────────────────────────────
df.to_csv(OUTPUT_PATH, index=False, encoding="utf-8-sig")

# ─────────────────────────────────────────────
# 8. QA Summary
# ─────────────────────────────────────────────
print("=" * 60)
print("  dim_tiempo — QA Summary")
print("=" * 60)
print(f"  Filas generadas:       {len(df)}")
print(f"  Columnas:              {len(df.columns)}")
print(f"  Archivo de salida:     {OUTPUT_PATH}")
print(f"  Encoding:              utf-8-sig (BOM)")
print(f"  Temporadas alta:       {df['es_temporada_alta'].value_counts()['True']} meses")
print(f"  Temporadas baja:       {df['es_temporada_baja'].value_counts()['True']} meses")
print(f"  Trimestres:            {df['trimestre'].nunique()}")
print()
print("  Preview:")
print(df.to_string(index=False))
print()
print("  ✅ dim_tiempo generada correctamente")
print("=" * 60)
