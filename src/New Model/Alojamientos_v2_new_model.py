import os
import time
from pathlib import Path
import requests
import pandas as pd
import numpy as np
import re
import unicodedata

# =========================
# HELPERS
# =========================
def normaliza(s: str) -> str:
    """lower + sin tildes + espacios limpios"""
    if pd.isna(s):
        return ""
    s = str(s).strip().lower()
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("utf-8")
    s = s.replace("–", "-").replace("—", "-")
    s = re.sub(r"\s*-\s*", "-", s)
    s = re.sub(r"\s+", " ", s)
    return s

def clean_headers(df: pd.DataFrame) -> pd.DataFrame:
    """BOM + strip + lower en headers"""
    df = df.copy()
    df.columns = (
        df.columns.astype(str)
        .str.replace("\ufeff", "", regex=False)
        .str.strip()
        .str.lower()
    )
    return df

def to_float_es(x):
    """Convierte '1.234,56' -> 1234.56"""
    if pd.isna(x):
        return np.nan
    x = str(x).strip()
    x = x.replace(".", "").replace(",", ".").replace("%", "")
    if x.lower() in ["", "nan", "none", "null", "-", "—"]:
        return np.nan
    try:
        return float(x)
    except ValueError:
        return np.nan

def ccaa_key(s: str) -> str:
    """Llave estable para CCAA (como la que ya te funciona)"""
    s = normaliza(s)

    if "navarra" in s:
        return "navarra"
    if "asturias" in s:
        return "asturias"
    if "madrid" in s:
        return "madrid"
    if "murcia" in s:
        return "murcia"
    if "rioja" in s:
        return "rioja"
    if "balears" in s or "illes" in s:
        return "balears"

    if "," in s:
        return s.split(",")[0].strip()

    return s

# =========================
# RUTAS
# =========================
BASE_DIR = Path(__file__).resolve().parents[2]

RAW_DIR = BASE_DIR / "data" / "raw"
INTERIM_DIR = BASE_DIR / "data" / "interim"
CLEAN_DIR = BASE_DIR / "data" / "clean"
OUTPUTS_DIR = BASE_DIR / "outputs"

RAW_DIR.mkdir(parents=True, exist_ok=True)
INTERIM_DIR.mkdir(parents=True, exist_ok=True)
CLEAN_DIR.mkdir(parents=True, exist_ok=True)
OUTPUTS_DIR.mkdir(parents=True, exist_ok=True)

# =========================
# INPUTS
# =========================
PATH_DIM_CCAA = CLEAN_DIR / "dim_ccaa_base.csv"
PATH_DIM_PROV = CLEAN_DIR / "dim_provincia_base.csv"

# Estos vienen de tu pipeline (interim)
PATH_HVAL = INTERIM_DIR / "df_hvaloracion_limpio.csv"
PATH_VUT = INTERIM_DIR / "df_vivienda_modelo_final.csv"

# =========================
# API DATAESTUR (HOTEL PRECIOS)
# =========================
BASE_URL = (
    "https://dataestur.azure-api.net/API-SEGITTUR-v1/"
    "PRECIOS_ALOJAMIENTOS_HOTELEROS_DL?"
    "desde%20%28a%C3%B1o%29=2023&desde%20%28mes%29=01&"
    "hasta%20%28a%C3%B1o%29=2026&hasta%20%28mes%29=12&"
    "CCAA=Todos&Provincia=Todos"
)

RAW_CSV_PATH = RAW_DIR / "precio_alojamientos_raw.csv"

# =========================
# 1) Descargar raw hotel (si no existe)
# =========================
headers = {"accept": "application/octet-stream"}
session = requests.Session()

if not RAW_CSV_PATH.exists():
    max_reintentos = 3
    for intento in range(1, max_reintentos + 1):
        r = session.get(BASE_URL, headers=headers, timeout=60)

        if r.status_code == 200:
            RAW_CSV_PATH.write_bytes(r.content)
            print("✅ Guardado raw:", RAW_CSV_PATH)
            break
        elif r.status_code == 429:
            espera = 30 * intento
            print(f"⚠️ 429 Too Many Requests. Esperando {espera}s... ({intento}/{max_reintentos})")
            time.sleep(espera)
        else:
            print("❌ Error:", r.status_code)
            print("Respuesta (primeros 300 chars):", r.text[:300])
            raise SystemExit
else:
    print("Usando raw existente:", RAW_CSV_PATH)

# =========================
# 2) Cargar dimensiones (CCAA + Provincia)
# =========================
dim_ccaa = pd.read_csv(PATH_DIM_CCAA, dtype=str, encoding="utf-8-sig")
dim_prov = pd.read_csv(PATH_DIM_PROV, dtype=str, encoding="utf-8-sig")

dim_ccaa = clean_headers(dim_ccaa)
dim_prov = clean_headers(dim_prov)

dim_ccaa["id_ccaa"] = dim_ccaa["id_ccaa"].astype(str).str.zfill(2)
dim_ccaa["ccaa_nombre"] = dim_ccaa["ccaa_nombre"].astype(str).apply(normaliza)
dim_ccaa["ccaa_key"] = dim_ccaa["ccaa_nombre"].apply(ccaa_key)
dim_ccaa = dim_ccaa.drop_duplicates(subset=["ccaa_key"]).copy()

# Provincia
if "id_pais" not in dim_prov.columns:
    dim_prov.insert(0, "id_pais", "ES")

dim_prov["id_pais"] = dim_prov["id_pais"].astype(str)
dim_prov["id_provincia"] = dim_prov["id_provincia"].astype(str).str.zfill(2)
dim_prov["provincia_nombre"] = dim_prov["provincia_nombre"].astype(str).apply(normaliza)

# =========================
# 3) Cargar HOTEL (Dataestur) y mapear IDs
# =========================
df_hotel = pd.read_csv(RAW_CSV_PATH, sep=";", encoding="latin-1", dtype=str)
df_hotel = clean_headers(df_hotel)

# =========================
# NORMALIZAR periodo_antelacion (principiante)
# =========================
def normaliza_periodo(s):
    if pd.isna(s):
        return pd.NA
    s = str(s).strip().lower()
    s = re.sub(r"\s+", " ", s)  # espacios dobles -> uno
    if s in ["", "nan", "none", "null", "-"]:
        return pd.NA
    return s

# Renombres por headers raros
df_hotel = df_hotel.rename(columns={
    "precio_check-in_entre_semana": "precio_checkin_entre_semana",
    "precio_check-in_fin_semana": "precio_checkin_fin_semana",
})

# Normalizar textos
for c in ["categoria_alojamiento", "ccaa", "provincia"]:
    if c in df_hotel.columns:
        df_hotel[c] = df_hotel[c].apply(normaliza)

# ccaa_key para mapear id_ccaa
df_hotel["ccaa_key"] = df_hotel["ccaa"].apply(ccaa_key)

# Mes numérico
df_hotel["mes"] = pd.to_numeric(df_hotel.get("mes", pd.NA), errors="coerce")

# Precios a float
for c in ["precio_checkin_entre_semana", "precio_checkin_fin_semana"]:
    if c not in df_hotel.columns:
        df_hotel[c] = np.nan
    df_hotel[c] = df_hotel[c].apply(to_float_es)


# id_pais fijo
df_hotel.insert(0, "id_pais", "ES")

# Provincia -> id_provincia
df_hotel = df_hotel.merge(
    dim_prov[["id_pais", "id_provincia", "provincia_nombre"]],
    left_on=["id_pais", "provincia"],
    right_on=["id_pais", "provincia_nombre"],
    how="left"
).drop(columns=["provincia_nombre"], errors="ignore")

# Normalizar periodo_antelacion si viene vacío
df_hotel["periodo_antelacion"] = df_hotel.get("periodo_antelacion", pd.NA)
df_hotel["periodo_antelacion"] = df_hotel["periodo_antelacion"].astype(str).str.strip()
df_hotel.loc[df_hotel["periodo_antelacion"].isin(["", "nan", "none", "null"]), "periodo_antelacion"] = pd.NA

df_hotel["periodo_antelacion"] = df_hotel["periodo_antelacion"].apply(normaliza_periodo)

# CCAA -> id_ccaa (por ccaa_key)
df_hotel = df_hotel.merge(
    dim_ccaa[["id_ccaa", "ccaa_key"]],
    on="ccaa_key",
    how="left"
)

print("QA HOTEL sin id_provincia:", df_hotel["id_provincia"].isna().sum())
print("QA HOTEL sin id_ccaa:", df_hotel["id_ccaa"].isna().sum())

# QA CCAA no match (si existe)
hotel_sin_ccaa = df_hotel[df_hotel["id_ccaa"].isna()].copy()
print("\nFILAS HOTEL CON id_ccaa NULO:", hotel_sin_ccaa.shape[0])

if hotel_sin_ccaa.shape[0] > 0:
    conteo = (
        hotel_sin_ccaa[["ccaa", "ccaa_key"]]
        .fillna("<<nulo>>")
        .value_counts()
        .reset_index(name="frecuencia")
    )
    qa_path = OUTPUTS_DIR / "qa_hotel_ccaa_no_match.csv"
    conteo.to_csv(qa_path, index=False, encoding="utf-8-sig")
    print("📝 QA guardado en:", qa_path)

# =========================
# 3.1) VALIDAR COLUMNAS DATAESTUR (PRINCIPIANTE)
# =========================
print("\n--- Columnas recibidas Dataestur (raw) ---")
print(df_hotel.columns.tolist())

# Columnas mínimas que normalmente necesitas para tu modelo
required_cols = {
    "mes",
    "ccaa",
    "provincia",
    "categoria_alojamiento",
    "periodo_antelacion",
    "precio_checkin_entre_semana",
    "precio_checkin_fin_semana",
}

missing = required_cols - set(df_hotel.columns)
if missing:
    print("\n❌ FALTAN COLUMNAS EN DATAESTUR:", missing)

    # Guardar QA para revisarlo fácil
    qa_missing_path = OUTPUTS_DIR / "qa_dataestur_missing_columns.txt"
    with open(qa_missing_path, "w", encoding="utf-8") as f:
        f.write("Faltan columnas en Dataestur:\n")
        for c in sorted(list(missing)):
            f.write(f"- {c}\n")
        f.write("\nColumnas disponibles:\n")
        for c in df_hotel.columns.tolist():
            f.write(f"- {c}\n")

    print("📝 QA guardado en:", qa_missing_path)
else:
    print("\n✅ OK: Dataestur trae todas las columnas requeridas.")

# =========================
# 4) Imputación de nulos en precios (hotel)
# =========================
pk_hotel = ["id_pais", "id_ccaa", "id_provincia", "mes", "categoria_alojamiento","periodo_antelacion"]

for col in ["precio_checkin_entre_semana", "precio_checkin_fin_semana"]:
    df_hotel[col] = df_hotel.groupby(pk_hotel)[col].transform(lambda x: x.fillna(x.mean()))
    df_hotel[col] = df_hotel.groupby(["id_pais", "id_ccaa", "mes", "categoria_alojamiento"])[col].transform(lambda x: x.fillna(x.mean()))
    df_hotel[col] = df_hotel.groupby(["id_pais", "mes", "categoria_alojamiento"])[col].transform(lambda x: x.fillna(x.mean()))
    df_hotel[col] = df_hotel[col].fillna(df_hotel[col].mean())

# =========================
# 5) Consolidar HOTEL a PK (sin año)
# =========================
df_hotel = df_hotel.drop(columns=["año"], errors="ignore")

print("Nulos periodo_antelacion (HOTEL raw):", df_hotel["periodo_antelacion"].isna().sum())
print("Valores únicos periodo_antelacion (top 20):",
      df_hotel["periodo_antelacion"].dropna().unique()[:20])

df_hotel_agg = (
    df_hotel.groupby(pk_hotel, as_index=False)
    .agg(
        precio_checkin_entre_semana=("precio_checkin_entre_semana", "mean"),
        precio_checkin_fin_semana=("precio_checkin_fin_semana", "mean"),
    )
)

df_hotel_agg["precio_checkin_entre_semana"] = df_hotel_agg["precio_checkin_entre_semana"].round(2)
df_hotel_agg["precio_checkin_fin_semana"] = df_hotel_agg["precio_checkin_fin_semana"].round(2)

df_hotel_agg["fuente"] = "hotel_dataestur"
df_hotel_agg["granularidad_origen"] = "provincia"
df_hotel_agg["es_dato_replicado"] = False
df_hotel_agg["nivel_geografico"] = "provincia"

print("Columnas df_hotel_agg:", df_hotel_agg.columns.tolist())
print("Nulos periodo_antelacion (HOTEL agg):", df_hotel_agg["periodo_antelacion"].isna().sum())

# =========================
# 6) Merge valoraciones hoteleras (HVAL) por IDs
# =========================
df_hval = pd.read_csv(PATH_HVAL, dtype=str, encoding="utf-8-sig")
df_hval = clean_headers(df_hval)
print(df_hval.columns.tolist())


df_hval["id_pais"] = df_hval.get("id_pais", "ES").astype(str)
df_hval["id_ccaa"] = df_hval["id_ccaa"].astype(str).str.zfill(2)
df_hval["id_provincia"] = df_hval["id_provincia"].astype(str).str.zfill(2)
df_hval["mes"] = pd.to_numeric(df_hval["mes"], errors="coerce")
df_hval["valoraciones"] = pd.to_numeric(df_hval["valoraciones"], errors="coerce")

pk_val = ["id_pais", "id_ccaa", "id_provincia", "mes", "categoria_alojamiento"]
df_hval = df_hval.groupby(pk_val, as_index=False).agg(valoraciones=("valoraciones", "mean"))

df_hotel_agg = df_hotel_agg.merge(
    df_hval[pk_val + ["valoraciones"]],
    on=pk_val,
    how="left"
)

df_hotel_agg["tiene_valoraciones"] = df_hotel_agg["valoraciones"].notna()

# =========================
# 7) VUT (CCAA) -> expandir a PROVINCIAS
# =========================
PATH_DIM_BASE = INTERIM_DIR / "dim_municipio_base.csv"   # ✅ usar base en interim

df_vut = pd.read_csv(PATH_VUT, dtype=str, encoding="utf-8-sig")
df_vut = clean_headers(df_vut)

# normalizar periodo_antelacion si existe (para evitar "1 Mes" vs "1 mes")
if "periodo_antelacion" in df_vut.columns:
    df_vut["periodo_antelacion"] = df_vut["periodo_antelacion"].apply(normaliza_periodo)

print("df_vut", df_vut.columns.tolist())

# Asegurar columnas mínimas en VUT
df_vut["id_pais"] = df_vut.get("id_pais", "ES").astype(str)

if "id_ccaa" not in df_vut.columns:
    raise KeyError(f"VUT no trae 'id_ccaa'. Revisa el archivo: {PATH_VUT}")
df_vut["id_ccaa"] = df_vut["id_ccaa"].astype(str).str.zfill(2)

# Mes numérico
if "mes" in df_vut.columns:
    df_vut["mes"] = pd.to_numeric(df_vut["mes"], errors="coerce")
else:
    df_vut["mes"] = pd.NA

# Asegurar columnas de valores
for col in ["precio_checkin_entre_semana", "precio_checkin_fin_semana", "valoraciones"]:
    if col in df_vut.columns:
        df_vut[col] = pd.to_numeric(df_vut[col], errors="coerce")
    else:
        df_vut[col] = np.nan

MAP_PERIODO = {
    "1 mes": "1 mes",
    "1 semana": "1 semana",
    "2 semanas": "2 semanas",
    "3 meses": "3 meses",
    "2-3 meses": "2-3 meses",
    "2 - 3 meses": "2-3 meses"
}

def normaliza_periodo(s):
    if pd.isna(s):
        return pd.NA
    s = str(s).strip().lower()
    s = re.sub(r"\s+", " ", s)
    s = s.replace("–", "-").replace("—", "-")
    if s in ["", "nan", "none", "null", "-"]:
        return pd.NA
    return MAP_PERIODO.get(s, s)  # si no está mapeado, lo deja igual

# -------------------------
# Cargar base municipios (solo para mapa CCAA -> provincias)
# -------------------------
df_base = pd.read_csv(PATH_DIM_BASE, dtype=str, encoding="utf-8-sig")
df_base = clean_headers(df_base)

# Validar columnas mínimas en dim_municipio_base
need_base = {"id_ccaa", "id_provincia"}
missing_base = need_base - set(df_base.columns)
if missing_base:
    raise KeyError(f"dim_municipio_base no tiene columnas requeridas: {missing_base}. Revisa: {PATH_DIM_BASE}")

df_base["id_ccaa"] = df_base["id_ccaa"].astype(str).str.zfill(2)
df_base["id_provincia"] = df_base["id_provincia"].astype(str).str.zfill(2)

# Tabla puente: 1 fila por (ccaa, provincia)
ccaa_to_prov = (
    df_base[["id_ccaa", "id_provincia"]]
    .dropna()
    .drop_duplicates()
    .reset_index(drop=True)
)

# -------------------------
# Expandir: VUT (ccaa) x provincias de esa ccaa
# -------------------------
df_vut = df_vut.merge(
    ccaa_to_prov,
    on="id_ccaa",
    how="left",
    validate="m:m"
)

print("✅ VUT expandido a provincia. Filas:", df_vut.shape[0])
print("❗ QA VUT filas sin id_provincia (debería ser 0):", df_vut["id_provincia"].isna().sum())

# Metadatos
df_vut["fuente"] = "vut_privada"
df_vut["granularidad_origen"] = "ccaa"
df_vut["es_dato_replicado"] = True
df_vut["nivel_geografico"] = "provincia"
df_vut["tiene_valoraciones"] = df_vut["valoraciones"].notna()


# =========================
# 8) Unir HOTEL + VUT (SIN LAT/LON)
# =========================
cols_final = [
    "id_pais",
    "id_ccaa",
    "id_provincia",
    "mes",
    "categoria_alojamiento",
    "periodo_antelacion",
    "precio_checkin_entre_semana",
    "precio_checkin_fin_semana",
    "valoraciones",
    "tiene_valoraciones",
    "fuente",
    "granularidad_origen",
    "es_dato_replicado",
    "nivel_geografico",
]

# Asegurar columnas (por si faltan)
for c in cols_final:
    if c not in df_hotel_agg.columns:
        df_hotel_agg[c] = pd.NA
    if c not in df_vut.columns:
        df_vut[c] = pd.NA

df_hotel_agg = df_hotel_agg[cols_final].copy()
df_vut = df_vut[cols_final].copy()

df_aloj = pd.concat([df_hotel_agg, df_vut], ignore_index=True)

# PK artificial
df_aloj = df_aloj.reset_index(drop=True)
df_aloj["id_alojamiento"] = df_aloj.index + 1

orden = ["id_alojamiento"] + cols_final
df_aloj = df_aloj[orden]

# Detectar escala hotel (0–100) y llevarla a 0–5
df_aloj["valoraciones_norm"] = df_aloj["valoraciones"]

mask_hotel = df_aloj["fuente"] == "hotel_dataestur"
df_aloj.loc[mask_hotel, "valoraciones_norm"] = (
    df_aloj.loc[mask_hotel, "valoraciones"] / 20
)

print("\n✅ Shape final df_aloj:", df_aloj.shape)
print("✅ Nulos id_ccaa:", df_aloj["id_ccaa"].isna().sum())
print("✅ Nulos id_provincia:", df_aloj["id_provincia"].isna().sum())
print(df_aloj["periodo_antelacion"].value_counts(dropna=False))
print(df_aloj.dtypes)
print(df_aloj.groupby("fuente")["valoraciones_norm"].agg(["min", "max"]))
df_aloj = df_aloj.drop(columns=["valoraciones"])
# =========================
# 9) Guardar (FINAL en CLEAN)
# =========================
OUT_FILE = CLEAN_DIR / "df_alojamientos.csv"
df_aloj.to_csv(OUT_FILE, index=False, encoding="utf-8-sig")
print("✅ Guardado FINAL (clean):", OUT_FILE)

# (Opcional) copia en outputs para auditoría/QA
OUT_FILE_QA = OUTPUTS_DIR / "df_alojamientos_qa.csv"
df_aloj.to_csv(OUT_FILE_QA, index=False, encoding="utf-8-sig")
print("📝 Copia QA (outputs):", OUT_FILE_QA)
