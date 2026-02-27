import os
import pandas as pd
def load_csv(path):
    try:
        return pd.read_csv(
            path,
            encoding="utf-8",
            sep=None,  # 🔑 detección automática
            engine="python"  # 🔑 necesario para sep=None
        )
    except UnicodeDecodeError:
        return pd.read_csv(
            path,
            encoding="latin1",
            sep=None,
            engine="python"
        )


BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RAW_DIR = os.path.join(BASE_DIR, "data", "raw")
CLEAN_DIR = os.path.join(BASE_DIR, "data", "clean")
OUTPUTS_DIR = os.path.join(BASE_DIR, "outputs")
NOTEBOOKS_DIR = os.path.join(BASE_DIR, "notebooks")

df_mun = pd.read_csv("../data/raw/df_codmun.csv")
df_isla = pd.read_csv("../data/raw/df_islas.csv")
df_dicc = pd.read_csv("../data/raw/df_dic.csv")

# =========================
# 5) VALIDACIONES BÁSICAS (PRINCIPIANTE)
# =========================
print("\n--- SHAPES ---")
print("Municipios:", df_mun.shape)
print("Islas:", df_isla.shape)

print("\n--- COLUMNAS MUNICIPIOS ---")
print(df_mun.columns.tolist())

print("\n--- EJEMPLO MUNICIPIOS (5 filas) ---")
print(df_mun.head(5))

print("\n--- EJEMPLO PROVINCIA + MUNICIPIO (10 filas) ---")
cols_show = [c for c in ["provincia", "cpro", "cmun", "nombre"] if c in df_mun.columns]
print(df_mun[cols_show].head(10))
print("\n--- EJEMPLO Islas (5 filas) ---")
print(df_isla.head(5))
print(df_mun.columns.tolist())
print(df_isla.columns.tolist())
# Append Columna Datasets Islas y Municipios
#Paso 1 alinear columnas
# columnas finales que queremos
cols_finales = [
    "id_municipio",
    "cpro",
    "cmun",
    "dc",
    "nombre",
    "provincia",
    "cisla",
    "isla"
]
# añadir columnas faltantes en df_mun
for col in ["cisla", "isla"]:
    if col not in df_mun.columns:
        df_mun[col] = None
# asegurar mismo orden de columnas
df_mun = df_mun[cols_finales]
df_islas = df_isla[cols_finales]
#Paso Append
df_espana = pd.concat(
    [df_mun, df_islas],
    ignore_index=True
)
#Validacion de Append
print("Shape final:", df_espana.shape)
print(df_espana.head(10))
print(df_espana.tail(10))
print("\nNulos por columna:")
print(df_espana.isna().sum())
# Tratamiento de Nulos
df_espana["cisla"] = df_espana["cisla"].fillna("No aplica")
df_espana["isla"]  = df_espana["isla"].fillna("No aplica")
# Agregar Columna Pais y Codigo Pais
df_espana["pais"] = "España"
df_espana["country_code"] = "ES"  # opcional, útil para mapas/APIs
# Siguiente Paso seria hacer un Join con el dataset diccionario
#Vamos a verificar las columnas
print(df_dicc.columns.tolist())
print(df_espana.columns.tolist())
print(df_dicc.head())


# df_espana.cpro  ←→  df_dic.cpro Los vamos unir por medio de ese valor
# Primero limpieza para dataset Dic
df_dic_ccaa = (
    df_dicc[["id_municipio"]]
    .drop_duplicates()
    .reset_index(drop=True)
)
print(df_dic_ccaa.head(10))
print(df_dic_ccaa.tail(10))
#Merge de datasets
df_espana_ccaa = df_espana.merge(
    df_dic_ccaa,
    on="id_municipio",
    how="left"
)
#Validar despues del merge
print(df_espana_ccaa.head(10))
print(df_espana_ccaa.tail(10))
#Por ultimo organizar columnas del dataset
def ordenar_columnas(df_espana_ccaa, orden_preferido):
    # columnas que sí existen en el df
    orden_existente = [col for col in orden_preferido if col in df_espana_ccaa.columns]
    # columnas que no fueron especificadas
    resto = [col for col in df_espana_ccaa.columns if col not in orden_existente]
    return df_espana_ccaa[orden_existente + resto]
#Lista del orden de columnas
orden_dim_geografia = [
    "pais",
    "country_code",
    "codauto",
    "provincia",
    "cpro",
    "cmun",
    "id_municipio",
    "nombre",
    "cisla",
    "isla",
    "dc"
]
df_espana_ccaa = ordenar_columnas(df_espana_ccaa, orden_dim_geografia)
#Validacion de orden
print(df_espana_ccaa.columns.tolist())
print(df_espana_ccaa.head(5))

print("id_municipio únicos:", df_espana_ccaa["id_municipio"].nunique(dropna=True))
print("id_municipio nulos:", df_espana_ccaa["id_municipio"].isna().sum())
print("Ejemplos id_municipio:", df_espana_ccaa["id_municipio"].head(10).tolist())

# Provincia de España
freq_provincia = df_espana_ccaa["provincia"].value_counts().reset_index()
freq_provincia.columns = ["provincia","frecuencia"]
print(freq_provincia)

freq_nombre= df_espana_ccaa["nombre"].value_counts().reset_index()
freq_nombre.columns = ["nombre","frecuencia"]
print(freq_nombre)

#Importar nuevo dataset a cvs
os.makedirs(CLEAN_DIR, exist_ok=True)
output_path = os.path.join(CLEAN_DIR, "df_espana_ccaa.csv")
df_espana_ccaa.to_csv(
    output_path,
    index=False,
    encoding="utf-8-sig"
)
print("Documento Descargado y Guardado")