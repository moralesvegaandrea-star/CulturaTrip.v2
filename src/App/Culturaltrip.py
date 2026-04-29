import streamlit as st
import pandas as pd
from pathlib import Path
import altair as alt
import os
from sqlalchemy import create_engine, text
from datetime import date
import math
import joblib
import numpy as np
import pickle
from sklearn.metrics.pairwise import cosine_similarity

@st.cache_resource(show_spinner=False)
def get_engine():
    # Defaults pensados para tu Docker (ajusta PORT si usas 5433)
    DB_USER = os.getenv("DB_USER", "culturatrip")
    DB_PASSWORD = os.getenv("DB_PASSWORD", "culturatrip")
    DB_HOST = os.getenv("DB_HOST", "localhost")   # desde tu PC: localhost
    DB_PORT = os.getenv("DB_PORT", "5433")        # si cambiaste a 5433, deja esto
    DB_NAME = os.getenv("DB_NAME", "culturatrip")

    return create_engine(
        f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}",
        pool_pre_ping=True
    )

# =========================
# Implementacion del boton aprobacion
# =========================
def guardar_plan_db():
    engine = get_engine()

    # 1) Tomar valores desde session_state
    email = st.session_state.get("email")
    id_pais_origen = st.session_state.get("id_pais_origen")
    id_pais_destino = st.session_state.get("id_pais")
    fecha_ida = st.session_state.get("fecha_ida")
    fecha_regreso = st.session_state.get("fecha_regreso")
    presupuesto = st.session_state.get("presupuesto")
    tipo_viaje = st.session_state.get("tipo_viaje", "Solo")
    categoria_aloj = st.session_state.get("categoria_alojamiento")

    id_provincia = st.session_state.get("id_provincia_destino")  # VARCHAR(2)
    categorias_act = st.session_state.get("categorias_actividad", [])
    cantidades_act = st.session_state.get("cantidades_actividad", {})

    # 2) Validación mínima
    if not (email and id_pais_origen and id_pais_destino and fecha_ida and fecha_regreso and categoria_aloj):
        st.error("Faltan datos: revisa correo, países, fechas y hospedaje.")
        return None

    if fecha_regreso < fecha_ida:
        st.error("Fecha regreso no puede ser anterior a fecha ida.")
        return None

    # 3) Insert con transacción
    with engine.begin() as conn:

        # Insert header -> devuelve id_plan
        sql_plan = text("""
            INSERT INTO culturatrip.fact_plan_viaje (
                email_usuario, id_pais_origen, id_pais_destino,
                fecha_ida, fecha_regreso, presupuesto_estimado,
                tipo_viaje, categoria_alojamiento, perfil_presupuesto
            )
            VALUES (
                :email, :id_pais_origen, :id_pais_destino,
                :fecha_ida, :fecha_regreso, :presupuesto,
                :tipo_viaje, :categoria_aloj, :perfil_presupuesto
            )
            ON CONFLICT (email_usuario, id_pais_origen, id_pais_destino, fecha_ida, fecha_regreso, categoria_alojamiento)
            DO NOTHING
            RETURNING id_plan;
        """)

        plan_id = conn.execute(sql_plan, {
            "email": email,
            "id_pais_origen": id_pais_origen,
            "id_pais_destino": id_pais_destino,
            "fecha_ida": fecha_ida,
            "fecha_regreso": fecha_regreso,
            "presupuesto": presupuesto,
            "tipo_viaje": tipo_viaje,
            "categoria_aloj": categoria_aloj,
            "perfil_presupuesto": st.session_state.get("perfil_presupuesto", "standard")
        }).scalar()

        # Si ya existía, NO seguir insertando detalle/preferencias
        if plan_id is None:
            st.warning("Este plan ya existe. No se guardó un duplicado.")
            return None

        # Insert destino (provincia) si existe
        if id_provincia:
            sql_dest = text("""
                INSERT INTO culturatrip.fact_plan_viaje_destino (id_plan, orden, id_provincia)
                VALUES (:id_plan, 1, :id_provincia);
            """)
            conn.execute(sql_dest, {"id_plan": plan_id, "id_provincia": id_provincia})


        if categorias_act:
            sql_pref = text("""
                        INSERT INTO culturatrip.fact_plan_viaje_preferencia (id_plan, categoria, cantidad)
                        VALUES (:id_plan, :categoria, :cantidad)
                        ON CONFLICT (id_plan, categoria)
                        DO UPDATE SET cantidad = EXCLUDED.cantidad;
                    """)

            for cat in categorias_act:
                conn.execute(sql_pref, {
                    "id_plan": plan_id,
                    "categoria": cat,
                    "cantidad": int(cantidades_act.get(cat, 1))
                })

    return plan_id

# =========================
# Funcion para guardar y actualizar checklist
# =========================

def guardar_checklist_item_db(id_plan, seccion, item, completado):
    engine = get_engine()

    with engine.begin() as conn:
        sql = text("""
            INSERT INTO culturatrip.fact_plan_checklist (
                id_plan, seccion, item, completado, updated_at
            )
            VALUES (
                :id_plan, :seccion, :item, :completado, CURRENT_TIMESTAMP
            )
            ON CONFLICT (id_plan, seccion, item)
            DO UPDATE SET
                completado = EXCLUDED.completado,
                updated_at = CURRENT_TIMESTAMP;
        """)
        conn.execute(sql, {
            "id_plan": id_plan,
            "seccion": seccion,
            "item": item,
            "completado": completado
        })

# =========================
# Funcion para cargar checklist guardado
# =========================
@st.cache_data(show_spinner=False)
def load_checklist_plan(id_plan: int) -> pd.DataFrame:
    engine = get_engine()
    query = text("""
        SELECT id_plan, seccion, item, completado
        FROM culturatrip.fact_plan_checklist
        WHERE id_plan = :id_plan
    """)
    return pd.read_sql(query, engine, params={"id_plan": id_plan})
# =========================
# Funcion para guardar gasto real
# =========================
def guardar_gasto_real_db(id_plan, fecha, categoria, descripcion, monto):
    engine = get_engine()

    if not id_plan:
        st.error("No hay un plan seleccionado.")
        return False

    if monto is None or monto < 0:
        st.error("El monto debe ser mayor o igual a 0.")
        return False

    with engine.begin() as conn:
        sql = text("""
            INSERT INTO culturatrip.fact_plan_gasto_real (
                id_plan, fecha, categoria, descripcion, monto
            )
            VALUES (
                :id_plan, :fecha, :categoria, :descripcion, :monto
            );
        """)
        conn.execute(sql, {
            "id_plan": id_plan,
            "fecha": fecha,
            "categoria": categoria,
            "descripcion": descripcion,
            "monto": monto
        })

    return True

# =========================
# Funcion para Identificacion de Temporada
# =========================
def obtener_temporada_por_fecha(fecha, df_temporada):
    if fecha is None:
        return None

    mes = fecha.month
    anio = fecha.year

    row = df_temporada[
        (df_temporada["anio"] == anio) &
        (df_temporada["mes"] == mes)
    ]

    if row.empty:
        return None

    return row["temporada"].iloc[0]

# =========================
# Loader para DB dim_pais
# =========================
@st.cache_data(show_spinner=False)
def load_dim_pais_db() -> pd.DataFrame:
    engine = get_engine()
    query = text("SELECT id_pais, pais, lat, lon FROM culturatrip.dim_pais ORDER BY pais;")
    return pd.read_sql(query, engine)

# =========================
# Loader Generico para nuevas y proximas vistas
# =========================

@st.cache_data(show_spinner=False)
def load_view(view_name: str) -> pd.DataFrame:
    engine = get_engine()
    q = text(f"SELECT * FROM culturatrip.{view_name};")
    return pd.read_sql(q, engine)

# =========================
# Loader modelo ML avanzado
# =========================
@st.cache_resource(show_spinner=False)
def load_modelo_avanzado():
    base_dir = Path(__file__).resolve().parents[2]
    model_path = base_dir / "outputs" / "regresion_precios" /"modelos"/ "modelo_avanzado_ridge_score_final.pkl"
    features_path = base_dir / "outputs" / "regresion_precios" /"modelos"/ "features_modelo_avanzado_ridge.pkl"

    modelo = joblib.load(model_path)

    with open(features_path, "rb") as f:
        features = joblib.load(f) if str(features_path).endswith(".joblib") else None

    # fallback seguro para pickle clásico
    if features is None:
        import pickle
        with open(features_path, "rb") as f:
            features = pickle.load(f)

    return modelo, features

# ===============================
# Loader modelo ML no supervisado_Pantalla_2
# ===============================
@st.cache_resource(show_spinner=False)
def load_modelo_no_supervisado():

    base_dir = Path(__file__).resolve().parents[2]

    scaler_path = base_dir /"outputs" / "regresion_precios" /"modelos"/ "scaler.pkl"
    kmeans_path = base_dir / "outputs" / "regresion_precios" /"modelos"/ "kmeans_model.pkl"

    # ===============================
    # Cargar scaler
    # ===============================
    scaler = joblib.load(scaler_path)

    # fallback seguro
    if scaler is None:
        import pickle
        with open(scaler_path, "rb") as f:
            scaler = pickle.load(f)

    # ===============================
    # Cargar kmeans
    # ===============================
    kmeans = joblib.load(kmeans_path)

    # fallback seguro
    if kmeans is None:
        import pickle
        with open(kmeans_path, "rb") as f:
            kmeans = pickle.load(f)

    return scaler, kmeans

# ===============================
# Loader modelo ML supervisado alojamiento_Pantalla_3
# ===============================
@st.cache_resource(show_spinner=False)
def load_modelo_alojamiento():
    base_dir = Path(__file__).resolve().parents[2]

    model_path = base_dir / "outputs" / "regresion_precios" / "modelos" / "random_forest_precio.pkl"
    features_path = base_dir / "outputs" / "regresion_precios" / "modelos" / "features_modelo_precio.pkl"

    modelo = joblib.load(model_path)

    # fallback seguro para features
    features = None
    try:
        with open(features_path, "rb") as f:
            features = joblib.load(f) if str(features_path).endswith(".joblib") else None
    except Exception:
        features = None

    if features is None:
        import pickle
        with open(features_path, "rb") as f:
            features = pickle.load(f)

    return modelo, features


# =========================
# Branding CulturaTrip · Logo SVG embebido
# (se codifica en base64 para inyectarlo inline sin archivos externos)
# =========================
import base64 as _b64
LOGO_SVG = (
    '<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 140 150" width="110" height="118">'
    '<path d="M8 128 L132 128 L118 146 L22 146 Z" fill="#FFDE59" stroke="#3DCD00" stroke-width="3" stroke-linejoin="round"/>'
    '<path d="M30 138 Q70 132 110 138" stroke="#3DCD00" stroke-width="2" fill="none" stroke-dasharray="3 3"/>'
    '<path d="M70 8 C47 8 30 25 30 48 C30 72 70 128 70 128 C70 128 110 72 110 48 C110 25 93 8 70 8 Z" fill="#004AAD"/>'
    '<circle cx="70" cy="48" r="26" fill="#38B6FF"/>'
    '<path d="M55 40 Q63 36 72 40 Q78 45 74 52 Q66 56 58 53 Q52 48 55 40 Z" fill="#4BFA00"/>'
    '<path d="M78 55 Q84 54 87 58 Q86 63 80 63 Q76 60 78 55 Z" fill="#4BFA00"/>'
    '<ellipse cx="70" cy="48" rx="26" ry="26" fill="none" stroke="#004AAD" stroke-width="1.5"/>'
    '<ellipse cx="70" cy="48" rx="10" ry="26" fill="none" stroke="#004AAD" stroke-width="1.5"/>'
    '<line x1="44" y1="48" x2="96" y2="48" stroke="#004AAD" stroke-width="1.5"/>'
    '</svg>'
)
LOGO_B64 = _b64.b64encode(LOGO_SVG.strip().encode("utf-8")).decode("utf-8")
LOGO_URI = f"data:image/svg+xml;base64,{LOGO_B64}"
# =========================
# Configuración de la página
# =========================
st.set_page_config(page_title="CulturaTrip",
                   layout="wide",
                   initial_sidebar_state="expanded",
                   page_icon=LOGO_URI,
                   )
# =========================
# Estilos generales · Branding CulturaTrip
# Paleta oficial:
#   #004AAD azul oscuro · #38B6FF azul claro · #4BFA00 verde lima
#   #FFDE59 amarillo   · #3DCD00 verde hoja
# Tipografía: Now (comercial) → sustituto libre: Nunito (Google Fonts)
# =========================
st.markdown(
    """
    <style>
    /* ---------- Tipografía ---------- */
    @import url('https://fonts.googleapis.com/css2?family=Nunito:wght@400;600;700;800;900&display=swap');

    /* ---------- Variables de marca ---------- */
    :root {
        --ct-azul-oscuro:  #004AAD;
        --ct-azul-claro:   #38B6FF;
        --ct-verde-lima:   #4BFA00;
        --ct-amarillo:     #FFDE59;
        --ct-verde-hoja:   #3DCD00;

        --ct-azul-tint:    #E6F4FF;
        --ct-amarillo-tint:#FFF7D6;
        --ct-verde-tint:   #E8FBDD;

        --ct-texto:        #0B2B55;
        --ct-texto-suave:  #5A6B85;
        --ct-borde:        #D7E3F4;

        --ct-font: 'Nunito', 'Now', system-ui, -apple-system, sans-serif;
    }

    /* ---------- Tipografía global ---------- */
    html, body, [class*="css"], .stApp, .stMarkdown, .stButton, .stTextInput,
    .stSelectbox, .stDateInput, .stRadio, .stCheckbox, .stMetric, .stTabs,
    .stSidebar, .stCaption {
        font-family: var(--ct-font) !important;
        color: var(--ct-texto);
    }

    /* ---------- Contenedor central ---------- */
    .main-container {
        max-width: 1100px;
        margin: auto;
    }

    /* ---------- Badge superior ---------- */
    .badge {
        display: inline-block;
        padding: 8px 16px;
        border-radius: 999px;
        background: var(--ct-azul-tint);
        color: var(--ct-azul-oscuro);
        font-weight: 700;
        font-size: 14px;
        letter-spacing: 0.3px;
        border: 1px solid rgba(56, 182, 255, 0.3);
    }

    /* ---------- Header con logo + título ---------- */
    .header-brand {
        display: flex;
        align-items: center;
        justify-content: center;
        gap: 18px;
        margin-top: 14px;
    }
    .header-brand img {
        height: 110px;
        width: auto;
    }
    .title-main {
        font-size: 92px;
        font-weight: 900;
        color: var(--ct-azul-oscuro);
        line-height: 1;
        letter-spacing: -2px;
        margin: 0;
    }

    /* ---------- Subtítulo ---------- */
    .subtitle-main {
        text-align: center;
        color: var(--ct-texto-suave);
        font-size: 22px;
        font-weight: 600;
        margin-bottom: 30px;
    }

    /* ---------- Cards ---------- */
    .card {
        background: white;
        border-radius: 18px;
        padding: 24px;
        text-align: center;
        box-shadow: 0 10px 30px rgba(0, 74, 173, 0.08);
        border: 1px solid var(--ct-borde);
    }
    .card-title {
        font-size: 40px;
        font-weight: 800;
        color: var(--ct-azul-oscuro);
        margin-bottom: 10px;
    }
    .card-text {
        color: var(--ct-texto-suave);
        font-size: 18px;
        line-height: 1.6;
    }

    /* ---------- Info-box azul (reemplaza fondos grises) ---------- */
    .info-box {
        padding: 22px;
        font-size: 18px;
        border-radius: 14px;
        background: var(--ct-azul-tint);
        border-left: 4px solid var(--ct-azul-claro);
        color: var(--ct-texto);
    }

    /* ---------- Botones ---------- */
    .stButton > button {
        background: var(--ct-azul-oscuro);
        color: white;
        border: none;
        border-radius: 10px;
        padding: 10px 20px;
        font-weight: 700;
        font-size: 15px;
        transition: all 0.2s ease;
        box-shadow: 0 4px 12px rgba(0, 74, 173, 0.2);
    }
    .stButton > button:hover {
        background: var(--ct-azul-claro);
        color: white;
        transform: translateY(-1px);
        box-shadow: 0 6px 18px rgba(56, 182, 255, 0.35);
    }
    .stButton > button:active,
    .stButton > button:focus {
        background: var(--ct-azul-oscuro);
        color: white;
        outline: none;
    }
    /* CTA primario en verde (uso: st.button("...", type="primary")) */
    .stButton > button[kind="primary"] {
        background: var(--ct-verde-hoja);
        color: white;
    }
    .stButton > button[kind="primary"]:hover {
        background: var(--ct-verde-lima);
        color: var(--ct-texto);
    }

    /* ---------- Inputs ---------- */
    div[data-baseweb="select"] > div {
        font-size: 16px !important;
        min-height: 44px;
        border-radius: 10px !important;
        border: 1.5px solid var(--ct-borde) !important;
    }
    div[data-baseweb="select"] > div:hover {
        border-color: var(--ct-azul-claro) !important;
    }
    .stDateInput > div > div > input,
    .stTextInput > div > div > input,
    .stNumberInput > div > div > input {
        border-radius: 10px !important;
        border: 1.5px solid var(--ct-borde) !important;
        font-size: 16px !important;
    }
    .stDateInput > div > div > input:focus,
    .stTextInput > div > div > input:focus,
    .stNumberInput > div > div > input:focus {
        border-color: var(--ct-azul-claro) !important;
        box-shadow: 0 0 0 3px rgba(56, 182, 255, 0.15) !important;
    }

    /* ---------- Labels ---------- */
    label, .stMarkdown label {
        font-size: 13px !important;
        font-weight: 700 !important;
        color: var(--ct-azul-oscuro) !important;
        text-transform: uppercase;
        letter-spacing: 0.3px;
    }

    /* ---------- Métricas ---------- */
    [data-testid="stMetric"] {
        background: white;
        border: 1px solid var(--ct-borde);
        border-radius: 14px;
        padding: 16px 18px;
        box-shadow: 0 4px 14px rgba(0, 74, 173, 0.06);
    }
    [data-testid="stMetricLabel"] {
        color: var(--ct-texto-suave) !important;
        font-weight: 700 !important;
        font-size: 13px !important;
        text-transform: uppercase;
        letter-spacing: 0.4px;
    }
    [data-testid="stMetricValue"] {
        color: var(--ct-azul-oscuro) !important;
        font-weight: 900 !important;
    }

    /* ---------- Sidebar ---------- */
    [data-testid="stSidebar"] {
        background: linear-gradient(180deg, var(--ct-azul-oscuro) 0%, #003580 100%);
    }
    [data-testid="stSidebar"] * {
        color: white !important;
    }
    [data-testid="stSidebar"] h1 {
        color: white !important;
        font-weight: 900;
        font-size: 30px;
        letter-spacing: -1px;
    }
    [data-testid="stSidebar"] .stRadio label {
        color: white !important;
        font-size: 15px !important;
        text-transform: none;
        letter-spacing: normal;
    }
    [data-testid="stSidebar"] .stButton > button {
        background: var(--ct-verde-hoja);
        color: white;
        border: none;
    }
    [data-testid="stSidebar"] .stButton > button:hover {
        background: var(--ct-verde-lima);
        color: var(--ct-texto);
    }
    [data-testid="stSidebar"] hr {
        border-color: rgba(255, 255, 255, 0.2) !important;
    }

    /* ---------- Alerts ---------- */
    div[data-testid="stAlert"] {
        border-radius: 12px !important;
        border-left-width: 5px !important;
        font-weight: 600;
    }
    div[data-baseweb="notification"][kind="positive"],
    div[data-testid="stAlert"][data-baseweb-kind="success"] {
        background: var(--ct-verde-tint) !important;
        border-left-color: var(--ct-verde-hoja) !important;
    }
    div[data-baseweb="notification"][kind="warning"],
    div[data-testid="stAlert"][data-baseweb-kind="warning"] {
        background: var(--ct-amarillo-tint) !important;
        border-left-color: var(--ct-amarillo) !important;
    }
    div[data-baseweb="notification"][kind="info"],
    div[data-testid="stAlert"][data-baseweb-kind="info"] {
        background: var(--ct-azul-tint) !important;
        border-left-color: var(--ct-azul-claro) !important;
    }

    /* ---------- Progress bar ---------- */
    .stProgress > div > div > div > div {
        background: linear-gradient(90deg, var(--ct-azul-claro), var(--ct-verde-hoja)) !important;
    }

    /* ---------- Tabs ---------- */
    .stTabs [data-baseweb="tab-list"] {
        gap: 4px;
        border-bottom: 2px solid var(--ct-borde);
    }
    .stTabs [data-baseweb="tab"] {
        font-weight: 700;
        color: var(--ct-texto-suave);
        padding: 10px 18px;
    }
    .stTabs [aria-selected="true"] {
        color: var(--ct-azul-oscuro) !important;
        border-bottom: 3px solid var(--ct-azul-oscuro) !important;
    }

    /* ---------- Dividers ---------- */
    hr {
        border-color: var(--ct-borde) !important;
    }

    /* ---------- Títulos ---------- */
    h1, h2, h3 {
        color: var(--ct-azul-oscuro) !important;
        font-weight: 800 !important;
    }

    </style>
    """,
    unsafe_allow_html=True
)

# =========================
# Card Bienvenidos
# =========================

st.markdown(
    """
    <div class="card">
        <div class="card-title">Bienvenidos</div>
        <div class="card-text">
        Descubre más allá de los destinos tradicionales. 
        Diseña tu viaje ideal combinando cultura, presupuesto y experiencias locales.
        </div>
    </div>
    """,
    unsafe_allow_html=True
)

# botón centrado
col1, col2, col3 = st.columns([3,2,3])
with col2:
    st.button("Conoce Datos Curiosos del Destino a Visitar", use_container_width=True)

st.markdown("<br>", unsafe_allow_html=True)


# ===============================
# Nuevas Rutas PostgreSQL DB
# ===============================

# Views para la pantalla_1
df_dropdown_paises = load_view("vw_ui_dropdown_paises")
df_pantalla1_global = load_view("vw_ui_pantalla1_global")
df_pantalla1_detalle = load_view("vw_ui_pantalla1_detalle_por_pais")
df_total_paises = load_view("vw_ui_total_paises")

# Views para la pantalla_2
df_dropdown_provincias = load_view("vw_ui_dropdown_provincias_por_pais")
df_dropdown_cat_aloj = load_view("vw_ui_dropdown_categoria_alojamiento")
df_dropdown_cat_act = load_view("vw_ui_dropdown_categoria_actividad")

df_rec_act = load_view("vw_rec_actividades_por_provincia")
df_rec_aloj = load_view("vw_rec_alojamiento_precio_provincia")

# Views de Machine Learning pantalla_2
df_ml_avanzado_base = load_view("vw_ml_avanzado_base_provincia")
df_ml_no_supervisado_base = load_view("vw_ml_no_supervisado_base_provincia")

# Views para la pantalla_3 y pantalla_4
df_plan_resumen = load_view("vw_plan_resumen_basico")
df_plan_costos = load_view("vw_plan_costos_estimados")
df_plan_presupuesto_cat = load_view("vw_plan_presupuesto_categoria")
df_temporada_mes = load_view("vw_temporada_por_mes")
df_ml_alojamiento_features = load_view("vw_ml_alojamiento_features_plan")


# Views para la pantalla_5
df_gasto_resumen = load_view("vw_plan_gasto_real_resumen")
df_gasto_categoria = load_view("vw_plan_gasto_real_por_categoria")
df_gasto_detalle = load_view("vw_plan_gasto_real_detalle")

# ===============================
# Rutas a datasets finales (data/clean)-> Temporal migrar a views de POSTGRESQL
# ===============================

BASE_DIR = Path(__file__).resolve().parents[2]
# DIM_PAIS_PATH = BASE_DIR / "data" / "clean" / "dim_pais.csv"->desactivada
DIM_MUNICIPIO_PATH = BASE_DIR / "data" / "clean" / "dim_municipio_final.csv"
DIM_GEO_MUNI_OSM_PATH = BASE_DIR / "data" / "clean" / "dim_geografia_municipio_osm.csv"

FACT_ACTIVIDADES_PATH = BASE_DIR / "data" / "clean" / "fact_actividades_provincia_enriquecida.csv"
ALOJAMIENTOS_PATH = BASE_DIR / "data" / "clean" / "df_alojamientos.csv"

@st.cache_data(show_spinner=False)
def load_csv(path: Path) -> pd.DataFrame:
    return pd.read_csv(path)

# Cargar datasets-> esto tambien se va a remplazar
df_paises = load_dim_pais_db()
df_muni = load_csv(DIM_MUNICIPIO_PATH)
df_geo = load_csv(DIM_GEO_MUNI_OSM_PATH)

df_actividades = load_csv(FACT_ACTIVIDADES_PATH)
df_alojamientos = load_csv(ALOJAMIENTOS_PATH)

# Merge para tener lat/lon a nivel municipio
df_divgeo = df_muni.merge(df_geo[["id_municipio", "lat", "lon"]], on="id_municipio", how="left")

# ===============================
# Sistema de autenticación simple
# ===============================
import hashlib

def _hash_password(password: str) -> str:
    """Hash SHA-256 con salt fijo del proyecto."""
    salt = "CulturaTrip2026"
    return hashlib.sha256(f"{salt}{password}".encode("utf-8")).hexdigest()

def _crear_tabla_usuarios():
    """Crea la tabla de usuarios si no existe."""
    engine = get_engine()
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS usuarios (
                email VARCHAR(255) PRIMARY KEY,
                nombre VARCHAR(255) NOT NULL,
                password_hash VARCHAR(255) NOT NULL,
                fecha_registro TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """))

def _registrar_usuario(email: str, nombre: str, password: str) -> tuple:
    """Registra un usuario. Retorna (ok: bool, mensaje: str)."""
    email = email.strip().lower()
    if not email or not nombre.strip() or not password:
        return False, "Todos los campos son obligatorios."
    if len(password) < 4:
        return False, "La contraseña debe tener al menos 4 caracteres."
    engine = get_engine()
    try:
        with engine.begin() as conn:
            existe = conn.execute(
                text("SELECT 1 FROM usuarios WHERE email = :email"),
                {"email": email}
            ).fetchone()
            if existe:
                return False, "Ya existe una cuenta con ese correo."
            conn.execute(
                text("INSERT INTO usuarios (email, nombre, password_hash) VALUES (:email, :nombre, :hash)"),
                {"email": email, "nombre": nombre, "hash": _hash_password(password)}
            )
        return True, "Cuenta creada con éxito. Ya puedes iniciar sesión."
    except Exception as e:
        return False, f"Error al registrar: {e}"

def _autenticar_usuario(email: str, password: str) -> tuple:
    """Verifica credenciales. Retorna (ok: bool, nombre: str o mensaje_error: str)."""
    email = email.strip().lower()
    if not email or not password:
        return False, "Ingresa correo y contraseña."
    engine = get_engine()
    try:
        with engine.begin() as conn:
            row = conn.execute(
                text("SELECT nombre, password_hash FROM usuarios WHERE email = :email"),
                {"email": email}
            ).fetchone()
            if not row:
                return False, "No existe una cuenta con ese correo."
            if row[1] != _hash_password(password):
                return False, "Contraseña incorrecta."
            return True, row[0]
    except Exception as e:
        return False, f"Error de autenticación: {e}"

# Crear tabla al iniciar (si no existe)
_crear_tabla_usuarios()

# --- Pantalla de Login ---
if "autenticado" not in st.session_state:
    st.session_state["autenticado"] = False
    st.session_state["usuario_nombre"] = ""

if not st.session_state["autenticado"]:
    # Centrar el formulario
    _col_l, _col_c, _col_r = st.columns([1, 2, 1])
    with _col_c:

        st.markdown(
            '<h1 style="text-align:center; color:#004AAD; font-weight:900; '
            'font-size:48px; margin:10px 0 4px;">CulturaTrip</h1>',
            unsafe_allow_html=True,
        )
        st.markdown(
            '<p style="text-align:center; color:#5A6B85; font-size:16px; margin-bottom:28px;">'
            'Planificación inteligente de turismo</p>',
            unsafe_allow_html=True,
        )

        tab_login, tab_registro = st.tabs(["Iniciar sesión", "Crear cuenta"])

        with tab_login:
            with st.form("form_login"):
                email_login = st.text_input("Correo electrónico", placeholder="tu@email.com")
                pass_login = st.text_input("Contraseña", type="password", placeholder="••••••••")
                btn_login = st.form_submit_button("Iniciar sesión", use_container_width=True, type="primary")

            if btn_login:
                ok, resultado = _autenticar_usuario(email_login, pass_login)
                if ok:
                    # Limpiar toda la sesión anterior antes de entrar
                    keys_to_keep = {"autenticado", "email", "usuario_nombre"}
                    for key in list(st.session_state.keys()):
                        if key not in keys_to_keep:
                            del st.session_state[key]
                    st.session_state["autenticado"] = True
                    st.session_state["email"] = email_login.strip().lower()
                    st.session_state["usuario_nombre"] = resultado
                    st.session_state["step"] = 1
                    st.session_state["menu"] = "Exploración Cultural"
                    st.rerun()
                else:
                    st.error(resultado)

        with tab_registro:
            with st.form("form_registro"):
                nombre_reg = st.text_input("Nombre completo", placeholder="Tu nombre")
                email_reg = st.text_input("Correo electrónico", placeholder="tu@email.com")
                pass_reg = st.text_input("Contraseña", type="password", placeholder="Mínimo 4 caracteres")
                pass_reg2 = st.text_input("Confirmar contraseña", type="password", placeholder="Repite la contraseña")
                btn_registro = st.form_submit_button("Crear cuenta", use_container_width=True)

            if btn_registro:
                if pass_reg != pass_reg2:
                    st.error("Las contraseñas no coinciden.")
                else:
                    ok, msg = _registrar_usuario(email_reg, nombre_reg, pass_reg)
                    if ok:
                        st.success(msg)
                    else:
                        st.error(msg)

    st.stop()  # Bloquea toda la app hasta autenticarse

# =========================
# Header global (visible en TODAS las pantallas, solo si autenticado)
# =========================
st.markdown('<div class="main-container">', unsafe_allow_html=True)
st.markdown(
    f'<div class="header-brand">{LOGO_SVG}<h1 class="title-main">CulturaTrip</h1></div>',
    unsafe_allow_html=True
)
st.markdown(
    '<div class="subtitle-main">Planificación inteligente de turismo</div>',
    unsafe_allow_html=True
)

# ===============================
# Session State (defaults)
# ===============================
def init_state():
    defaults = {
        # Navegación (si sigues con step)
        "step": 1,
        "menu": "Exploración Cultural",

        # País destino (desde Pantalla 1)
        "pais": None,            # nombre UI (ej: "España")
        "id_pais": None,         # código (ej: "ES")
        "pais_ui": None,         # selectbox key

        # País origen (Pantalla 2)
        "pais_origen": None,        # nombre UI
        "id_pais_origen": None,     # código

        # Provincia seleccionada por la usuaria
        "provincia_destino": None,
        "id_provincia_destino": None,

        # Provincia recomendada por ML avanzado
        "provincia_ml_recomendada": None,
        "id_provincia_ml_recomendada": None,

        # Usuario/correo
        "email": "",

        # Fechas
        "fecha_ida": None,
        "fecha_regreso": None,

        # Presupuesto
        "presupuesto": 0,

        # Selecciones
        "categoria_alojamiento": None,
        # 🔹 antigua (mantener temporalmente)
        "categoria_actividad": None,
        # 🔹 nueva multiselección
        "categorias_actividad": [],
        "cantidades_actividad": {},

        "tipo_viaje": "Solo",

        "guardando": False,
        "plan_guardado": False,
        "ultimo_plan_id": None,

        "perfil_presupuesto": "standard",

        # NUEVO: multi-plan
        "plan_seleccionado": None,
        "modo_edicion_plan": False,

    }
    for k, v in defaults.items():
        if k not in st.session_state:
            st.session_state[k] = v

init_state()


# ===============================
# Reset State
# ===============================
def reset_plan_completo():
    keys_to_reset = {
        # Navegación / selección base
        "pais": None,
        "id_pais": None,
        "pais_ui": None,
        "pais_origen": None,
        "id_pais_origen": None,

        # Destino
        "provincia_destino": None,
        "id_provincia_destino": None,

        # ML
        "provincia_ml_recomendada": None,
        "id_provincia_ml_recomendada": None,

        # Usuario
        "email": "",

        # Fechas
        "fecha_ida": None,
        "fecha_regreso": None,

        # Presupuesto y preferencias
        "presupuesto": 0,
        "categoria_alojamiento": None,
        "categorias_actividad": None,
        "tipo_viaje": "Solo",
        "perfil_presupuesto": "standard",

        # Control de guardado
        "plan_guardado": False,
        "guardando": False,
        "ultimo_plan_id": None,

        # NUEVO: multi-plan
        "plan_seleccionado": None,
        "modo_edicion_plan": False,
    }

    for k, v in keys_to_reset.items():
        st.session_state[k] = v

# ===============================
# Iniciar Nuevo Plan
# ===============================

def iniciar_nuevo_plan():
    email_actual = st.session_state.get("email", "")

    # limpiar formulario y contexto del plan activo
    st.session_state["pais"] = None
    st.session_state["id_pais"] = None
    st.session_state["pais_ui"] = None

    st.session_state["pais_origen"] = None
    st.session_state["id_pais_origen"] = None

    st.session_state["provincia_destino"] = None
    st.session_state["id_provincia_destino"] = None

    st.session_state["provincia_ml_recomendada"] = None
    st.session_state["id_provincia_ml_recomendada"] = None

    st.session_state["fecha_ida"] = None
    st.session_state["fecha_regreso"] = None

    st.session_state["presupuesto"] = 0

    st.session_state["categoria_alojamiento"] = None
    st.session_state["categoria_actividad"] = None
    st.session_state["categorias_actividad"] = []
    st.session_state["cantidades_actividad"] = {}

    st.session_state["tipo_viaje"] = "Solo"
    st.session_state["perfil_presupuesto"] = "standard"

    st.session_state["guardando"] = False
    st.session_state["plan_guardado"] = False
    st.session_state["ultimo_plan_id"] = None

    # MUY IMPORTANTE
    st.session_state["plan_seleccionado"] = None
    st.session_state["modo_edicion_plan"] = False

    # conservar email si quieres seguir viendo los planes del mismo usuario
    st.session_state["email"] = email_actual

# ===============================
# Sidebar navegación (nuevo layout)
# ===============================
with st.sidebar:
    # Logo pequeño en el sidebar (sin duplicar el header principal)
    st.markdown(
        f'<div style="text-align:center; padding:8px 0 4px;">'
        f'<div style="display:inline-flex; align-items:center; justify-content:center; '
        f'width:70px; height:70px; border-radius:50%; background:white; '
        f'box-shadow:0 2px 10px rgba(0,0,0,0.15); margin-bottom:6px;">'
        f'<div style="transform:scale(0.4); transform-origin:center; line-height:0;">{LOGO_SVG}</div>'
        f'</div>'
        f'<div style="font-size:22px; font-weight:900; color:white;">CulturaTrip</div>'
        f'</div>',
        unsafe_allow_html=True,
    )

    # Saludo + cerrar sesión
    nombre_usuario = st.session_state.get("usuario_nombre", "")
    if nombre_usuario:
        st.markdown(
            f'<div style="padding:8px 12px; border-radius:10px; '
            f'background:rgba(255,255,255,0.15); margin-bottom:10px; font-size:13px;">'
            f'👋 Hola, <b>{nombre_usuario}</b></div>',
            unsafe_allow_html=True,
        )
    if st.button("🚪 Cerrar sesión", use_container_width=True):
        # Limpiar toda la sesión
        for key in list(st.session_state.keys()):
            del st.session_state[key]
        st.session_state["autenticado"] = False
        st.rerun()

    st.divider()

    opciones = [
        "Exploración Cultural",
        "Gestión de planes",
        "Planificación",
        "Itinerario",
        "Presupuesto",
        "Control de Gastos",
        "Checklist",
        "Resumen final",
        "Nuestro Equipo",
    ]

    # Mantiene sincronía con step (1..7)
    default_index = max(0, min(len(opciones) - 1, st.session_state.step - 1))

    menu = st.radio("Menú", opciones, index=default_index)

    st.session_state.menu = menu
    st.session_state.step = opciones.index(menu) + 1  # ✅ actualiza step para tu router viejo

    st.divider()
    st.markdown("### Acciones rápidas")
    st.caption("Comienza una nueva planificación desde cero")
    if st.button("🆕 Nuevo plan", use_container_width=True):
        iniciar_nuevo_plan()
        st.session_state["step"] = 3  # ir a Planificación
        st.rerun()

# ===============================
# Helpers
# ===============================
def pais_display(nombre_en_dim_pais: str) -> str:
    """Normaliza el nombre para UI (title) y aplica overrides puntuales."""
    if not isinstance(nombre_en_dim_pais, str):
        return nombre_en_dim_pais

    base = nombre_en_dim_pais.strip().title()

    overrides = {
        "Spain": "España",
        "Italy": "Italia",
        "France": "Francia",
        "Costa Rica": "Costa Rica",
        "United States": "Estados Unidos",
        "United Kingdom": "Reino Unido",
        "Dominican Republic": "República Dominicana",
        "Czech Republic": "República Checa",
    }
    return overrides.get(base, base)
# ===============================
# Preparamos lista UI + mapa nombre -> id_pais
# ===============================

df_dropdown_paises["pais_ui"] = df_dropdown_paises["pais"].apply(pais_display)

df_paises_ui = (
    df_dropdown_paises[["pais_ui", "id_pais"]]
    .dropna()
    .drop_duplicates(subset=["pais_ui"])
)

map_paisui_a_id = dict(zip(df_paises_ui["pais_ui"], df_paises_ui["id_pais"]))
lista_paises_ui = sorted(df_paises_ui["pais_ui"].tolist())

# =========================
# Helpers ML avanzado pantalla_2
# Funciones auxiliares para el modelo
# =========================
def calcular_noches_tipo_dia(fecha_ida, fecha_regreso):
    """
    fecha_regreso debe ser posterior a fecha_ida.
    Cuenta noches entre fecha_ida y fecha_regreso - 1.
    Viernes=4, sábado=5 como noches fin de semana.
    """
    if fecha_ida is None or fecha_regreso is None or fecha_regreso <= fecha_ida:
        return 0, 0, 0

    fechas_noche = pd.date_range(start=fecha_ida, end=fecha_regreso - pd.Timedelta(days=1), freq="D")
    noches_totales = len(fechas_noche)
    noches_fin_semana = int(sum(f.weekday() in [4, 5] for f in fechas_noche))
    noches_semana = noches_totales - noches_fin_semana

    return noches_totales, noches_semana, noches_fin_semana


def construir_features_modelo_avanzado(
    df_base: pd.DataFrame,
    id_pais_destino: str,
    fecha_ida,
    fecha_regreso,
    presupuesto_usuario: float,
    feature_names: list
) -> pd.DataFrame:
    """
    Construye las 4 features del modelo avanzado para cada provincia candidata.
    """

    if df_base.empty or not id_pais_destino:
        return pd.DataFrame()

    candidatos = df_base[df_base["id_pais"] == id_pais_destino].copy()

    if candidatos.empty:
        return pd.DataFrame()

    noches_totales, noches_semana, noches_fin_semana = calcular_noches_tipo_dia(
        pd.to_datetime(fecha_ida),
        pd.to_datetime(fecha_regreso)
    )

    dias_viaje = (pd.to_datetime(fecha_regreso) - pd.to_datetime(fecha_ida)).days + 1
    dias_viaje = max(dias_viaje, 1)

    # Estimación de alojamiento por provincia
    candidatos["alojamiento_estimado"] = (
        candidatos["precio_noche_semana"].fillna(0) * noches_semana
        + candidatos["precio_noche_fin_semana"].fillna(0) * noches_fin_semana
    ).round(2)

    # Para este modelo, como no hay input de categorías, usamos una aproximación base:
    # número de actividades sugeridas = min(días de viaje, n_actividades disponibles)
    candidatos["n_actividades_modelo"] = candidatos["n_actividades"].fillna(0).astype(int).clip(lower=0)
    candidatos["n_actividades_modelo"] = candidatos["n_actividades_modelo"].apply(
        lambda x: min(x, dias_viaje)
    )

    # Costo base de actividades
    candidatos["actividades_estimado"] = (
        candidatos["precio_actividad_promedio"].fillna(0)
        * candidatos["n_actividades_modelo"]
    ).round(2)

    # Componentes del presupuesto por perfil standard
    candidatos["transporte_estimado"] = (presupuesto_usuario * candidatos["pct_transporte"].fillna(0)).round(2)
    candidatos["alimentacion_estimado"] = (presupuesto_usuario * candidatos["pct_alimentacion"].fillna(0)).round(2)
    candidatos["servicios_estimado"] = (presupuesto_usuario * candidatos["pct_servicios"].fillna(0)).round(2)
    candidatos["otros_estimado"] = (presupuesto_usuario * candidatos["pct_otros"].fillna(0)).round(2)

    # Costo total estimado del viaje para cada provincia
    candidatos["costo_total_viaje"] = (
        candidatos["alojamiento_estimado"]
        + candidatos["actividades_estimado"]
        + candidatos["transporte_estimado"]
        + candidatos["alimentacion_estimado"]
        + candidatos["servicios_estimado"]
        + candidatos["otros_estimado"]
    ).round(2)

    # Ratios
    candidatos["ratio_alojamiento"] = np.where(
        candidatos["costo_total_viaje"] > 0,
        candidatos["alojamiento_estimado"] / candidatos["costo_total_viaje"],
        0
    )

    candidatos["ratio_actividades"] = np.where(
        candidatos["costo_total_viaje"] > 0,
        candidatos["actividades_estimado"] / candidatos["costo_total_viaje"],
        0
    )

    # Nombre exacto esperado por el modelo
    candidatos["n_actividades"] = candidatos["n_actividades_modelo"].astype(float)

    # DataFrame final para predicción
    X = candidatos[feature_names].copy()

    # Limpieza final
    X = X.replace([np.inf, -np.inf], 0).fillna(0)

    # Adjuntar al dataset original
    for col in feature_names:
        candidatos[col] = X[col]

    return candidatos


    # ===============================
    # TOP 5 provincias similares
    # ===============================
def generar_top5_provincias_modelo_avanzado(
    df_base: pd.DataFrame,
    id_pais_destino: str,
    fecha_ida,
    fecha_regreso,
    presupuesto_usuario: float
) -> pd.DataFrame:
    modelo, feature_names = load_modelo_avanzado()

    candidatos = construir_features_modelo_avanzado(
        df_base=df_base,
        id_pais_destino=id_pais_destino,
        fecha_ida=fecha_ida,
        fecha_regreso=fecha_regreso,
        presupuesto_usuario=presupuesto_usuario,
        feature_names=feature_names
    )

    if candidatos.empty:
        return pd.DataFrame()

    X_pred = candidatos[feature_names].copy()
    candidatos["score_modelo"] = modelo.predict(X_pred)

    # Métrica de ajuste al presupuesto
    candidatos["diferencia_presupuesto"] = (
        presupuesto_usuario - candidatos["costo_total_viaje"]
    ).round(2)

    candidatos["abs_diferencia_presupuesto"] = candidatos["diferencia_presupuesto"].abs()

    # Ranking:
    # 1) score alto
    # 2) más cerca del presupuesto
    # 3) menor costo si persiste empate
    ranking = candidatos.sort_values(
        by=["score_modelo", "abs_diferencia_presupuesto", "costo_total_viaje"],
        ascending=[False, True, True]
    ).copy()

    ranking["ranking"] = range(1, len(ranking) + 1)

    columnas_salida = [
        "ranking",
        "id_provincia",
        "provincia_nombre",
        "score_modelo",
        "costo_total_viaje",
        "diferencia_presupuesto",
        "n_actividades",
        "ratio_alojamiento",
        "ratio_actividades",
        "alojamiento_estimado",
        "actividades_estimado",
        "transporte_estimado",
        "alimentacion_estimado",
        "servicios_estimado",
        "otros_estimado",
    ]

    return ranking[columnas_salida].head(5)

def obtener_top5_provincias_similares(
    df_base: pd.DataFrame,
    id_provincia_seleccionada: str
) -> pd.DataFrame:
    """
    Devuelve top 5 provincias similares a la provincia seleccionada
    usando scaler + kmeans + similitud coseno dentro del mismo cluster.
    """

    if df_base.empty or not id_provincia_seleccionada:
        return pd.DataFrame()

    df = df_base.copy()

    # Renombrar columnas SQL al naming exacto del entrenamiento
    df = df.rename(columns={
        "cnt_comida_bebida": "cnt_comida y bebida",
        "cnt_servicios": "cnt_servicios",
        "cnt_vida_nocturna": "cnt_vida nocturna",
        "cnt_paisaje_naturaleza": "cnt_paisaje naturaleza",
        "cnt_paisaje_urbano": "cnt_paisaje urbano",
        "cnt_compras": "cnt_compras",
        "cnt_otros": "cnt_otros",
        "prop_comida_bebida": "pct_comida y bebida",
        "prop_servicios": "pct_servicios",
        "prop_vida_nocturna": "pct_vida nocturna",
        "prop_paisaje_naturaleza": "pct_paisaje naturaleza",
        "prop_paisaje_urbano": "pct_paisaje urbano",
        "prop_compras": "pct_compras",
        "prop_otros": "pct_otros"
    })

    scaler, kmeans = load_modelo_no_supervisado()

    # Tomar el orden REAL de features desde el scaler
    if hasattr(scaler, "feature_names_in_"):
        feature_cols_no_sup = list(scaler.feature_names_in_)
    else:
        raise ValueError(
            "El scaler no contiene feature_names_in_. "
            "Debes guardar también el listado de features del entrenamiento."
        )

    faltantes = [c for c in feature_cols_no_sup if c not in df.columns]
    if faltantes:
        raise ValueError(
            f"Faltan columnas requeridas por el scaler: {faltantes}"
        )

    # Asegurar mismo orden exacto
    X = df.loc[:, feature_cols_no_sup].copy()

    for col in feature_cols_no_sup:
        X[col] = pd.to_numeric(X[col], errors="coerce").fillna(0)

    X_scaled = scaler.transform(X)

    df["cluster"] = kmeans.predict(X_scaled)

    fila_origen = df[df["id_provincia"].astype(str) == str(id_provincia_seleccionada)].copy()
    if fila_origen.empty:
        return pd.DataFrame()

    cluster_origen = fila_origen.iloc[0]["cluster"]
    idx_origen = fila_origen.index[0]

    df_cluster = df[df["cluster"] == cluster_origen].copy()
    idx_cluster = df_cluster.index.tolist()

    if idx_origen not in idx_cluster:
        return pd.DataFrame()

    X_cluster = X_scaled[idx_cluster]
    pos_origen_local = idx_cluster.index(idx_origen)

    similitudes = cosine_similarity(
        X_cluster[pos_origen_local].reshape(1, -1),
        X_cluster
    )[0]

    df_cluster["score_similitud"] = similitudes

    df_cluster = df_cluster[
        df_cluster["id_provincia"].astype(str) != str(id_provincia_seleccionada)
    ].copy()

    if df_cluster.empty:
        return pd.DataFrame()

    top5 = (
        df_cluster.sort_values(
            by=["score_similitud", "provincia_nombre"],
            ascending=[False, True]
        )
        .head(5)
        .copy()
    )

    top5["ranking"] = range(1, len(top5) + 1)
    top5["score_similitud"] = top5["score_similitud"].round(4)

    return top5[
        [
            "ranking",
            "id_provincia",
            "provincia_nombre",
            "cluster",
            "score_similitud",
            "total_actividades",
            "categorias_unicas",
            "valoracion_general_promedio"
        ]
    ]
def mostrar_top5_provincias_similares(
    df_base: pd.DataFrame,
    id_provincia_seleccionada: str,
    provincia_nombre_seleccionada: str
):
    st.subheader("Si te gusta este destino, también te puede gustar")
    st.caption(
        f"Te mostramos destinos con un perfil parecido al que elegiste: {provincia_nombre_seleccionada}"
    )

    if not id_provincia_seleccionada:
        st.info("Selecciona una provincia destino para ver provincias similares.")
        return

    try:
        df_top5 = obtener_top5_provincias_similares(
            df_base=df_base,
            id_provincia_seleccionada=id_provincia_seleccionada
        )

        if df_top5.empty:
            st.info("No se encontraron provincias similares para la provincia seleccionada.")
            return

        # Colores para las medallas de ranking
        _medallas = {1: "#004AAD", 2: "#38B6FF", 3: "#3DCD00", 4: "#FFDE59", 5: "#5A6B85"}
        _medalla_iconos = {1: "🥇", 2: "🥈", 3: "🥉", 4: "4", 5: "5"}

        # Colores para tags de categoría
        _tag_colors = {
            "comida y bebida": "#FFDE59",
            "servicios": "#38B6FF",
            "vida nocturna": "#9B59B6",
            "paisaje naturaleza": "#3DCD00",
            "paisaje urbano": "#004AAD",
            "compras": "#E67E22",
            "otros": "#5A6B85",
        }

        # Máximo de similitud para escalar las barras
        max_score = float(df_top5["score_similitud"].max()) if not df_top5.empty else 1.0
        if max_score == 0:
            max_score = 1.0

        cards_html = '<div style="display:flex; flex-direction:column; gap:12px;">'

        for _, row in df_top5.iterrows():
            rank = int(row["ranking"])
            nombre = str(row["provincia_nombre"]).title()
            score = float(row["score_similitud"])
            pct = min(int((score / max_score) * 100), 100)

            # Etiqueta de compatibilidad
            if score >= 0.7:
                compat_label = "Alta"
                compat_color = "#3DCD00"
            elif score >= 0.5:
                compat_label = "Media"
                compat_color = "#FFDE59"
            else:
                compat_label = "Moderada"
                compat_color = "#38B6FF"

            # Tags de categorías
            cats_raw = row.get("categorias_unicas", "")
            tags_html = ""
            if cats_raw and str(cats_raw).strip() and str(cats_raw).strip().lower() != "nan":
                cats = [c.strip().lower() for c in str(cats_raw).split(",") if c.strip()]
                for cat in cats[:5]:
                    c_color = _tag_colors.get(cat, "#5A6B85")
                    cat_display = cat.title()
                    tags_html += (
                        f'<span style="display:inline-block; padding:2px 8px; border-radius:999px; '
                        f'font-size:10px; font-weight:700; margin:2px 3px 2px 0; '
                        f'background:{c_color}18; color:{c_color}; '
                        f'border:1px solid {c_color}30;">{cat_display}</span>'
                    )

            # Medalla
            medalla_color = _medallas.get(rank, "#5A6B85")
            if rank <= 3:
                medalla_html = f'<span style="font-size:28px;">{_medalla_iconos[rank]}</span>'
            else:
                medalla_html = (
                    f'<span style="display:inline-flex; align-items:center; justify-content:center; '
                    f'width:32px; height:32px; border-radius:50%; '
                    f'background:{medalla_color}20; color:{medalla_color}; '
                    f'font-size:15px; font-weight:800;">{rank}</span>'
                )

            cards_html += (
                f'<div style="display:flex; align-items:center; gap:16px; '
                f'background:white; border-radius:14px; padding:16px 20px; '
                f'border:1px solid var(--ct-borde, #D7E3F4); '
                f'border-left:5px solid {medalla_color}; '
                f'box-shadow:0 2px 10px rgba(0,74,173,0.05);">'

                # Medalla
                f'<div style="flex-shrink:0; width:40px; text-align:center;">{medalla_html}</div>'

                # Contenido principal
                f'<div style="flex:1; min-width:0;">'

                # Nombre + compatibilidad
                f'<div style="display:flex; align-items:center; gap:10px; margin-bottom:6px;">'
                f'<span style="font-size:17px; font-weight:800; color:var(--ct-azul-oscuro, #004AAD);">{nombre}</span>'
                f'<span style="padding:2px 8px; border-radius:999px; font-size:10px; font-weight:700; '
                f'background:{compat_color}20; color:{compat_color};">{compat_label} compatibilidad</span>'
                f'</div>'

                # Barra de compatibilidad
                f'<div style="width:100%; height:8px; border-radius:4px; background:var(--ct-borde, #D7E3F4); '
                f'margin-bottom:8px; overflow:hidden;">'
                f'<div style="width:{pct}%; height:100%; border-radius:4px; '
                f'background:linear-gradient(90deg, {medalla_color}, {compat_color}); '
                f'transition:width 0.5s ease;"></div></div>'

                # Tags de categorías
                f'<div style="display:flex; flex-wrap:wrap;">{tags_html}</div>'

                f'</div>'
                f'</div>'
            )

        cards_html += '</div>'
        st.markdown(cards_html, unsafe_allow_html=True)

    except Exception as e:
        st.warning(f"No fue posible calcular provincias similares: {e}")

# =========================
# Helper para formato moneda
# =========================
def format_eur(valor):
    try:
        return f"€{float(valor):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    except Exception:
        return "€0,00"
# =========================
# Helper para pantalla_3
# Predicción ML alojamiento por plan
# =========================
def predecir_costo_alojamiento_ml(id_plan: int, df_features_plan: pd.DataFrame) -> dict:
    resultado_default = {
        "ok": False,
        "presupuesto_alojamiento_ine": 0.0,
        "precio_noche_semana_ml": 0.0,
        "precio_noche_fin_semana_ml": 0.0,
        "alojamiento_ml_estimado": 0.0,
        "diferencia_alojamiento_ml": 0.0,
        "alcanza_alojamiento": False,
        "mensaje": "No fue posible calcular la predicción ML."
    }

    if df_features_plan is None or df_features_plan.empty:
        resultado_default["mensaje"] = "La vista de features ML está vacía."
        return resultado_default

    fila = df_features_plan[df_features_plan["id_plan"] == id_plan].copy()
    if fila.empty:
        resultado_default["mensaje"] = f"No existe información ML para el plan {id_plan}."
        return resultado_default

    fila = fila.iloc[0]

    try:
        modelo, feature_cols = load_modelo_alojamiento()

        if feature_cols is None:
            resultado_default["mensaje"] = "No se pudo cargar el listado de features del modelo."
            return resultado_default

        # Normalizar si viene como numpy array / Index / lista
        feature_cols = list(feature_cols)

        base_data = {
            "id_ccaa": fila.get("id_ccaa"),
            "id_provincia": fila.get("id_provincia"),
            "mes": fila.get("mes"),
            "temporada_cod": fila.get("temporada_cod"),
            "categoria_alojamiento_cod": fila.get("categoria_alojamiento_cod"),
            "periodo_antelacion_cod": fila.get("periodo_antelacion_cod"),
            "valoraciones_norm": fila.get("valoraciones_norm", 0),
            "tiene_valoraciones": fila.get("tiene_valoraciones", 0),
        }

        # Validación mínima
        campos_criticos = [
            "id_ccaa",
            "id_provincia",
            "mes",
            "temporada_cod",
            "categoria_alojamiento_cod",
            "periodo_antelacion_cod"
        ]
        faltantes_criticos = [c for c in campos_criticos if pd.isna(base_data.get(c))]
        if faltantes_criticos:
            resultado_default["mensaje"] = (
                f"Faltan features críticas para ML: {faltantes_criticos}"
            )
            return resultado_default

        def construir_input(tipo_dia_cod: int) -> pd.DataFrame:
            row_dict = base_data.copy()
            row_dict["tipo_dia_cod"] = tipo_dia_cod

            X = pd.DataFrame([row_dict])

            # asegurar columnas esperadas por el modelo
            for col in feature_cols:
                if col not in X.columns:
                    X[col] = 0

            X = X[feature_cols].copy()

            for col in X.columns:
                X[col] = pd.to_numeric(X[col], errors="coerce").fillna(0)

            return X

        X_semana = construir_input(tipo_dia_cod=0)
        X_fin_semana = construir_input(tipo_dia_cod=1)

        pred_semana = float(modelo.predict(X_semana)[0])
        pred_fin_semana = float(modelo.predict(X_fin_semana)[0])

        noches_semana = float(fila.get("noches_semana", 0) or 0)
        noches_fin_semana = float(fila.get("noches_fin_semana", 0) or 0)
        presupuesto_alojamiento_ine = float(fila.get("presupuesto_alojamiento_ine", 0) or 0)

        alojamiento_ml_estimado = round(
            (pred_semana * noches_semana) + (pred_fin_semana * noches_fin_semana),
            2
        )

        diferencia = round(presupuesto_alojamiento_ine - alojamiento_ml_estimado, 2)

        return {
            "ok": True,
            "presupuesto_alojamiento_ine": round(presupuesto_alojamiento_ine, 2),
            "precio_noche_semana_ml": round(pred_semana, 2),
            "precio_noche_fin_semana_ml": round(pred_fin_semana, 2),
            "alojamiento_ml_estimado": alojamiento_ml_estimado,
            "diferencia_alojamiento_ml": diferencia,
            "alcanza_alojamiento": diferencia >= 0,
            "mensaje": "Predicción ML calculada correctamente."
        }

    except Exception as e:
        resultado_default["mensaje"] = f"Error al calcular ML de alojamiento: {e}"
        return resultado_default
# =========================
# Helper para pantalla_3
# Tabla comparativa INE vs Actual vs ML
# =========================
def construir_tabla_comparacion_categoria(
    row_costos: pd.Series,
    row_presupuesto_cat: pd.Series,
    resultado_ml: dict
) -> pd.DataFrame:

    alojamiento_actual = float(row_costos.get("alojamiento_estimado", 0) or 0)
    alimentacion = float(row_costos.get("alimentacion_estimado", 0) or 0)
    actividades = float(row_costos.get("actividades_estimado", 0) or 0)
    servicios = float(row_costos.get("servicios_estimado", 0) or 0)
    otros = float(row_costos.get("otros_estimado", 0) or 0)
    transporte = float(row_costos.get("transporte_estimado", 0) or 0)

    alojamiento_ml = (
        float(resultado_ml.get("alojamiento_ml_estimado", 0) or 0)
        if resultado_ml.get("ok")
        else alojamiento_actual
    )

    data = [
        {
            "Categoría": "Alojamiento",
            "Presupuesto recomendado (€)": float(row_presupuesto_cat.get("presupuesto_alojamiento", 0) or 0),
            "Estimación inicial (€)": alojamiento_actual,
            "Estimación optimizada (€)": alojamiento_ml,
        },
        {
            "Categoría": "Alimentación",
            "Presupuesto recomendado (€)": float(row_presupuesto_cat.get("presupuesto_alimentacion", 0) or 0),
            "Estimación inicial (€)": alimentacion,
            "Estimación optimizada (€)": alimentacion,
        },
        {
            "Categoría": "Actividades",
            "Presupuesto recomendado (€)": float(row_presupuesto_cat.get("presupuesto_actividades", 0) or 0),
            "Estimación inicial (€)": actividades,
            "Estimación optimizada (€)": actividades,
        },
        {
            "Categoría": "Servicios",
            "Presupuesto recomendado (€)": float(row_presupuesto_cat.get("presupuesto_servicios", 0) or 0),
            "Estimación inicial (€)": servicios,
            "Estimación optimizada (€)": servicios,
        },
        {
            "Categoría": "Otros",
            "Presupuesto recomendado (€)": float(row_presupuesto_cat.get("presupuesto_otros", 0) or 0),
            "Estimación inicial (€)": otros,
            "Estimación optimizada (€)": otros,
        },
        {
            "Categoría": "Transporte",
            "Presupuesto recomendado (€)": float(row_presupuesto_cat.get("presupuesto_transporte", 0) or 0),
            "Estimación inicial (€)": transporte,
            "Estimación optimizada (€)": transporte,
        },
    ]

    df_comp = pd.DataFrame(data)

    fila_total = pd.DataFrame([{
        "Categoría": "TOTAL",
        "Presupuesto recomendado (€)": round(df_comp["Presupuesto recomendado (€)"].sum(), 2),
        "Estimación inicial (€)": round(df_comp["Estimación inicial (€)"].sum(), 2),
        "Estimación optimizada (€)": round(df_comp["Estimación optimizada (€)"].sum(), 2),
    }])

    df_comp = pd.concat([df_comp, fila_total], ignore_index=True)

    return df_comp


def obtener_planes_por_email(email: str) -> pd.DataFrame:
    if not email:
        return pd.DataFrame()

    df = df_plan_resumen.copy()

    df = df[
        df["email_usuario"].astype(str).str.strip().str.lower()
        == email.strip().lower()
    ].copy()

    if df.empty:
        return df

    return df.sort_values("created_at", ascending=False)


def obtener_plan_por_id(plan_id: int) -> pd.DataFrame:
    if not plan_id:
        return pd.DataFrame()

    return df_plan_resumen[df_plan_resumen["id_plan"] == plan_id].copy()


@st.cache_data(show_spinner=False)
def load_preferencias_plan(id_plan: int) -> pd.DataFrame:
    engine = get_engine()
    query = text("""
        SELECT id_plan, categoria, cantidad
        FROM culturatrip.fact_plan_viaje_preferencia
        WHERE id_plan = :id_plan
        ORDER BY categoria
    """)
    return pd.read_sql(query, engine, params={"id_plan": id_plan})


def cargar_plan_en_session_state(id_plan: int):
    df_plan = obtener_plan_por_id(id_plan)

    if df_plan.empty:
        st.warning("No se encontró el plan seleccionado.")
        return False

    plan = df_plan.iloc[0]

    df_pref = load_preferencias_plan(id_plan)
    categorias = df_pref["categoria"].tolist() if not df_pref.empty else []
    cantidades = dict(zip(df_pref["categoria"], df_pref["cantidad"])) if not df_pref.empty else {}

    st.session_state["plan_seleccionado"] = int(plan["id_plan"])
    st.session_state["modo_edicion_plan"] = True

    st.session_state["email"] = plan["email_usuario"]
    st.session_state["id_pais_origen"] = plan["id_pais_origen"]
    st.session_state["id_pais"] = plan["id_pais_destino"]

    pais_origen_row = df_dropdown_paises[df_dropdown_paises["id_pais"] == plan["id_pais_origen"]]
    pais_destino_row = df_dropdown_paises[df_dropdown_paises["id_pais"] == plan["id_pais_destino"]]

    if not pais_origen_row.empty:
        st.session_state["pais_origen"] = pais_display(pais_origen_row["pais"].iloc[0])

    if not pais_destino_row.empty:
        st.session_state["pais"] = pais_display(pais_destino_row["pais"].iloc[0])

    provincia = plan.get("provincia_destino")
    id_provincia = plan.get("id_provincia_destino")

    st.session_state["provincia_destino"] = None if pd.isna(provincia) else provincia
    st.session_state["id_provincia_destino"] = None if pd.isna(id_provincia) else id_provincia

    st.session_state["fecha_ida"] = pd.to_datetime(plan["fecha_ida"]).date() if pd.notna(plan["fecha_ida"]) else None
    st.session_state["fecha_regreso"] = pd.to_datetime(plan["fecha_regreso"]).date() if pd.notna(plan["fecha_regreso"]) else None

    st.session_state["presupuesto"] = int(plan["presupuesto_estimado"]) if pd.notna(plan["presupuesto_estimado"]) else 0
    st.session_state["categoria_alojamiento"] = plan["categoria_alojamiento"]
    st.session_state["categorias_actividad"] = categorias
    st.session_state["cantidades_actividad"] = cantidades
    st.session_state["perfil_presupuesto"] = plan["perfil_presupuesto"]
    st.session_state["tipo_viaje"] = plan["tipo_viaje"]

    return True


def eliminar_plan_db(id_plan: int):
    engine = get_engine()

    with engine.begin() as conn:
        conn.execute(
            text("""
                DELETE FROM culturatrip.fact_plan_viaje
                WHERE id_plan = :id_plan
            """),
            {"id_plan": id_plan}
        )

# ===============================
# Helper funcion para actualizar plan
# ===============================

def actualizar_plan_db(id_plan: int):
        engine = get_engine()

        # ===============================
        # Solo campos permitidos en edición
        # ===============================
        presupuesto = st.session_state.get("presupuesto")
        tipo_viaje = st.session_state.get("tipo_viaje", "Solo")
        perfil_presupuesto = st.session_state.get("perfil_presupuesto", "standard")

        id_provincia = st.session_state.get("id_provincia_destino")
        categorias_act = st.session_state.get("categorias_actividad", [])
        cantidades_act = st.session_state.get("cantidades_actividad", {})

        # ===============================
        # Campos bloqueados por índice único
        # Se leen solo para validación mínima visual/lógica,
        # pero NO se actualizan en DB
        # ===============================
        email = st.session_state.get("email")
        id_pais_origen = st.session_state.get("id_pais_origen")
        id_pais_destino = st.session_state.get("id_pais")
        fecha_ida = st.session_state.get("fecha_ida")
        fecha_regreso = st.session_state.get("fecha_regreso")
        categoria_aloj = st.session_state.get("categoria_alojamiento")

        if not id_plan:
            st.error("No hay plan seleccionado para actualizar.")
            return False

        # Validación mínima para no romper flujo general
        if not (email and id_pais_origen and id_pais_destino and fecha_ida and fecha_regreso and categoria_aloj):
            st.error("Faltan datos obligatorios del plan.")
            return False

        if fecha_regreso < fecha_ida:
            st.error("Fecha regreso no puede ser anterior a fecha ida.")
            return False

        if presupuesto is None or float(presupuesto) <= 0:
            st.error("Debes ingresar un presupuesto mayor a 0.")
            return False

        with engine.begin() as conn:
            # ===============================
            # 1. Actualizar SOLO cabecera permitida
            # ===============================
            conn.execute(text("""
                UPDATE culturatrip.fact_plan_viaje
                SET
                    presupuesto_estimado = :presupuesto,
                    tipo_viaje = :tipo_viaje,
                    perfil_presupuesto = :perfil_presupuesto
                WHERE id_plan = :id_plan
            """), {
                "id_plan": id_plan,
                "presupuesto": presupuesto,
                "tipo_viaje": tipo_viaje,
                "perfil_presupuesto": perfil_presupuesto
            })

            # ===============================
            # 2. Reemplazar provincia destino
            # ===============================
            conn.execute(text("""
                DELETE FROM culturatrip.fact_plan_viaje_destino
                WHERE id_plan = :id_plan
            """), {
                "id_plan": id_plan
            })

            if id_provincia:
                conn.execute(text("""
                    INSERT INTO culturatrip.fact_plan_viaje_destino (id_plan, orden, id_provincia)
                    VALUES (:id_plan, 1, :id_provincia)
                """), {
                    "id_plan": id_plan,
                    "id_provincia": id_provincia
                })

            # ===============================
            # 3. Reemplazar preferencias
            # ===============================
            conn.execute(text("""
                DELETE FROM culturatrip.fact_plan_viaje_preferencia
                WHERE id_plan = :id_plan
            """), {
                "id_plan": id_plan
            })

            if categorias_act:
                for cat in categorias_act:
                    conn.execute(text("""
                        INSERT INTO culturatrip.fact_plan_viaje_preferencia (id_plan, categoria, cantidad)
                        VALUES (:id_plan, :categoria, :cantidad)
                    """), {
                        "id_plan": id_plan,
                        "categoria": cat,
                        "cantidad": int(cantidades_act.get(cat, 1))
                    })

        return True


# ===============================
# Pantalla 1
# ===============================
def pantalla_1():

    st.markdown("<h2 style='margin-bottom:0.3rem;'>Selecciona un destino</h2>", unsafe_allow_html=True)

    # ===============================
    # Total países (desde view)
    # ===============================
    total_paises = int(df_total_paises["total_paises"].iloc[0])

    st.markdown(
        f"""
        <div style="padding:18px; font-size:22px; border-radius:12px;">
        <b>Explora</b> <b>{total_paises}</b> <b>destinos en todo el mundo</b>
        </div>
        """,
        unsafe_allow_html=True
    )

    # ===============================
    # Dropdown país
    # ===============================
    pais_guardado = st.session_state.get("pais")

    index_default = (
        lista_paises_ui.index(pais_guardado)
        if pais_guardado in lista_paises_ui
        else None
    )
    if pais_guardado:
        pais_ui = pais_guardado
    pais_ui = st.selectbox(
        label="",
        options=lista_paises_ui,
        placeholder="— Selecciona un país —",
        key="pais_ui"
    )

    # Persistencia
    if pais_ui is None:
        if pais_guardado is None:
            st.info("Elige un destino para conocer datos curiosos")
            return
        pais = pais_guardado
    else:
        st.session_state["pais"] = pais_ui
        st.session_state["id_pais"] = map_paisui_a_id.get(pais_ui)
        pais = pais_ui

    id_pais = st.session_state.get("id_pais")

    # ===============================
    # Métricas del país (desde view)
    # ===============================
    row = df_pantalla1_detalle[df_pantalla1_detalle["id_pais"] == id_pais]

    if row.empty:
        st.markdown(
            f"""
            <div style="padding:25px; font-size:20px; border-radius:12px; background:#F5F7F9;">
            <b>¿Sabías que…?</b> Aún no tenemos división política municipal cargada para <b>{pais}</b>.
            </div>
            """,
            unsafe_allow_html=True
        )
        return

    n_municipios = int(row["total_municipios"].iloc[0])
    n_provincias = int(row["total_provincias"].iloc[0])
    n_islas = int(row["total_islas"].iloc[0])

    # ===============================
    # Sabías que
    # ===============================
    st.markdown(
        f"""
        <div style="padding:25px; font-size:20px; border-radius:12px; background:#F5F7F9;">
        <b>¿Sabías que…?</b> <b>{pais}</b> contiene <b>{n_municipios:,}</b> municipios únicos,
        organizados en <b>{n_provincias:,}</b> provincias.
        Además, se identifican <b>{n_islas:,}</b> islas distintas.
        </div>
        """.replace(",", "."),
        unsafe_allow_html=True
    )

    st.divider()

    # ===============================
    # Destacados culturales + imagen
    # ===============================

    # --- Iconos SVG mini por categoría (paleta CulturaTrip) ---
    _ICON_HISTORIA = '<svg viewBox="0 0 24 24" width="22" height="22"><path d="M3 21h18v-2H3v2zm0-4h18v-2H3v2zm0-4h18v-2H3v2zm0-4h18V7H3v2zm0-6v2h18V3H3z" fill="#004AAD" opacity="0.15"/><path d="M12 2L3 7v2h18V7L12 2zm0 2.26L17.18 7H6.82L12 4.26zM3 21h18v-2H3v2zm2-4h14v-1H5v1zm0-3h14v-1H5v1zm1-3h4v4H6v-4zm6 0h4v4h-4v-4z" fill="#004AAD"/></svg>'
    _ICON_RELIGION = '<svg viewBox="0 0 24 24" width="22" height="22"><path d="M12 2C8.13 2 5 5.13 5 9c0 5.25 7 13 7 13s7-7.75 7-13c0-3.87-3.13-7-7-7z" fill="#38B6FF" opacity="0.15"/><path d="M12 2C8.13 2 5 5.13 5 9c0 5.25 7 13 7 13s7-7.75 7-13c0-3.87-3.13-7-7-7zm0 18.35C9.07 16.87 7 13.39 7 9c0-2.76 2.24-5 5-5s5 2.24 5 5c0 4.39-2.07 7.87-5 11.35zM11 6h2v5h-2zm0 6h2v2h-2z" fill="#38B6FF"/></svg>'
    _ICON_CULTURA = '<svg viewBox="0 0 24 24" width="22" height="22"><circle cx="12" cy="12" r="10" fill="#3DCD00" opacity="0.15"/><path d="M12 2C6.48 2 2 6.48 2 12s4.48 10 10 10 10-4.48 10-10S17.52 2 12 2zm-1 17.93c-3.95-.49-7-3.85-7-7.93 0-.62.08-1.21.21-1.79L9 15v1c0 1.1.9 2 2 2v1.93zm6.9-2.54c-.26-.81-1-1.39-1.9-1.39h-1v-3c0-.55-.45-1-1-1H8v-2h2c.55 0 1-.45 1-1V7h2c1.1 0 2-.9 2-2v-.41c2.93 1.19 5 4.06 5 7.41 0 2.08-.8 3.97-2.1 5.39z" fill="#3DCD00"/></svg>'
    _ICON_GASTRO = '<svg viewBox="0 0 24 24" width="22" height="22"><path d="M8.1 13.34l2.83-2.83L3.91 3.5a4.008 4.008 0 000 5.66l4.19 4.18zm6.78-1.81c1.53.71 3.68.21 5.27-1.38 1.91-1.91 2.28-4.65.81-6.12-1.46-1.46-4.2-1.1-6.12.81-1.59 1.59-2.09 3.74-1.38 5.27L3.7 19.87l1.41 1.41L12 14.41l6.88 6.88 1.41-1.41L13.41 13l1.47-1.47z" fill="#FFDE59"/><path d="M8.1 13.34l2.83-2.83L3.91 3.5a4.008 4.008 0 000 5.66l4.19 4.18zm6.78-1.81c1.53.71 3.68.21 5.27-1.38 1.91-1.91 2.28-4.65.81-6.12-1.46-1.46-4.2-1.1-6.12.81-1.59 1.59-2.09 3.74-1.38 5.27L3.7 19.87l1.41 1.41L12 14.41l6.88 6.88 1.41-1.41L13.41 13l1.47-1.47z" fill="#DAA520" opacity="0.3"/></svg>'
    _ICON_LENGUA = '<svg viewBox="0 0 24 24" width="22" height="22"><path d="M20 2H4c-1.1 0-2 .9-2 2v18l4-4h14c1.1 0 2-.9 2-2V4c0-1.1-.9-2-2-2z" fill="#4BFA00" opacity="0.15"/><path d="M20 2H4c-1.1 0-2 .9-2 2v12c0 1.1.9 2 2 2h14l4 4V4c0-1.1-.9-2-2-2zm0 14H6l-2 2V4h16v12z" fill="#4BFA00"/><path d="M7 9h2v2H7zm4 0h2v2h-2zm4 0h2v2h-2z" fill="#4BFA00"/></svg>'

    # Colores de borde por categoría temática
    _CAT_COLORS = {
        "historia":     "#004AAD",   # azul oscuro
        "religion":     "#38B6FF",   # azul claro
        "cultura":      "#3DCD00",   # verde hoja
        "gastronomia":  "#FFDE59",   # amarillo
        "lengua":       "#4BFA00",   # verde lima
    }
    _CAT_ICONS = {
        "historia":     _ICON_HISTORIA,
        "religion":     _ICON_RELIGION,
        "cultura":      _ICON_CULTURA,
        "gastronomia":  _ICON_GASTRO,
        "lengua":       _ICON_LENGUA,
    }
    _CAT_LABELS = {
        "historia":     "Historia",
        "religion":     "Patrimonio religioso",
        "cultura":      "Cultura y sociedad",
        "gastronomia":  "Gastronomía",
        "lengua":       "Lenguas",
    }

    DATOS_CULTURALES = {
        "España": [
            {"titulo": "Antigüedad",   "texto": "España fue uno de los primeros territorios europeos explotados por metales por fenicios y romanos.", "cat": "historia"},
            {"titulo": "Granada",      "texto": "Fue el último reino musulmán de la Península Ibérica hasta 1492.", "cat": "historia"},
            {"titulo": "Córdoba",      "texto": "En el siglo X fue una de las ciudades más grandes del mundo occidental.", "cat": "historia"},
            {"titulo": "Mérida",       "texto": "Fue una de las capitales romanas más importantes fuera de Italia.", "cat": "historia"},
            {"titulo": "Toledo",       "texto": "Durante siglos convivieron cristianos, judíos y musulmanes.", "cat": "cultura"},
            {"titulo": "Santiago",     "texto": "El Camino de Santiago es una de las rutas de peregrinación más antiguas.", "cat": "religion"},
            {"titulo": "Sevilla",      "texto": "Desde aquí se gestionaba el comercio con América.", "cat": "cultura"},
            {"titulo": "Idiomas",      "texto": "Existen lenguas cooficiales como catalán, gallego y euskera.", "cat": "lengua"},
            {"titulo": "Gastronomía",  "texto": "Las tapas nacieron como una forma de cubrir bebidas.", "cat": "gastronomia"},
        ],
        "Italia": [
            {"titulo": "Roma",  "texto": "Centro del Imperio Romano durante siglos.", "cat": "historia"},
        ],
        "Francia": [
            {"titulo": "París", "texto": "Uno de los centros culturales y artísticos más importantes de Europa.", "cat": "cultura"},
        ],
    }

    st.subheader("Destacados culturales del país")

    # Grid de cards culturales (2 columnas)
    if pais in DATOS_CULTURALES:
        datos = DATOS_CULTURALES[pais]

        # Construir el grid HTML completo
        cards_html = '<div style="display:grid; grid-template-columns: 1fr 1fr; gap:14px;">'

        for d in datos:
            cat = d.get("cat", "historia")
            border_color = _CAT_COLORS.get(cat, "#004AAD")
            icon_svg = _CAT_ICONS.get(cat, _ICON_HISTORIA)
            cat_label = _CAT_LABELS.get(cat, "Historia")

            cards_html += (
                f'<div style="background:white; border-radius:14px; padding:18px 20px;'
                f' border-left:5px solid {border_color};'
                f' box-shadow:0 2px 12px rgba(0,74,173,0.06);'
                f' border:1px solid var(--ct-borde); border-left:5px solid {border_color};">'
                f'<div style="display:flex; align-items:center; gap:10px; margin-bottom:8px;">'
                f'<span style="flex-shrink:0;">{icon_svg}</span>'
                f'<span style="font-size:17px; font-weight:800; color:var(--ct-azul-oscuro, #004AAD);">{d["titulo"]}</span>'
                f'</div>'
                f'<div style="font-size:14px; line-height:1.6; color:var(--ct-texto, #0B2B55); margin-bottom:8px;">'
                f'{d["texto"]}'
                f'</div>'
                f'<span style="display:inline-block; padding:3px 10px; border-radius:999px;'
                f' font-size:11px; font-weight:700; letter-spacing:0.3px;'
                f' background:{border_color}18; color:{border_color};">'
                f'{cat_label}</span>'
                f'</div>'
            )

        cards_html += '</div>'

        st.markdown(cards_html, unsafe_allow_html=True)
    else:
        st.info("No hay datos culturales para este país todavía")

    st.divider()
# ===============================
# Pantalla Gestion de Planes
# ===============================
def pantalla_gestion_planes():
    st.header("Gestión de planes")
    st.caption("Consulta, crea, selecciona, edita o elimina tus planes guardados.")

    # =========================
    # 1. Usuario / correo
    # =========================
    email = st.session_state.get("email", "").strip()

    if not email:
        email_input = st.text_input(
            "Ingresa tu correo para ver tus planes",
            key="email_gestion_planes"
        )

        if email_input:
            st.session_state["email"] = email_input.strip()
            st.rerun()
        else:
            st.info("Debes ingresar tu correo para continuar.")
            return

    col_user1, col_user2 = st.columns([4, 1])

    with col_user1:
        st.markdown(f"**Usuario:** {email}")

    with col_user2:
        if st.button("Cambiar usuario", key="btn_cambiar_usuario_gp", use_container_width=True):
            st.session_state["email"] = ""
            st.session_state["plan_seleccionado"] = None
            st.session_state["modo_edicion_plan"] = False
            st.rerun()

    st.divider()

    # =========================
    # 2. Crear nuevo plan
    # =========================
    st.markdown("### ➕ Crear nuevo plan")
    st.caption("Si deseas iniciar una nueva planificación, crea un plan desde aquí.")

    if st.button("Crear nuevo plan", key="btn_crear_nuevo_plan", use_container_width=True):
        iniciar_nuevo_plan()
        st.session_state["email"] = email
        st.session_state["step"] = 3  # Planificación
        st.rerun()

    st.divider()

    # =========================
    # 3. Obtener planes del usuario
    # =========================
    df_planes = obtener_planes_por_email(email)

    if df_planes.empty:
        st.info("No tienes planes guardados todavía.")
        st.caption("Puedes crear tu primer plan desde el botón anterior.")
        return

    st.markdown("### Listado de planes")

    # =========================
    # 4. Encabezado tipo tabla
    # =========================
    h1, h2, h3, h4, h5, h6 = st.columns([1.2, 2.2, 2.2, 3.0, 2.0, 2.4])

    with h1:
        st.markdown("**N.º plan**")
    with h2:
        st.markdown("**País origen**")
    with h3:
        st.markdown("**País destino**")
    with h4:
        st.markdown("**Fechas ida / regreso**")
    with h5:
        st.markdown("**Presupuesto**")
    with h6:
        st.markdown("**Acciones**")

    st.divider()

    # =========================
    # 5. Filas de la tabla
    # =========================
    for _, row in df_planes.iterrows():
        plan_id = int(row["id_plan"])

        pais_origen = row["pais_origen"] if pd.notna(row["pais_origen"]) else "—"
        pais_destino = row["pais_destino"] if pd.notna(row["pais_destino"]) else "—"

        fecha_ida = str(row["fecha_ida"]) if pd.notna(row["fecha_ida"]) else "—"
        fecha_regreso = str(row["fecha_regreso"]) if pd.notna(row["fecha_regreso"]) else "—"

        presupuesto = format_eur(row["presupuesto_estimado"]) if pd.notna(row["presupuesto_estimado"]) else "€0,00"

        c1, c2, c3, c4, c5, c6 = st.columns([1.2, 2.2, 2.2, 3.0, 2.0, 2.4])

        with c1:
            st.write(plan_id)

        with c2:
            st.write(pais_origen)

        with c3:
            st.write(pais_destino)

        with c4:
            st.write(f"{fecha_ida} → {fecha_regreso}")

        with c5:
            st.write(presupuesto)

        with c6:
            a1, a2, a3 = st.columns(3)

            # =========================
            # Ver plan
            # =========================
            with a1:
                if st.button("👁", key=f"ver_plan_{plan_id}", use_container_width=True):
                    st.session_state["plan_seleccionado"] = plan_id
                    st.session_state["modo_edicion_plan"] = False
                    st.session_state["step"] = 4  # Itinerario / Resumen del plan
                    st.rerun()

            # =========================
            # Editar plan
            # =========================
            with a2:
                if st.button("✏️", key=f"editar_plan_{plan_id}", use_container_width=True):
                    ok = cargar_plan_en_session_state(plan_id)
                    if ok:
                        st.session_state["step"] = 3  # Planificación
                        st.rerun()

            # =========================
            # Eliminar plan
            # =========================
            with a3:
                if st.button("🗑", key=f"eliminar_plan_{plan_id}", use_container_width=True):
                    eliminar_plan_db(plan_id)
                    st.cache_data.clear()

                    if st.session_state.get("plan_seleccionado") == plan_id:
                        st.session_state["plan_seleccionado"] = None
                        st.session_state["modo_edicion_plan"] = False

                    st.success(f"Plan {plan_id} eliminado correctamente.")
                    st.rerun()

        st.divider()

# ===============================
# Pantalla 2
# ===============================
def pantalla_2():
        st.header("Planifica tu viaje")
        st.caption("Completa la información base del viaje para construir el plan y estimar costos.")

        modo_edicion = bool(st.session_state.get("modo_edicion_plan", False))

        if modo_edicion:
            st.info(
                "Estás editando un plan existente. Para cambiar correo, países, fechas o tipo de hospedaje, debes crear un plan nuevo.")

        col_left, col_right = st.columns([1, 1], gap="large")

        # ===============================
        # COLUMNA IZQUIERDA
        # ===============================
        with col_left:
            # -------------------------------
            # ORIGEN Y DESTINO
            # -------------------------------
            st.subheader("Origen y destino")

            st.selectbox(
                "Perfil de presupuesto",
                options=["standard"],
                index=0,
                disabled=True,
                help="Perfil base del modelo según distribución de referencia INE/EGATUR."
            )
            st.session_state["perfil_presupuesto"] = "standard"

            st.selectbox(
                "Tipo viaje",
                options=["Solo"],
                index=0,
                disabled=True
            )
            st.session_state["tipo_viaje"] = "Solo"

            pais_origen_ui = st.selectbox(
                "País origen",
                options=lista_paises_ui,
                index=lista_paises_ui.index(st.session_state["pais_origen"])
                if st.session_state["pais_origen"] in lista_paises_ui else None,
                placeholder="— Selecciona país origen —",
                key="pais_origen_ui",
                disabled=modo_edicion
            )

            if pais_origen_ui:
                st.session_state["pais_origen"] = pais_origen_ui
                st.session_state["id_pais_origen"] = map_paisui_a_id.get(pais_origen_ui)
                st.session_state["plan_guardado"] = False

            pais_destino_ui = st.selectbox(
                "País destino",
                options=lista_paises_ui,
                index=lista_paises_ui.index(st.session_state["pais"])
                if st.session_state["pais"] in lista_paises_ui else None,
                placeholder="— Selecciona país destino —",
                key="pais_destino_ui",
                disabled=modo_edicion
            )

            if pais_destino_ui:
                st.session_state["pais"] = pais_destino_ui
                st.session_state["id_pais"] = map_paisui_a_id.get(pais_destino_ui)
                st.session_state["plan_guardado"] = False

            st.divider()

            # -------------------------------
            # MACHINE LEARNING
            # -------------------------------
            st.subheader("Descubre las provincias más recomendadas para ti")

            id_pais_destino_ml = st.session_state.get("id_pais")
            fecha_ida_ml = st.session_state.get("fecha_ida")
            fecha_regreso_ml = st.session_state.get("fecha_regreso")
            presupuesto_ml = float(st.session_state.get("presupuesto", 0) or 0)

            fechas_validas_ml = (
                    fecha_ida_ml is not None
                    and fecha_regreso_ml is not None
                    and fecha_regreso_ml > fecha_ida_ml
            )

            condiciones_ml_ok = (
                    bool(id_pais_destino_ml)
                    and fechas_validas_ml
                    and presupuesto_ml > 0
            )

            if not condiciones_ml_ok:
                st.info("Completa país destino, fechas y presupuesto para ver recomendaciones.")
            else:
                try:
                    df_top5_ml = generar_top5_provincias_modelo_avanzado(
                        df_base=df_ml_avanzado_base,
                        id_pais_destino=id_pais_destino_ml,
                        fecha_ida=fecha_ida_ml,
                        fecha_regreso=fecha_regreso_ml,
                        presupuesto_usuario=presupuesto_ml
                    )
                    if df_top5_ml.empty:
                        st.warning("No se encontraron provincias candidatas para ese país destino.")
                    else:
                        mejor = df_top5_ml.iloc[0]

                        # Guardar recomendación principal ML
                        st.session_state["provincia_ml_recomendada"] = mejor["provincia_nombre"]
                        st.session_state["id_provincia_ml_recomendada"] = mejor["id_provincia"]

                        score_principal = float(mejor["score_modelo"])

                        if score_principal >= 0:
                            compat_label = "Alta compatibilidad"
                            compat_color = "#3DCD00"
                        elif score_principal >= -3:
                            compat_label = "Compatibilidad media"
                            compat_color = "#FFDE59"
                        else:
                            compat_label = "Baja compatibilidad"
                            compat_color = "#E74C3C"

                        # --- Coordenadas aproximadas de provincias españolas para el mapa ---
                        _COORDS_PROVINCIA = {
                            "a coruña": (50, 65), "lugo": (80, 60), "ourense": (75, 85),
                            "pontevedra": (45, 85), "asturias": (115, 50), "cantabria": (150, 50),
                            "vizcaya": (180, 48), "guipúzcoa": (195, 45), "álava": (185, 58),
                            "navarra": (210, 52), "la rioja": (195, 68), "zaragoza": (230, 78),
                            "huesca": (235, 58), "teruel": (245, 100), "lleida": (270, 62),
                            "girona": (305, 55), "barcelona": (300, 72), "tarragona": (280, 82),
                            "castellón": (275, 105), "valencia": (265, 125), "alicante": (260, 145),
                            "murcia": (245, 155), "almería": (210, 170), "granada": (190, 165),
                            "málaga": (170, 175), "cádiz": (130, 180), "sevilla": (140, 160),
                            "huelva": (110, 160), "córdoba": (165, 150), "jaén": (195, 148),
                            "ciudad real": (195, 130), "toledo": (175, 115), "madrid": (185, 100),
                            "cuenca": (225, 108), "guadalajara": (215, 92), "ávila": (155, 95),
                            "segovia": (170, 88), "soria": (200, 75), "valladolid": (145, 78),
                            "burgos": (160, 62), "palencia": (145, 65), "león": (115, 62),
                            "zamora": (110, 80), "salamanca": (105, 100), "cáceres": (115, 125),
                            "badajoz": (120, 145), "albacete": (235, 135),
                            "las palmas": (85, 220), "santa cruz de tenerife": (50, 220),
                            "illes balears": (320, 105), "ceuta": (155, 195), "melilla": (215, 192),
                        }

                        # Colores para los pins
                        _pin_colors = ["#004AAD", "#38B6FF", "#3DCD00", "#FFDE59", "#5A6B85"]

                        # --- Construir mapa SVG ---
                        svg_w, svg_h = 380, 240

                        # Silueta simplificada de España
                        spain_path = (
                            "M45,55 L65,40 L100,35 L140,38 L175,35 L200,40 L220,38 "
                            "L245,42 L270,38 L300,45 L320,55 L310,70 L305,85 "
                            "L285,100 L275,115 L270,135 L260,150 L245,160 L225,175 "
                            "L200,180 L170,185 L140,185 L120,175 L105,165 "
                            "L95,145 L100,125 L95,105 L85,90 L60,80 L42,70 Z"
                        )

                        map_svg = (
                            f'<svg width="100%" viewBox="0 0 {svg_w} {svg_h}" '
                            f'xmlns="http://www.w3.org/2000/svg" '
                            f'style="max-width:500px; margin:0 auto; display:block;">'

                            # Fondo del mapa
                            f'<rect width="{svg_w}" height="{svg_h}" rx="16" '
                            f'fill="var(--ct-azul-tint, #E6F4FF)" opacity="0.5"/>'

                            # Silueta de España
                            f'<path d="{spain_path}" fill="white" stroke="var(--ct-borde, #D7E3F4)" '
                            f'stroke-width="1.5" opacity="0.9"/>'
                        )

                        # Pins de las 5 provincias
                        legend_items = []
                        for idx_ml, (_, row_ml) in enumerate(df_top5_ml.iterrows()):
                            prov_name = str(row_ml["provincia_nombre"]).lower().strip()
                            rank_ml = int(row_ml["ranking"])
                            pin_color = _pin_colors[idx_ml % len(_pin_colors)]

                            coords = _COORDS_PROVINCIA.get(prov_name, None)
                            if coords:
                                cx, cy = coords
                            else:
                                cx = 180 + idx_ml * 25
                                cy = 120

                            # Pin SVG (gota)
                            pin_size = 14 if rank_ml == 1 else 10
                            map_svg += (
                                f'<g transform="translate({cx},{cy})">'
                                f'<circle r="{pin_size}" fill="{pin_color}" opacity="0.25"/>'
                                f'<circle r="{pin_size - 4}" fill="{pin_color}"/>'
                                f'<text x="0" y="1" text-anchor="middle" dominant-baseline="central" '
                                f'font-family="Nunito,sans-serif" font-size="{9 if rank_ml == 1 else 7}" '
                                f'font-weight="800" fill="white">{rank_ml}</text>'
                                f'</g>'
                            )

                            legend_items.append((rank_ml, row_ml["provincia_nombre"].title(), pin_color))

                        map_svg += '</svg>'

                        # --- Renderizar mapa + leyenda ---
                        st.markdown(
                            f'<div style="text-align:center; margin-bottom:12px;">{map_svg}</div>',
                            unsafe_allow_html=True,
                        )

                        # Compatibilidad badge
                        st.markdown(
                            f'<div style="display:flex; align-items:center; gap:8px; margin-bottom:14px;">'
                            f'<span style="display:inline-block; width:12px; height:12px; border-radius:50%; '
                            f'background:{compat_color};"></span>'
                            f'<span style="font-size:14px; font-weight:700; '
                            f'color:var(--ct-texto, #0B2B55);">'
                            f'Provincia principal: {mejor["provincia_nombre"].title()} '
                            f'— {compat_label}</span></div>',
                            unsafe_allow_html=True,
                        )

                        # Leyenda como lista compacta
                        legend_html = '<div style="display:flex; flex-direction:column; gap:6px;">'
                        for rank_l, name_l, color_l in legend_items:
                            legend_html += (
                                f'<div style="display:flex; align-items:center; gap:10px; '
                                f'padding:8px 14px; border-radius:10px; '
                                f'background:white; border:1px solid var(--ct-borde, #D7E3F4);">'
                                f'<span style="display:inline-flex; align-items:center; justify-content:center; '
                                f'width:26px; height:26px; border-radius:50%; flex-shrink:0; '
                                f'background:{color_l}; color:white; font-size:12px; font-weight:800;">'
                                f'{rank_l}</span>'
                                f'<span style="font-size:15px; font-weight:700; '
                                f'color:var(--ct-azul-oscuro, #004AAD);">{name_l}</span>'
                                f'</div>'
                            )
                        legend_html += '</div>'

                        st.markdown(legend_html, unsafe_allow_html=True)

                except Exception as e:
                    st.error(f"No fue posible generar las recomendaciones ML: {e}")

            st.divider()

            # -------------------------------
            # MODELO NO SUPERVISADO
            # -------------------------------
            provincia_actual_ns = st.session_state.get("provincia_destino")
            id_provincia_actual_ns = st.session_state.get("id_provincia_destino")

            mostrar_top5_provincias_similares(
                df_base=df_ml_no_supervisado_base,
                id_provincia_seleccionada=id_provincia_actual_ns,
                provincia_nombre_seleccionada=provincia_actual_ns if provincia_actual_ns else "—"
            )
        # ===============================
        # COLUMNA DERECHA
        # ===============================
        with col_right:
            # -------------------------------
            # DETALLES DEL VIAJE
            # -------------------------------
            st.subheader("Detalles del viaje")

            email = st.text_input(
                "Usuario / correo",
                value=st.session_state["email"],
                placeholder="usuario@email.com",
                disabled=modo_edicion
            )

            if email is not None:
                st.session_state["email"] = email
                st.session_state["plan_guardado"] = False

            fecha_ida_default = st.session_state["fecha_ida"] if st.session_state[
                                                                     "fecha_ida"] is not None else date.today()
            fecha_regreso_default = st.session_state["fecha_regreso"] if st.session_state[
                                                                             "fecha_regreso"] is not None else date.today()

            fecha_ida = st.date_input(
                "Fecha de ida",
                value=fecha_ida_default,
                key="fecha_ida_input",
                disabled=modo_edicion
            )
            fecha_regreso = st.date_input(
                "Fecha de regreso",
                value=fecha_regreso_default,
                key="fecha_regreso_input",
                disabled=modo_edicion
            )

            st.session_state["fecha_ida"] = fecha_ida
            st.session_state["fecha_regreso"] = fecha_regreso
            st.session_state["plan_guardado"] = False

            temporada_ida = obtener_temporada_por_fecha(fecha_ida, df_temporada_mes)
            temporada_regreso = obtener_temporada_por_fecha(fecha_regreso, df_temporada_mes)

            col_temp1, col_temp2 = st.columns(2)
            with col_temp1:
                st.metric("Temporada ida", temporada_ida.capitalize() if temporada_ida else "—")
            with col_temp2:
                st.metric("Temporada regreso", temporada_regreso.capitalize() if temporada_regreso else "—")

            hoy = date.today()
            dias_restantes = (fecha_ida - hoy).days if fecha_ida else None
            duracion_dias = (fecha_regreso - fecha_ida).days if fecha_ida and fecha_regreso else None

            col_kpi1, col_kpi2 = st.columns(2)
            with col_kpi1:
                st.metric("Días restantes", f"{max(dias_restantes, 0)}" if dias_restantes is not None else "—")
            with col_kpi2:
                st.metric("Duración del viaje", f"{duracion_dias} días" if duracion_dias is not None else "—")

            if duracion_dias is not None and duracion_dias <= 0:
                st.warning("La fecha de regreso debe ser posterior a la fecha de ida.")

            presupuesto = st.number_input(
                "Presupuesto estimado (€)",
                min_value=0,
                value=int(st.session_state["presupuesto"]),
                step=50
            )
            st.session_state["presupuesto"] = presupuesto
            st.session_state["plan_guardado"] = False

            st.divider()

            # -------------------------------
            # PREFERENCIAS DEL USUARIO
            # -------------------------------
            st.subheader("Preferencias del usuario")

            id_pais_destino = st.session_state.get("id_pais")

            if id_pais_destino:
                df_prov = df_dropdown_provincias[
                    df_dropdown_provincias["id_pais"] == id_pais_destino
                    ].copy()

                lista_prov = sorted(df_prov["provincia_nombre"].dropna().unique().tolist())

                provincia_actual = st.session_state.get("provincia_destino")
                index_prov = lista_prov.index(provincia_actual) if provincia_actual in lista_prov else None

                provincia_ui = st.selectbox(
                    "Provincia destino",
                    options=lista_prov,
                    index=index_prov,
                    placeholder="— Selecciona provincia —",
                    key="provincia_destino_ui"
                )

                if provincia_ui:
                    st.session_state["provincia_destino"] = provincia_ui
                    st.session_state["plan_guardado"] = False

                    row = df_prov[df_prov["provincia_nombre"] == provincia_ui].head(1)
                    if not row.empty:
                        st.session_state["id_provincia_destino"] = row["id_provincia"].iloc[0]
            else:
                st.info("Selecciona primero un país destino.")

            lista_aloj = sorted(
                df_dropdown_cat_aloj["categoria_alojamiento"].dropna().unique().tolist()
            )

            tipo_aloj = st.selectbox(
                "Tipo hospedaje",
                options=lista_aloj,
                index=lista_aloj.index(st.session_state["categoria_alojamiento"])
                if st.session_state["categoria_alojamiento"] in lista_aloj else None,
                placeholder="— Selecciona hospedaje —",
                key="tipo_hospedaje_ui",
                disabled=modo_edicion
            )

            if tipo_aloj:
                st.session_state["categoria_alojamiento"] = tipo_aloj
                st.session_state["plan_guardado"] = False

            st.subheader("Preferencias de actividades")

            lista_act = sorted(
                df_dropdown_cat_act["categoria"].dropna().unique().tolist()
            )

            categorias_act = st.multiselect(
                "Categorías deseadas",
                options=lista_act,
                default=st.session_state.get("categorias_actividad", []),
                placeholder="— Selecciona una o varias categorías —",
            )

            st.session_state["categorias_actividad"] = categorias_act
            st.session_state["plan_guardado"] = False

            cantidades_actuales = st.session_state.get("cantidades_actividad", {}).copy()
            nuevas_cantidades = {}

            if categorias_act:
                c1, c2 = st.columns(2)
                for idx, cat in enumerate(categorias_act):
                    target_col = c1 if idx % 2 == 0 else c2
                    with target_col:
                        nuevas_cantidades[cat] = st.number_input(
                            f"Cantidad para '{cat}'",
                            min_value=1,
                            max_value=20,
                            value=int(cantidades_actuales.get(cat, 1)),
                            step=1,
                            key=f"cant_{cat}"
                        )

            st.session_state["cantidades_actividad"] = nuevas_cantidades

        st.divider()
        st.subheader("✅ Guardar / aprobar plan")

        col_save, col_next = st.columns([1, 1])

        with col_save:
            modo_edicion = bool(st.session_state.get("modo_edicion_plan", False))
            plan_id_edicion = st.session_state.get("plan_seleccionado")

            texto_boton_guardar = "Actualizar plan ✏️" if modo_edicion else "Guardar plan nuevo ✅"

            if modo_edicion:
                st.caption("Solo se actualizarán los campos permitidos del plan actual.")

            disabled_save = st.session_state.get("guardando", False)

            if st.button(texto_boton_guardar, use_container_width=True, disabled=disabled_save):
                errores = []

                if not st.session_state.get("email"):
                    errores.append("Debes ingresar un correo.")

                if not st.session_state.get("id_pais_origen"):
                    errores.append("Debes seleccionar el país de origen.")

                if not st.session_state.get("id_pais"):
                    errores.append("Debes seleccionar el país destino.")

                if not st.session_state.get("id_provincia_destino"):
                    errores.append("Debes seleccionar una provincia destino.")

                if not st.session_state.get("categoria_alojamiento"):
                    errores.append("Debes seleccionar un tipo de hospedaje.")

                if not st.session_state.get("categorias_actividad"):
                    errores.append("Debes seleccionar al menos una categoría de actividad.")

                fecha_ida_ss = st.session_state.get("fecha_ida")
                fecha_regreso_ss = st.session_state.get("fecha_regreso")

                if fecha_ida_ss and fecha_regreso_ss and fecha_regreso_ss <= fecha_ida_ss:
                    errores.append("La fecha de regreso debe ser posterior a la fecha de ida.")

                if st.session_state.get("presupuesto", 0) <= 0:
                    errores.append("Debes ingresar un presupuesto mayor a 0.")

                if errores:
                    for err in errores:
                        st.warning(err)
                else:
                    st.session_state["guardando"] = True
                    try:
                        if modo_edicion and plan_id_edicion:
                            ok = actualizar_plan_db(plan_id_edicion)
                            plan_id = plan_id_edicion if ok else None
                        else:
                            plan_id = guardar_plan_db()
                    finally:
                        st.session_state["guardando"] = False

                    if plan_id:
                        st.session_state["ultimo_plan_id"] = int(plan_id)
                        st.session_state["plan_seleccionado"] = int(plan_id)
                        st.session_state["plan_guardado"] = True

                        if modo_edicion:
                            st.session_state["modo_edicion_plan"] = True
                            st.success(f"Plan actualizado con éxito. ID del plan: {plan_id}")
                        else:
                            st.session_state["modo_edicion_plan"] = False
                            st.success(f"Plan guardado con éxito. ID del plan: {plan_id}")

                        st.cache_data.clear()

        with col_next:
            if st.button("Ir a Gestión de planes ➜", use_container_width=True):
                st.cache_data.clear()
                st.session_state["step"] = 2
                st.rerun()




def pantalla_3():

        st.header("Resumen del plan")

        plan_id = st.session_state.get("plan_seleccionado")

        if not plan_id:
            st.info("No hay un plan seleccionado.")
            st.caption("Primero crea un nuevo plan o selecciona uno en Gestión de planes.")
            return

        plan_df = df_plan_resumen[df_plan_resumen["id_plan"] == plan_id]

        if plan_df.empty:
            st.warning("No se encontró el plan seleccionado.")
            return

        plan = plan_df.iloc[0]

        row_costos_df = df_plan_costos[df_plan_costos["id_plan"] == plan_id]
        row_presupuesto_cat_df = df_plan_presupuesto_cat[df_plan_presupuesto_cat["id_plan"] == plan_id]

        st.subheader(f"ID del plan: {plan_id}")

        presupuesto_usuario = float(plan["presupuesto_estimado"] or 0)

        alojamiento = 0.0
        alimentacion = 0.0
        actividades = 0.0
        servicios = 0.0
        otros = 0.0
        transporte = 0.0
        total_estimado = 0.0

        if not row_costos_df.empty:
            costos = row_costos_df.iloc[0]
            alojamiento = float(costos.get("alojamiento_estimado", 0) or 0)
            alimentacion = float(costos.get("alimentacion_estimado", 0) or 0)
            actividades = float(costos.get("actividades_estimado", 0) or 0)
            servicios = float(costos.get("servicios_estimado", 0) or 0)
            otros = float(costos.get("otros_estimado", 0) or 0)
            transporte = float(costos.get("transporte_estimado", 0) or 0)
            total_estimado = float(costos.get("costo_total_estimado", 0) or 0)
        else:
            costos = pd.Series(dtype="object")

        diferencia = round(presupuesto_usuario - total_estimado, 2)

        # ===============================
        # KPIs principales
        # ===============================
        k1, k2, k3, k4 = st.columns(4)

        with k1:
            st.metric("Días de viaje", int(plan["dias_viaje"]))

        with k2:
            st.metric("Noches", int(plan["noches_viaje"]))

        with k3:
            st.metric("Presupuesto ingresado", format_eur(presupuesto_usuario))

        with k4:
            st.metric("Costo estimado total", format_eur(total_estimado))

        st.divider()

        # ===============================
        # Resumen general del viaje
        # ===============================
        colA, colB = st.columns(2)

        with colA:
            st.markdown("### Información del viaje")
            st.write(f"**Correo:** {plan['email_usuario']}")
            st.write(f"**País origen:** {plan['pais_origen']}")
            st.write(f"**País destino:** {plan['pais_destino']}")

            provincia = plan.get("provincia_destino", None)
            if pd.isna(provincia) or provincia is None:
                st.write("**Provincia destino:** (no definida)")
            else:
                st.write(f"**Provincia destino:** {provincia}")

            st.write(f"**Fecha ida:** {plan['fecha_ida']}")
            st.write(f"**Fecha regreso:** {plan['fecha_regreso']}")

        with colB:
            st.markdown("### Preferencias del plan")
            st.write(f"**Tipo de viaje:** {plan['tipo_viaje']}")
            st.write(f"**Perfil de presupuesto:** {plan['perfil_presupuesto']}")
            st.write(f"**Hospedaje:** {plan['categoria_alojamiento']}")

            categorias = plan.get("categorias_actividad", "")
            if categorias:
                st.write(f"**Categorías deseadas:** {categorias}")
            else:
                st.write("**Categorías deseadas:** (no definidas)")

        st.divider()

        # ===============================
        # Estado del presupuesto
        # ===============================
        st.markdown("### Estado general del presupuesto")

        c1, c2, c3 = st.columns(3)

        with c1:
            st.metric("Presupuesto usuario", format_eur(presupuesto_usuario))

        with c2:
            st.metric("Costo total estimado", format_eur(total_estimado))

        with c3:
            st.metric(
                "Diferencia",
                format_eur(abs(diferencia)),
                "Dentro del presupuesto" if diferencia >= 0 else "Excedido"
            )

        if presupuesto_usuario > 0:
            pct_consumido = round((total_estimado / presupuesto_usuario) * 100, 2)
            st.progress(min(pct_consumido / 100, 1.0))
            st.caption(f"Consumo estimado del presupuesto: {pct_consumido:.2f}%")

        if diferencia >= 0:
            st.success(f"✅ El plan se mantiene dentro del presupuesto. Margen estimado: {format_eur(diferencia)}")
        else:
            st.warning(f"⚠️ El costo estimado supera el presupuesto en {format_eur(abs(diferencia))}")

        st.divider()

        # ===============================
        # Predicción ML alojamiento
        # ===============================
        resultado_ml_aloj = predecir_costo_alojamiento_ml(
            id_plan=plan_id,
            df_features_plan=df_ml_alojamiento_features
        )

        if not resultado_ml_aloj.get("ok"):
            st.warning(
                "No fue posible calcular la predicción ML de alojamiento; "
                "se mostrará el costo estimado actual como referencia."
            )
            st.caption(resultado_ml_aloj.get("mensaje", "Sin detalle adicional."))

        # ===============================
        # Comparación por categoría
        # ===============================
        st.markdown("### Comparación por categoría")
        st.caption(
            "Esta comparación muestra el presupuesto recomendado, la estimación inicial del plan "
             "y una estimación optimizada para el alojamiento."
        )

        col_kpi1, col_kpi2, col_kpi3 = st.columns(3)

        presupuesto_aloj_ine = float(resultado_ml_aloj.get("presupuesto_alojamiento_ine", 0) or 0)
        alojamiento_ml_estimado = (
            float(resultado_ml_aloj.get("alojamiento_ml_estimado", 0) or 0)
            if resultado_ml_aloj.get("ok")
            else alojamiento
        )
        diferencia_ml_aloj = float(resultado_ml_aloj.get("diferencia_alojamiento_ml", 0) or 0)
        alcanza_aloj = bool(resultado_ml_aloj.get("alcanza_alojamiento", False))

        with col_kpi1:
            st.metric("Presupuesto disponible para alojamiento", format_eur(presupuesto_aloj_ine))

        with col_kpi2:
            st.metric("Costo optimizado de alojamiento", format_eur(alojamiento_ml_estimado))

        with col_kpi3:
            if alcanza_aloj:
                st.success("🟢 El presupuesto de alojamiento ALCANZA para este viaje")
            else:
                st.error("🔴 El presupuesto de alojamiento NO ALCANZA para este viaje")
            st.caption(f"Diferencia estimada: {format_eur(diferencia_ml_aloj)}")

        if not row_presupuesto_cat_df.empty:
            pcat = row_presupuesto_cat_df.iloc[0]

            df_comparacion = construir_tabla_comparacion_categoria(
                row_costos=costos,
                row_presupuesto_cat=pcat,
                resultado_ml=resultado_ml_aloj
            )

            df_comparacion_show = df_comparacion.copy()
            cols_monto = [
                "Presupuesto recomendado (€)",
                "Estimación inicial (€)",
                "Estimación optimizada (€)"
            ]

            for col in cols_monto:
                df_comparacion_show[col] = df_comparacion_show[col].apply(format_eur)

            st.dataframe(df_comparacion_show, use_container_width=True, hide_index=True)

            total_ine = float(df_comparacion.iloc[-1]["Presupuesto recomendado (€)"])
            total_actual = float(df_comparacion.iloc[-1]["Estimación inicial (€)"])
            total_ml = float(df_comparacion.iloc[-1]["Estimación optimizada (€)"])

            st.info(
                f"Total recomendado: {format_eur(total_ine)} | "
                f"Total estimado: {format_eur(total_actual)} | "
                f"Total optimizado: {format_eur(total_ml)}"
            )
        else:
            st.info("No se encontró distribución de presupuesto por categoría para este plan.")

        st.divider()

        # ===============================
        # Calendario simple
        # ===============================
        st.markdown("### Calendario simple del viaje")

        fecha_ida = pd.to_datetime(plan["fecha_ida"])
        dias = int(plan["dias_viaje"])

        engine = get_engine()
        query_pref = text("""
            SELECT categoria, cantidad
            FROM culturatrip.fact_plan_viaje_preferencia
            WHERE id_plan = :plan_id
            ORDER BY categoria
        """)
        df_pref = pd.read_sql(query_pref, engine, params={"plan_id": plan_id})

        lista_actividades = []
        for _, row in df_pref.iterrows():
            lista_actividades.extend([row["categoria"]] * int(row["cantidad"]))

        calendario = []
        for i in range(dias):
            fecha = (fecha_ida + pd.Timedelta(days=i)).date()
            bloque_comida = "Desayuno + almuerzo + cena"

            if i < len(lista_actividades):
                actividad_texto = f"{bloque_comida} + actividad {lista_actividades[i]}"
            elif i == 0:
                actividad_texto = f"{bloque_comida} + llegada / paseo inicial"
            elif i == dias - 1:
                actividad_texto = f"{bloque_comida} + cierre del viaje / paseo libre"
            else:
                actividad_texto = f"{bloque_comida} + tiempo libre"

            calendario.append({
                "Día": i + 1,
                "Fecha": fecha,
                "Plan sugerido": actividad_texto
            })

        st.dataframe(pd.DataFrame(calendario), use_container_width=True, hide_index=True)

def pantalla_4():
        st.header("Plan financiero de tu viaje")

        plan_id = st.session_state.get("plan_seleccionado")

        if not plan_id:
            st.info("No hay un plan seleccionado.")
            st.caption("Primero crea un nuevo plan o selecciona uno en Gestión de planes.")
            return

        if df_plan_resumen.empty:
            st.info("Aún no hay planes guardados en la base de datos.")
            st.caption("Primero completa la Pantalla 2 y guarda un plan.")
            return

        # ===============================
        # Obtener plan más reciente
        # ===============================
        plan_df = df_plan_resumen[df_plan_resumen["id_plan"] == plan_id]

        if plan_df.empty:
            st.warning("No se encontró el plan seleccionado.")
            return

        plan = plan_df.iloc[0]

        row_costos = df_plan_costos[df_plan_costos["id_plan"] == plan_id]

        if row_costos.empty:
            st.info("No hay estimación de costos disponible para este plan.")
            return

        costos = row_costos.iloc[0]

        # ===============================
        # Variables base
        # ===============================
        presupuesto = float(plan["presupuesto_estimado"] or 0)
        dias_viaje = int(plan["dias_viaje"] or 0)

        alojamiento = float(costos.get("alojamiento_estimado", 0) or 0)
        alimentacion = float(costos.get("alimentacion_estimado", 0) or 0)
        actividades = float(costos.get("actividades_estimado", 0) or 0)
        servicios = float(costos.get("servicios_estimado", 0) or 0)
        otros = float(costos.get("otros_estimado", 0) or 0)
        transporte = float(costos.get("transporte_estimado", 0) or 0)

        total_estimado = float(costos.get("costo_total_estimado", 0) or 0)

        costo_por_dia = round(total_estimado / dias_viaje, 2) if dias_viaje > 0 else 0
        diferencia = round(presupuesto - total_estimado, 2)

        # ===============================
        # Cálculo ahorro mensual
        # ===============================
        fecha_ida = pd.to_datetime(plan["fecha_ida"]).date()
        hoy = date.today()

        dias_restantes = max((fecha_ida - hoy).days, 0)
        meses_restantes = max(math.ceil(dias_restantes / 30), 1)

        monto_a_ahorrar = max(total_estimado - presupuesto, 0)
        meta_mensual = round(monto_a_ahorrar / meses_restantes, 2) if meses_restantes > 0 else 0

        st.caption(f"Plan ID: {plan_id}")

        # ===============================
        # KPIs principales
        # ===============================
        k1, k2, k3, k4 = st.columns(4)

        with k1:
            st.metric(
                "Presupuesto disponible",
                f"€{presupuesto:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
            )

        with k2:
            st.metric(
                "Costo estimado total",
                f"€{total_estimado:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
            )

        with k3:
            st.metric(
                "Diferencia",
                f"€{abs(diferencia):,.2f}".replace(",", "X").replace(".", ",").replace("X", "."),
                "Dentro del presupuesto" if diferencia >= 0 else "Excedido"
            )

        with k4:
            st.metric(
                "Costo por día",
                f"€{costo_por_dia:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
            )

        st.divider()

        # ===============================
        # Indicador de riesgo presupuestario
        # ===============================
        st.subheader("Indicador de riesgo presupuestario")

        if presupuesto > 0:

            pct_consumido = round((total_estimado / presupuesto) * 100, 2)

            r1, r2, r3 = st.columns(3)

            with r1:
                st.metric("Presupuesto consumido", f"{pct_consumido:.2f}%")

            with r2:
                if diferencia >= 0:
                    st.metric(
                        "Margen disponible",
                        f"€{diferencia:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
                    )
                else:
                    st.metric(
                        "Exceso estimado",
                        f"€{abs(diferencia):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
                    )

            with r3:

                if pct_consumido <= 80:
                    nivel_riesgo = "🟢 Bajo riesgo"
                elif pct_consumido <= 100:
                    nivel_riesgo = "🟡 Ajustado"
                else:
                    nivel_riesgo = "🔴 Sobre presupuesto"

                st.metric("Nivel de riesgo", nivel_riesgo)

            st.progress(min(pct_consumido / 100, 1.0))

            if pct_consumido <= 80:
                st.success(
                    "El viaje presenta un nivel de riesgo bajo. El presupuesto estimado cubre el costo con suficiente margen."
                )
            elif pct_consumido <= 100:
                st.warning(
                    "El viaje se encuentra en una zona ajustada. Se recomienda revisar algunos gastos variables."
                )
            else:
                st.error(
                    "El costo estimado supera el presupuesto disponible. Se recomienda ajustar el plan o aumentar el presupuesto."
                )

        st.divider()

        # ===============================
        # Plan simple de ahorro
        # ===============================
        st.subheader("Plan de ahorro")

        if monto_a_ahorrar <= 0:
            st.success("No necesitas ahorrar más. Tu presupuesto actual ya cubre el viaje.")
        else:
            a1, a2, a3 = st.columns(3)

            with a1:
                st.metric("Días restantes", dias_restantes)

            with a2:
                st.metric("Meses restantes", meses_restantes)

            with a3:
                st.metric(
                    "Meta mensual de ahorro",
                    f"€{meta_mensual:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
                )

            ahorro_rows = []

            for i in range(1, meses_restantes + 1):
                ahorro_rows.append({
                    "Mes": f"Mes {i}",
                    "Meta de ahorro (€)": meta_mensual
                })

            df_ahorro = pd.DataFrame(ahorro_rows)
            st.dataframe(df_ahorro, use_container_width=True, hide_index=True)


def pantalla_5():
        st.header("Control Dinámico del Presupuesto")

        plan_id = st.session_state.get("plan_seleccionado")

        if not plan_id:
            st.info("No hay un plan seleccionado.")
            st.caption("Primero crea un nuevo plan o selecciona uno en Gestión de planes.")
            return

        if df_plan_resumen.empty:
            st.info("Aún no hay planes guardados en la base de datos.")
            st.caption("Primero completa la Pantalla 2 y guarda un plan.")
            return
        # ===============================
        # Obtener plan seleccionado
        # ===============================
        plan_df = df_plan_resumen[df_plan_resumen["id_plan"] == plan_id]

        if plan_df.empty:
            st.warning("No se encontró el plan seleccionado.")
            return

        plan = plan_df.iloc[0]
        row_costos = df_plan_costos[df_plan_costos["id_plan"] == plan_id]
        if row_costos.empty:
            st.info("No hay costos estimados para este plan.")
            return

        costos = row_costos.iloc[0]

        # ===============================
        # Costos estimados desde SQL
        # ===============================
        alojamiento = float(costos.get("alojamiento_estimado", 0) or 0)
        alimentacion = float(costos.get("alimentacion_estimado", 0) or 0)
        actividades = float(costos.get("actividades_estimado", 0) or 0)
        servicios = float(costos.get("servicios_estimado", 0) or 0)
        otros = float(costos.get("otros_estimado", 0) or 0)
        transporte = float(costos.get("transporte_estimado", 0) or 0)

        presupuesto_aprobado = float(costos.get("costo_total_estimado", 0) or 0)
        presupuesto_usuario = float(plan.get("presupuesto_estimado", 0) or 0)

        # ===============================
        # Gastos reales registrados
        # ===============================
        row_gasto = df_gasto_resumen[df_gasto_resumen["id_plan"] == plan_id]
        gasto_real_total = float(row_gasto["gasto_real_total"].iloc[0]) if not row_gasto.empty else 0.0

        diferencia_vs_estimado = round(presupuesto_aprobado - gasto_real_total, 2)
        diferencia_vs_usuario = round(presupuesto_usuario - gasto_real_total, 2)

        pct_ejecutado = round((gasto_real_total / presupuesto_aprobado) * 100, 2) if presupuesto_aprobado > 0 else 0

        # ===============================
        # Semáforo
        # ===============================
        if pct_ejecutado < 80:
            estado = "🟢 Dentro del presupuesto estimado"
            st.success(estado)
        elif pct_ejecutado <= 100:
            estado = "🟡 Cerca del límite estimado"
            st.warning(estado)
        else:
            estado = "🔴 Presupuesto estimado excedido"
            st.error(estado)

        # ===============================
        # KPIs principales
        # ===============================
        k1, k2, k3, k4 = st.columns(4)

        with k1:
            st.metric(
                "Costo estimado del plan",
                f"€{presupuesto_aprobado:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
            )

        with k2:
            st.metric(
                "Gastos registrados",
                f"€{gasto_real_total:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
            )

        with k3:
            st.metric(
                "Diferencia vs estimado",
                f"€{abs(diferencia_vs_estimado):,.2f}".replace(",", "X").replace(".", ",").replace("X", "."),
                "Disponible" if diferencia_vs_estimado >= 0 else "Excedido"
            )

        with k4:
            st.metric("% ejecutado", f"{pct_ejecutado:.1f}%")

        st.divider()
        # ===============================
        # Resumen comparativo
        # ===============================
        margen_vs_presupuesto = round(presupuesto_usuario - presupuesto_aprobado, 2)
        saldo_disponible_usuario = round(presupuesto_usuario - gasto_real_total, 2)

        c1, c2, c3 = st.columns(3)

        with c1:
            st.metric(
                "Presupuesto usuario",
                f"€{presupuesto_usuario:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
            )

        with c2:
            st.metric(
                "Margen vs presupuesto usuario",
                f"€{abs(margen_vs_presupuesto):,.2f}".replace(",", "X").replace(".", ",").replace("X", "."),
                "Disponible" if margen_vs_presupuesto >= 0 else "Insuficiente"
            )

        with c3:
            st.metric(
                "Saldo disponible del usuario",
                f"€{abs(saldo_disponible_usuario):,.2f}".replace(",", "X").replace(".", ",").replace("X", "."),
                "Disponible" if saldo_disponible_usuario >= 0 else "Excedido"
            )

        st.progress(min(pct_ejecutado / 100, 1.0))

        if diferencia_vs_estimado >= 0:
            st.info(
                f"Todavía has ejecutado menos gasto del previsto. "
                f"Te quedan {format_eur(diferencia_vs_estimado)} antes de alcanzar el costo estimado del plan."
            )
        else:
            st.warning(
                f"Ya superaste el costo estimado del plan en {format_eur(abs(diferencia_vs_estimado))}."
            )

        st.divider()

        # ===============================
        # Referencia de costos estimados por categoría
        # ===============================
        st.subheader("Referencia del plan estimado por categoría")

        df_estimado_cat = pd.DataFrame({
            "Categoría": [
                "Alojamiento",
                "Alimentación",
                "Actividades",
                "Servicios",
                "Otros",
                "Transporte"
            ],
            "Costo estimado (€)": [
                alojamiento,
                alimentacion,
                actividades,
                servicios,
                otros,
                transporte
            ]
        })

        st.dataframe(df_estimado_cat, use_container_width=True, hide_index=True)

        st.divider()

        # ===============================
        # Registro de gasto
        # ===============================
        st.subheader("Agregar gasto real")

        c1, c2 = st.columns(2)
        with c1:
            fecha_gasto = st.date_input("Fecha del gasto", value=date.today(), key="fecha_gasto_real")
            categoria_gasto = st.selectbox(
                "Categoría",
                options=["Transporte", "Alojamiento", "Actividades", "Alimentación", "Servicios", "Otros"],
                key="categoria_gasto_real"
            )
        with c2:
            descripcion_gasto = st.text_input(
                "Descripción",
                placeholder="Ej. Almuerzo en Madrid",
                key="descripcion_gasto_real"
            )
            monto_gasto = st.number_input(
                "Monto (€)",
                min_value=0.0,
                step=1.0,
                key="monto_gasto_real"
            )

        if st.button("➕ Registrar gasto", use_container_width=True):
            ok = guardar_gasto_real_db(
                id_plan=plan_id,
                fecha=fecha_gasto,
                categoria=categoria_gasto,
                descripcion=descripcion_gasto,
                monto=monto_gasto
            )
            if ok:
                st.cache_data.clear()
                st.success("Gasto registrado correctamente.")
                st.rerun()

        st.divider()

        # ===============================
        # Gasto real por categoría
        # ===============================
        st.subheader("Gasto real por categoría")

        df_cat_plan = df_gasto_categoria[df_gasto_categoria["id_plan"] == plan_id].copy()

        if df_cat_plan.empty:
            st.info("Todavía no hay gastos registrados para este plan.")
        else:
            chart = (
                alt.Chart(df_cat_plan)
                .mark_bar()
                .encode(
                    x=alt.X("categoria:N", title="Categoría"),
                    y=alt.Y("gasto_real_categoria:Q", title="Gasto real (€)"),
                    tooltip=["categoria", "gasto_real_categoria", "n_movimientos"]
                )
                .properties(height=320)
            )
            st.altair_chart(chart, use_container_width=True)

            st.dataframe(
                df_cat_plan[["categoria", "gasto_real_categoria", "n_movimientos"]],
                use_container_width=True,
                hide_index=True
            )

        st.divider()

        # ===============================
        # Últimos gastos registrados
        # ===============================
        st.subheader("Últimos gastos registrados")

        df_det_plan = df_gasto_detalle[df_gasto_detalle["id_plan"] == plan_id].copy()

        if df_det_plan.empty:
            st.info("No hay gastos registrados todavía.")
        else:
            df_det_plan = df_det_plan.sort_values(["fecha", "id_gasto"], ascending=[False, False]).head(10)
            st.dataframe(
                df_det_plan[["fecha", "descripcion", "categoria", "monto"]],
                use_container_width=True,
                hide_index=True
            )
def pantalla_6():
        st.header("🧳 Checklist de viaje")

        plan_id = st.session_state.get("plan_seleccionado")

        if not plan_id:
            st.info("No hay un plan seleccionado.")
            st.caption("Primero crea un nuevo plan o selecciona uno en Gestión de planes.")
            return

        # ===============================
        # Validación base
        # ===============================
        if df_plan_resumen.empty:
            st.info("Aún no hay planes guardados en la base de datos.")
            st.caption("Primero completa la Pantalla 2 y guarda un plan.")
            return


        # Tomamos el plan realmente seleccionado
        plan_df = df_plan_resumen[df_plan_resumen["id_plan"] == plan_id]

        if plan_df.empty:
            st.warning("No se encontró el plan seleccionado.")
            return

        plan = plan_df.iloc[0]
        plan_id = int(plan["id_plan"])

        st.subheader(f"ID del plan: {plan_id}")

        pais_destino = plan["pais_destino"]
        provincia_destino = plan["provincia_destino"] if not pd.isna(plan["provincia_destino"]) else ""
        dias_viaje = int(plan["dias_viaje"])
        categorias = plan["categorias_actividad"] if not pd.isna(plan["categorias_actividad"]) else ""

        st.caption(
            f"Destino: {pais_destino} | Provincia: {provincia_destino} | Duración: {dias_viaje} días"
        )

        # ===============================
        # Cargar checklist guardado desde DB
        # ===============================
        df_checklist_guardado = load_checklist_plan(plan_id)

        estado_guardado = {}
        if not df_checklist_guardado.empty:
            estado_guardado = {
                (row["seccion"], row["item"]): bool(row["completado"])
                for _, row in df_checklist_guardado.iterrows()
            }

        # ===============================
        # Cantidades recomendadas simples
        # ===============================
        muda_base = max(dias_viaje, 1)
        ropa_ligera = max(round(dias_viaje * 0.5), 2)
        ropa_interior = dias_viaje + 1

        # ===============================
        # Checklist ampliado
        # ===============================
        checklist = {
            "Importante": [
                {"item": "Pasaporte / documento de identidad"},
                {"item": "Documentos de viaje (reservas / actividades)"},
                {"item": "Dinero"},
                {"item": "Billetera"},
                {"item": "Licencia de conducir"},
                {"item": "Seguro de viaje"},
                {"item": "Copia de seguro"},
                {"item": "Copia de pasaporte"},
                {"item": "Copia de cédula"},
                {"item": "Notificar salida del país al banco"},
                {"item": "Tarjeta de crédito / débito"},
                {"item": "Confirmación de vuelo"},
            ],
            "Tecnología": [
                {"item": "Celular"},
                {"item": "Cámara"},
                {"item": "Cargador"},
                {"item": "Powerbank / cargador portátil"},
                {"item": "Audífonos"},
                {"item": "Cargador de cámara"},
                {"item": "Roaming / eSIM"},
            ],
            "Higiene": [
                {"item": "Cepillo dental y pasta dental"},
                {"item": "Cepillo para el cabello"},
                {"item": "Accesorios para el cabello"},
                {"item": "Shampoo / acondicionador"},
                {"item": "Jabón corporal"},
                {"item": "Crema corporal"},
                {"item": "Repelente"},
                {"item": "Bloqueador solar"},
                {"item": "Desodorante"},
                {"item": "Perfume"},
                {"item": "Alcohol en gel"},
                {"item": "Bolsa de maquillaje"},
                {"item": "Skincare"},
                {"item": "Medicamentos"},
            ],
            "Ropa para el viaje": [
                {"item": "Vestimenta Completa", "cantidad": ropa_ligera},
                {"item": "Sueter", "cantidad": 1},
                {"item": "Sandalias", "cantidad": 1},
                {"item": "Ropa de playa", "cantidad": 1},
                {"item": "Ropa interior", "cantidad": ropa_interior},
                {"item": "Pijama", "cantidad": 1},
                {"item": "Medias", "cantidad": max(dias_viaje - 1, 1)},
                {"item": "Tenis", "cantidad": 1},
                {"item": "Zapatos", "cantidad": 1},
            ],
            "Accesorios": [
                {"item": "Bolso"},
                {"item": "Anteojos de sol"},
                {"item": "Bisutería"},
                {"item": "Reloj"},
            ],
            "Otros": [
                {"item": "Libro"},
                {"item": "Snacks"},
                {"item": "Botella para agua"},
                {"item": "Toalla de playa"},
            ],
        }

        # ===============================
        # Extras según categoría
        # ===============================
        cat_lower = categorias.lower()
        extras = []

        if "cultura" in cat_lower:
            extras.extend([
                {"item": "Libreta para apuntes o recuerdos"},
                {"item": "Calzado cómodo para caminatas urbanas"},
                {"item": "Espacio libre en celular o cámara"},
            ])

        if "naturaleza" in cat_lower:
            extras.extend([
                {"item": "Chaqueta ligera"},
                {"item": "Ropa deportiva"},
                {"item": "Botella extra de agua"},
            ])

        if "gastronom" in cat_lower:
            extras.extend([
                {"item": "Presupuesto extra para comidas"},
                {"item": "Ropa casual para restaurantes"},
            ])

        if dias_viaje >= 7:
            extras.append({"item": "Recambio adicional de ropa"})
            extras.append({"item": "Bolsa para ropa sucia"})

        checklist["Extras recomendados"] = extras if extras else [{"item": "Sin recomendaciones adicionales por ahora"}]

        # ===============================
        # Render en 2 columnas (orden manual)
        # ===============================
        total_items = 0
        total_checked = 0

        col1_order = ["Importante", "Tecnología", "Higiene"]
        col2_order = ["Ropa para el viaje", "Accesorios", "Otros", "Extras recomendados"]

        col1_sections = [(k, checklist[k]) for k in col1_order if k in checklist]
        col2_sections = [(k, checklist[k]) for k in col2_order if k in checklist]

        col1, col2 = st.columns(2, gap="large")

        def render_secciones(lista_secciones):
            nonlocal total_items, total_checked

            for seccion, items in lista_secciones:
                st.subheader(seccion)

                for registro in items:
                    item = registro["item"]
                    cantidad = registro.get("cantidad", None)

                    key_item = f"check_{plan_id}_{seccion}_{item}"
                    texto = item if cantidad is None else f"{item} (Cantidad recomendada: {cantidad})"

                    # Inicializar session_state con el valor guardado en DB
                    if key_item not in st.session_state:
                        st.session_state[key_item] = estado_guardado.get((seccion, item), False)

                    checked = st.checkbox(
                        texto,
                        key=key_item
                    )

                    total_items += 1
                    if checked:
                        total_checked += 1

                st.divider()

        with col1:
            render_secciones(col1_sections)

        with col2:
            render_secciones(col2_sections)

        # ===============================
        # Resumen de progreso
        # ===============================
        pct = round((total_checked / total_items) * 100, 2) if total_items > 0 else 0

        st.subheader("Progreso del checklist")

        c1, c2, c3 = st.columns(3)

        with c1:
            st.metric("Total de artículos", total_items)

        with c2:
            st.metric("Completados", total_checked)

        with c3:
            st.metric("Progreso", f"{pct}%")

        st.progress(min(pct / 100, 1.0))

        if pct == 100:
            st.success("✅ Checklist completado.")
        elif pct >= 50:
            st.info("Vas avanzando bien con tu preparación.")
        else:
            st.warning("Aún faltan varios elementos por preparar.")

        # ===============================
        # Guardar checklist
        # ===============================
        if st.button("💾 Guardar checklist", use_container_width=True):
            for seccion, items in checklist.items():
                for registro in items:
                    item = registro["item"]
                    key_item = f"check_{plan_id}_{seccion}_{item}"
                    completado = st.session_state.get(key_item, False)

                    guardar_checklist_item_db(
                        id_plan=plan_id,
                        seccion=seccion,
                        item=item,
                        completado=completado
                    )

            st.cache_data.clear()
            st.success("Checklist guardado correctamente.")


def pantalla_7():

        st.header("Resumen final del viaje")

        plan_id = st.session_state.get("plan_seleccionado")

        if not plan_id:
            st.info("No hay un plan seleccionado.")
            st.caption("Primero crea un nuevo plan o selecciona uno en Gestión de planes.")
            return

        if df_plan_resumen.empty:
            st.info("Aún no hay planes guardados en la base de datos.")
            st.caption("Primero completa la Pantalla 2 y guarda un plan.")
            return

        # ===============================
        # Plan seleccionado
        # ===============================
        plan_df = df_plan_resumen[df_plan_resumen["id_plan"] == plan_id]

        if plan_df.empty:
            st.warning("No se encontró el plan seleccionado.")
            return

        plan = plan_df.iloc[0]
        plan_id = int(plan["id_plan"])

        row_costos = df_plan_costos[df_plan_costos["id_plan"] == plan_id]

        alojamiento = 0.0
        alimentacion = 0.0
        actividades = 0.0
        servicios = 0.0
        otros = 0.0
        transporte = 0.0
        total_estimado = 0.0
        diferencia = 0.0

        if not row_costos.empty:
            costos = row_costos.iloc[0]

            alojamiento = float(costos.get("alojamiento_estimado", 0) or 0)
            alimentacion = float(costos.get("alimentacion_estimado", 0) or 0)
            actividades = float(costos.get("actividades_estimado", 0) or 0)
            servicios = float(costos.get("servicios_estimado", 0) or 0)
            otros = float(costos.get("otros_estimado", 0) or 0)
            transporte = float(costos.get("transporte_estimado", 0) or 0)
            total_estimado = float(costos.get("costo_total_estimado", 0) or 0)

            diferencia = float(plan["presupuesto_estimado"]) - total_estimado

        # ===============================
        # Checklist guardado desde DB
        # ===============================
        df_checklist_guardado = load_checklist_plan(plan_id)

        total_items_checklist = 0
        total_checked_checklist = 0

        if not df_checklist_guardado.empty:
            total_items_checklist = len(df_checklist_guardado)
            total_checked_checklist = int(df_checklist_guardado["completado"].sum())

        pct = round(
            (total_checked_checklist / total_items_checklist) * 100, 2
        ) if total_items_checklist > 0 else 0

        # ===============================
        # Bloque principal
        # ===============================
        c1, c2, c3 = st.columns(3)

        with c1:
            st.metric("ID del plan", plan_id)

        with c2:
            st.metric("Días de viaje", int(plan["dias_viaje"]))

        with c3:
            st.metric(
                "Presupuesto",
                f"€{float(plan['presupuesto_estimado']):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
            )

        st.divider()

        # ===============================
        # Información del viaje
        # ===============================
        col1, col2 = st.columns(2)

        with col1:
            st.subheader("Destino y fechas")
            st.write(f"**País origen:** {plan['pais_origen']}")
            st.write(f"**País destino:** {plan['pais_destino']}")

            provincia = plan["provincia_destino"] if not pd.isna(plan["provincia_destino"]) else "(no definida)"
            st.write(f"**Provincia destino:** {provincia}")

            st.write(f"**Fecha ida:** {plan['fecha_ida']}")
            st.write(f"**Fecha regreso:** {plan['fecha_regreso']}")

        with col2:
            st.subheader("Preferencias del plan")
            st.write(f"**Tipo de viaje:** {plan['tipo_viaje']}")
            st.write(f"**Perfil de presupuesto:** {plan['perfil_presupuesto']}")
            st.write(f"**Hospedaje:** {plan['categoria_alojamiento']}")

            categorias = plan["categorias_actividad"] if not pd.isna(plan["categorias_actividad"]) else ""
            if categorias:
                st.write(f"**Actividades:** {categorias}")
            else:
                st.write("**Actividades:** (no definidas)")

        st.divider()

        # ===============================
        # Resumen financiero
        # ===============================
        st.subheader("Resumen financiero")

        k1, k2, k3, k4 = st.columns(4)

        with k1:
            st.metric("Alojamiento", f"€{alojamiento:,.2f}".replace(",", "X").replace(".", ",").replace("X", "."))
        with k2:
            st.metric("Alimentación", f"€{alimentacion:,.2f}".replace(",", "X").replace(".", ",").replace("X", "."))
        with k3:
            st.metric("Actividades", f"€{actividades:,.2f}".replace(",", "X").replace(".", ",").replace("X", "."))
        with k4:
            st.metric("Transporte", f"€{transporte:,.2f}".replace(",", "X").replace(".", ",").replace("X", "."))

        k5, k6, k7 = st.columns(3)

        with k5:
            st.metric("Servicios", f"€{servicios:,.2f}".replace(",", "X").replace(".", ",").replace("X", "."))
        with k6:
            st.metric("Otros", f"€{otros:,.2f}".replace(",", "X").replace(".", ",").replace("X", "."))
        with k7:
            st.metric("Total estimado", f"€{total_estimado:,.2f}".replace(",", "X").replace(".", ",").replace("X", "."))

        if total_estimado > 0:
            if diferencia >= 0:
                st.success(
                    f"✅ Tu viaje se mantiene dentro del presupuesto. Margen estimado: "
                    f"€{diferencia:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
                )
            else:
                st.warning(
                    f"⚠️ Tu viaje supera el presupuesto por "
                    f"€{abs(diferencia):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
                )

        st.divider()

        # ===============================
        # Estado checklist desde base de datos
        # ===============================
        st.subheader("Estado de preparación")

        r1, r2, r3 = st.columns(3)

        with r1:
            st.metric("Total artículos", total_items_checklist)
        with r2:
            st.metric("Completados", total_checked_checklist)
        with r3:
            st.metric("Progreso checklist", f"{pct}%")

        st.progress(min(pct / 100, 1.0))

        if total_items_checklist == 0:
            st.info("Aún no hay checklist guardado para este plan.")
        elif pct == 100:
            st.success("✅ Todo listo. El checklist está completo.")
        elif pct >= 50:
            st.info("Vas bien. Tu preparación está avanzada.")
        else:
            st.warning("Aún faltan varios elementos del checklist.")

        st.divider()

        # ===============================
        # Estado final del plan
        # ===============================
        st.subheader("Estado final del plan")

        if total_estimado > 0 and diferencia >= 0 and pct >= 50:
            st.success("Tu plan está bastante sólido: presupuesto controlado y buena preparación.")
        elif total_estimado > 0 and diferencia < 0:
            st.warning("Revisa el presupuesto antes de finalizar el viaje.")
        elif pct < 50:
            st.warning("Antes del viaje conviene completar mejor la preparación del checklist.")
        else:
            st.info("Completa más información para tener un cierre de plan más completo.")

        b1, b2 = st.columns(2)

        with b1:
            st.button("📄 Descargar resumen (próximamente)", use_container_width=True)

        with b2:
            st.button("📤 Compartir plan (próximamente)", use_container_width=True)

# ===============================
# Pantalla Nuestro Equipo
# ===============================
# ===============================
# Pantalla Nuestro Equipo
# ===============================
def pantalla_equipo():
    import base64 as _b64_team

    # --- Helper: cargar foto o generar avatar SVG ---
    def _foto_o_avatar(foto_filename, iniciales, color):
        """Intenta cargar la foto desde team_members/. Si no existe, genera avatar SVG."""
        if foto_filename:
            foto_path = BASE_DIR / "team_members" / foto_filename
            if foto_path.exists() and foto_path.is_file():
                try:
                    img_bytes = foto_path.read_bytes()
                    img_b64 = _b64_team.b64encode(img_bytes).decode("utf-8")
                    ext = foto_path.suffix.lower().replace(".", "").replace("jfif", "jpeg").replace("jpg", "jpeg")
                    return (
                        f'<img src="data:image/{ext};base64,{img_b64}" '
                        f'style="width:100px; height:100px; border-radius:50%; object-fit:cover; '
                        f'border:3px solid {color}; box-shadow:0 4px 12px rgba(0,74,173,0.12);"/>'
                    )
                except Exception:
                    pass  # fallback al avatar SVG

        # Fallback: avatar con iniciales
        txt_color = "#004AAD" if color in ("#FFDE59", "#4BFA00") else "#FFFFFF"
        return (
            f'<svg width="100" height="100" viewBox="0 0 100 100" xmlns="http://www.w3.org/2000/svg">'
            f'<circle cx="50" cy="50" r="48" fill="{color}" stroke="white" stroke-width="3"/>'
            f'<text x="50" y="50" text-anchor="middle" dominant-baseline="central" '
            f'font-family="Nunito,sans-serif" font-size="34" font-weight="800" fill="{txt_color}">'
            f'{iniciales}</text></svg>'
        )

    # --- Datos del equipo ---
    EQUIPO = [
        {
            "nombre": "Ronald Rojas Barquero",
            "iniciales": "RR",
            "foto": "1739285482616.jfif",
            "color": "#004AAD",
            "rol_actual": "Global People Optimisation Portfolio Lead · Smith+Nephew",
            "rol_ct": "Scrum Master & Data Engineering Lead",
            "experiencia_anios": 11,
            "descripcion": (
                "Ingeniero Industrial con más de 11 años de experiencia en efectividad operacional "
                "y eficiencia de procesos. Especialista en Big Data y Business Intelligence aplicados "
                "a la optimización de decisiones."
            ),
            "linkedin": "https://www.linkedin.com/in/ronald-rojas-64a722143/",
        },
        {
            "nombre": "Andrea Lucía Morales Vega",
            "iniciales": "AM",
            "foto": "1730839721600.jfif",
            "color": "#38B6FF",
            "rol_actual": "Associate Manager Business Operations · DXC Technology",
            "rol_ct": "Product Owner & Strategic Planning Lead",
            "experiencia_anios": 10,
            "descripcion": (
                "Ingeniera Industrial con más de 10 años de experiencia en análisis de datos, mejora de procesos y gestión "
                "de proyectos. Experta en comunicación de negocios, gestión de stakeholders y "
                "coordinación de entregables con Power BI."
            ),
            "linkedin": "https://www.linkedin.com/in/andrea-morales-04804870/",
        },
        {
            "nombre": "Ana Belén Chaves Jiménez",
            "iniciales": "AC",
            "foto": "IMG_20210906_181700.jpeg",
            "color": "#3DCD00",
            "rol_actual": "Nuclear Medicine Technologist · Hospital San Juan de Dios | Profesora · UCR",
            "rol_ct": "Quality Assurance & Process Documentation Lead",
            "experiencia_anios": 12,
            "descripcion": (
                "Tecnóloga en Imágenes Médicas y Máster en Dirección Estratégica de Organizaciones "
                "de Salud. Más de 12 años de experiencia clínica y académica. Certificada en Scrum "
                "Master y Product Owner."
            ),
            "linkedin": "https://www.linkedin.com/in/ana-bel%C3%A9n-chaves-52340b93/",
        },
        {
            "nombre": "Montserrat Ulloa Álvarez",
            "iniciales": "MU",
            "foto": "1699314433125.jfif",
            "color": "#FFDE59",
            "rol_actual": "Analista de Datos Global · Ferretería EPA",
            "rol_ct": "Business Intelligence Analyst & Data Visualization Lead",
            "experiencia_anios": 3,
            "descripcion": (
                "Analista de datos especializada en R, Python y SQL con experiencia en visualización, "
                "modelado estadístico y automatización de reportes. Licenciada en Ingeniería Industrial. "
                "Bilingüe con conocimientos de portugués."
            ),
            "linkedin": "https://www.linkedin.com/in/montserrat-ulloa-7b8950261/",
        },
        {
            "nombre": "Hilda Mireya Ibarra Mata",
            "iniciales": "HI",
            "foto": "1764462404757.jfif",
            "color": "#4BFA00",
            "rol_actual": "Master Data Analyst · CEMEX",
            "rol_ct": "UX/Branding Designer & Frontend Developer",
            "experiencia_anios": 8,
            "descripcion": (
                "Más de 8 años de experiencia en CEMEX en análisis de datos maestros y administración "
                "de información. Licenciada en Mercadotecnia con Máster en Big Data y Business "
                "Intelligence."
            ),
            "linkedin": "https://www.linkedin.com/in/hilda-mireya-ibarra-27b371113/",
        },
    ]

    # --- Header ---
    st.markdown(
        '<div style="text-align:center; margin-bottom:8px;">'
        '<span style="display:inline-block; padding:6px 16px; border-radius:999px; '
        'background:var(--ct-azul-tint, #E6F4FF); color:var(--ct-azul-oscuro, #004AAD); '
        'font-weight:700; font-size:13px; letter-spacing:0.3px; '
        'border:1px solid rgba(56,182,255,0.3);">'
        '👥 Equipo CulturaTrip</span></div>',
        unsafe_allow_html=True,
    )
    st.markdown(
        '<h1 style="text-align:center; color:var(--ct-azul-oscuro, #004AAD); '
        'font-weight:900; font-size:42px; margin-bottom:4px;">Conoce a nuestro equipo</h1>',
        unsafe_allow_html=True,
    )
    st.markdown(
        '<p style="text-align:center; color:var(--ct-texto-suave, #5A6B85); '
        'font-size:18px; margin-bottom:32px;">'
        'El talento detrás de CulturaTrip — ingeniería, datos y pasión por el turismo cultural.'
        '</p>',
        unsafe_allow_html=True,
    )

    # --- LinkedIn icon SVG (reutilizable) ---
    li_icon = (
        '<svg viewBox="0 0 24 24" width="16" height="16" style="vertical-align:middle;">'
        '<path d="M19 0h-14c-2.76 0-5 2.24-5 5v14c0 2.76 2.24 5 5 5h14c2.76 0 5-2.24 '
        '5-5v-14c0-2.76-2.24-5-5-5zm-11 19h-3v-10h3v10zm-1.5-11.27c-.97 0-1.75-.79-1.75'
        '-1.76s.78-1.76 1.75-1.76 1.75.79 1.75 1.76-.78 1.76-1.75 1.76zm13.5 11.27h-3v-5.34'
        'c0-1.27-.02-2.91-1.77-2.91-1.77 0-2.04 1.38-2.04 2.81v5.44h-3v-10h2.88v1.37h.04'
        'c.4-.76 1.38-1.56 2.84-1.56 3.04 0 3.6 2 3.6 4.59v5.6z" fill="#004AAD"/></svg>'
    )

    # --- Generar las 5 cards ---
    cards_html = '<div style="display:flex; flex-wrap:wrap; gap:22px; justify-content:center;">'

    for m in EQUIPO:
        avatar_html = _foto_o_avatar(m["foto"], m["iniciales"], m["color"])

        cards_html += (
            f'<div style="background:white; border-radius:20px; padding:28px 22px 22px; '
            f'width:210px; text-align:center; '
            f'box-shadow:0 4px 20px rgba(0,74,173,0.08); '
            f'border:1px solid var(--ct-borde, #D7E3F4); '
            f'border-top:4px solid {m["color"]}; '
            f'display:flex; flex-direction:column; align-items:center; gap:6px;">'

            # Foto / avatar
            f'<div style="margin-bottom:6px;">{avatar_html}</div>'

            # Nombre
            f'<div style="font-size:17px; font-weight:800; color:var(--ct-azul-oscuro, #004AAD); '
            f'line-height:1.2;">{m["nombre"]}</div>'

            # Rol en CulturaTrip (badge)
            f'<div style="margin:4px 0;">'
            f'<span style="display:inline-block; padding:4px 10px; border-radius:999px; '
            f'font-size:11px; font-weight:700; letter-spacing:0.2px; '
            f'background:{m["color"]}20; color:{m["color"]}; '
            f'border:1px solid {m["color"]}30;">'
            f'{m["rol_ct"].split("·")[0].strip()}</span></div>'

            # Descripción
            f'<div style="font-size:12.5px; line-height:1.5; color:var(--ct-texto-suave, #5A6B85); '
            f'margin:6px 0; min-height:80px;">{m["descripcion"]}</div>'

            # Años de experiencia
            f'<div style="font-size:12px; font-weight:700; color:var(--ct-azul-oscuro, #004AAD); '
            f'margin:2px 0;">⏱ {m["experiencia_anios"]} años de experiencia</div>'

            # Rol actual
            f'<div style="font-size:11px; color:var(--ct-texto-suave, #5A6B85); '
            f'line-height:1.3; margin:2px 0; font-style:italic;">{m["rol_actual"]}</div>'

            # Rol CulturaTrip detallado
            f'<div style="font-size:11px; color:var(--ct-azul-oscuro, #004AAD); '
            f'line-height:1.3; margin:2px 0; font-weight:600;">'
            f'CulturaTrip: {m["rol_ct"]}</div>'

            # LinkedIn
            f'<a href="{m["linkedin"]}" target="_blank" '
            f'style="display:inline-flex; align-items:center; gap:5px; margin-top:8px; '
            f'padding:6px 14px; border-radius:999px; font-size:12px; font-weight:700; '
            f'color:var(--ct-azul-oscuro, #004AAD); text-decoration:none; '
            f'background:var(--ct-azul-tint, #E6F4FF); '
            f'border:1px solid rgba(56,182,255,0.3); transition:all 0.2s;">'
            f'{li_icon} LinkedIn</a>'

            f'</div>'
        )

    cards_html += '</div>'

    st.markdown(cards_html, unsafe_allow_html=True)

    # --- Nota al pie ---
    st.markdown("<div style='height:24px;'></div>", unsafe_allow_html=True)
    st.markdown(
        '<p style="text-align:center; font-size:13px; color:var(--ct-texto-suave, #5A6B85);">'
        'Proyecto académico · Máster en Big Data & Business Intelligence · 2025-2026'
        '</p>',
        unsafe_allow_html=True,
    )
# ===============================
# Router
# ===============================
if st.session_state.step == 1:
    pantalla_1()
elif st.session_state.step == 2:
    pantalla_gestion_planes()
elif st.session_state.step == 3:
    pantalla_2()
elif st.session_state.step == 4:
    pantalla_3()
elif st.session_state.step == 5:
    pantalla_4()
elif st.session_state.step == 6:
    pantalla_5()
elif st.session_state.step == 7:
    pantalla_6()
elif st.session_state.step == 8:
    pantalla_7()
elif st.session_state.step == 9:
    pantalla_equipo()

