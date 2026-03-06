import streamlit as st
import pandas as pd
from pathlib import Path
import altair as alt
import os
from sqlalchemy import create_engine, text
from datetime import date

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
    categoria_act = st.session_state.get("categoria_actividad")  # 1 categoría por ahora

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
                tipo_viaje, categoria_alojamiento
            )
            VALUES (
                :email, :id_pais_origen, :id_pais_destino,
                :fecha_ida, :fecha_regreso, :presupuesto,
                :tipo_viaje, :categoria_aloj
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
            "categoria_aloj": categoria_aloj
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

        # Insert preferencia (1 categoría por ahora)
        if categoria_act:
            sql_pref = text("""
                INSERT INTO culturatrip.fact_plan_viaje_preferencia (id_plan, categoria)
                VALUES (:id_plan, :categoria)
                ON CONFLICT (id_plan, categoria) DO NOTHING;
            """)
            conn.execute(sql_pref, {"id_plan": plan_id, "categoria": categoria_act})

    return plan_id

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
# Configuración de la página
# =========================
st.set_page_config(page_title="CulturaTrip", layout="wide",
                   initial_sidebar_state="expanded" )  # ✅ fuerza sidebar abierto
# =========================
# Estilos generales (layout moderno)
# =========================
st.markdown(
    """
    <style>

    /* contenedor central */
    .main-container {
        max-width: 1100px;
        margin: auto;
    }

    /* badge superior */
    .badge {
        display:inline-block;
        padding:8px 14px;
        border-radius:999px;
        background:#EEF2FF;
        color:#2563EB;
        font-weight:600;
        font-size:14px;
    }

    /* titulo principal */
    .title-main {
        text-align:center;
        font-size:100px;
        font-weight:800;
        margin-top:10px;
    }

    /* subtitulo */
    .subtitle-main {
        text-align:center;
        color:#6b7280;
        font-size:22px;
        margin-bottom:30px;
    }

    /* card bienvenida */
    .card {
        background:white;
        border-radius:14px;
        padding:16px;
        text-align:center;
        box-shadow:0px 10px 25px rgba(0,0,0,0.05);
        border:1px solid rgba(0,0,0,0.05);
    }

    .card-title{
        font-size:40px;
        font-weight:800;
        margin-bottom:10px;
    }

    .card-text{
        color:#6b7280;
        font-size:20px;
        line-height:1.6;
    }

    /* selectbox grande */
    div[data-baseweb="select"] > div {
        font-size:18px !important;
        min-height:10px;
    }

    label{
        font-size:12px !important;
        font-weight:600;
    }

    </style>
    """,
    unsafe_allow_html=True
)

# =========================
# Header principal
# =========================

st.markdown('<div class="main-container">', unsafe_allow_html=True)

st.markdown(
    '<div style="text-align:center;"><span class="badge">✨ Exploración Cultural</span></div>',
    unsafe_allow_html=True
)

st.markdown(
    '<div class="title-main">CulturaTrip</div>',
    unsafe_allow_html=True
)

st.markdown(
    '<div class="subtitle-main">Planificación inteligente de turismo</div>',
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

# Views para la pagina_1
df_dropdown_paises = load_view("vw_ui_dropdown_paises")
df_pantalla1_global = load_view("vw_ui_pantalla1_global")
df_pantalla1_detalle = load_view("vw_ui_pantalla1_detalle_por_pais")
df_total_paises = load_view("vw_ui_total_paises")

# Views para la pagina_2
df_dropdown_provincias = load_view("vw_ui_dropdown_provincias_por_pais")
df_dropdown_cat_aloj = load_view("vw_ui_dropdown_categoria_alojamiento")
df_dropdown_cat_act = load_view("vw_ui_dropdown_categoria_actividad")

df_rec_act = load_view("vw_rec_actividades_por_provincia")
df_rec_aloj = load_view("vw_rec_alojamiento_precio_provincia")


# Views para la pagina_3

df_plan_resumen = load_view("vw_plan_resumen_basico")
df_plan_costos = load_view("vw_plan_costos_estimados")

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

        # Provincia destino (Pantalla 2)
        "provincia_destino": None,     # nombre provincia
        "id_provincia_destino": None,  # VARCHAR(2)

        # Usuario/correo
        "email": "",

        # Fechas
        "fecha_ida": None,
        "fecha_regreso": None,

        # Presupuesto
        "presupuesto": 0,

        # Selecciones
        "categoria_alojamiento": None,
        "categoria_actividad": None,
        "tipo_viaje": "Solo",

        "guardando": False,
        "plan_guardado": False,
        "ultimo_plan_id": None,
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

        # Usuario
        "email": "",

        # Fechas
        "fecha_ida": None,
        "fecha_regreso": None,

        # Presupuesto y preferencias
        "presupuesto": 0,
        "categoria_alojamiento": None,
        "categoria_actividad": None,
        "tipo_viaje": "Solo",

        # Control de guardado
        "plan_guardado": False,
        "guardando": False,
        "ultimo_plan_id": None,
    }

    for k, v in keys_to_reset.items():
        st.session_state[k] = v

# ===============================
# Sidebar navegación (nuevo layout)
# ===============================
with st.sidebar:
    st.title("CulturaTrip")
    st.caption("Planificación inteligente de turismo")

    opciones = [
        "Exploración Cultural",
        "Planificación",
        "Itinerario",
        "Checklist",
        "Presupuesto",
        "Actividades",
        "Resumen final",
    ]

    # Mantiene sincronía con step (1..7)
    default_index = max(0, min(len(opciones) - 1, st.session_state.step - 1))

    menu = st.radio("Menú", opciones, index=default_index)

    st.session_state.menu = menu
    st.session_state.step = opciones.index(menu) + 1  # ✅ actualiza step para tu router viejo

    st.divider()
    st.markdown("### Acciones rápidas")
    st.button("↩️ Regresar a editar")
    st.button("🧾 Ajustar presupuesto")
    st.button("🗺️ Modificar itinerario")
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


# ===============================
# Pantalla 1
# ===============================
def pantalla_1():

    # Reset completo
    col_reset_left, col_reset_right = st.columns([6, 2])
    with col_reset_right:
        if st.button("🔄 Nuevo plan", use_container_width=True):
            reset_plan_completo()
            st.session_state["step"] = 1
            st.rerun()

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

    pais_ui = st.selectbox(
        label="",
        options=lista_paises_ui,
        index=index_default,
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
    DATOS_CULTURALES = {
        "España": [
            {"titulo": "🏺 Antigüedad", "texto": "España fue uno de los primeros territorios europeos explotados por metales por fenicios y romanos."},
            {"titulo": "🏰 Granada", "texto": "Fue el último reino musulmán de la Península Ibérica hasta 1492."},
            {"titulo": "🕌 Córdoba", "texto": "En el siglo X fue una de las ciudades más grandes del mundo occidental."},
            {"titulo": "🏛️ Mérida", "texto": "Fue una de las capitales romanas más importantes fuera de Italia."},
            {"titulo": "🗡️ Toledo", "texto": "Durante siglos convivieron cristianos, judíos y musulmanes."},
            {"titulo": "⛪ Santiago", "texto": "El Camino de Santiago es una de las rutas de peregrinación más antiguas."},
            {"titulo": "🌍 Sevilla", "texto": "Desde aquí se gestionaba el comercio con América."},
            {"titulo": "🗣️ Idiomas", "texto": "Existen lenguas cooficiales como catalán, gallego y euskera."},
            {"titulo": "🍷 Gastronomía", "texto": "Las tapas nacieron como una forma de cubrir bebidas."},
        ],
        "Italia": [
            {"titulo": "🏛️ Roma", "texto": "Centro del Imperio Romano durante siglos."}
        ],
        "Francia": [
            {"titulo": "🗼 París", "texto": "Uno de los centros culturales y artísticos más importantes de Europa."}
        ],
    }

    IMAGEN_PAIS = {
        "España": BASE_DIR / "assets" / "spain_map.jfif",
        "Italia": BASE_DIR / "assets" / "italia.jpg",
        "Francia": BASE_DIR / "assets" / "francia.jpg",
    }

    st.subheader("Destacados culturales del país")

    col_izq, col_der = st.columns([2, 1], gap="medium")

    with col_izq:

        if pais in DATOS_CULTURALES:

            for d in DATOS_CULTURALES[pais]:

                st.markdown(
                    f"""
                    <div style="margin-bottom:14px;">
                        <div style="font-size:18px; font-weight:700;">
                            {d["titulo"]}
                        </div>
                        <div style="font-size:16px; line-height:1.6; color:#374151;">
                            {d["texto"]}
                        </div>
                    </div>
                    """,
                    unsafe_allow_html=True
                )
        else:
            st.info("No hay datos culturales para este país todavía")

    with col_der:

        img_path = IMAGEN_PAIS.get(pais)

        if img_path and img_path.exists():
            st.image(str(img_path), use_container_width=True)
        else:
            st.info("Aún no hay imagen para este país.")

    st.divider()

# ===============================
# Pantalla 2
# ===============================
def pantalla_2():

    st.header("🧭 Planifica tu viaje")

    colA, colB = st.columns(2)

    # ===============================
    # Columna izquierda
    # ===============================
    with colA:

        st.subheader("Origen y destino")

        # País origen
        pais_origen_ui = st.selectbox(
            "País origen",
            options=lista_paises_ui,
            index=lista_paises_ui.index(st.session_state["pais_origen"])
            if st.session_state["pais_origen"] in lista_paises_ui else None,
            placeholder="— Selecciona país origen —",
        )

        if pais_origen_ui:
            st.session_state["pais_origen"] = pais_origen_ui
            st.session_state["id_pais_origen"] = map_paisui_a_id.get(pais_origen_ui)
            st.session_state["plan_guardado"] = False

        # País destino (viene desde pantalla 1 si ya fue seleccionado)
        pais_destino_ui = st.selectbox(
            "País destino",
            options=lista_paises_ui,
            index=lista_paises_ui.index(st.session_state["pais"])
            if st.session_state["pais"] in lista_paises_ui else None,
            placeholder="— Selecciona país destino —",
        )

        if pais_destino_ui:
            st.session_state["pais"] = pais_destino_ui
            st.session_state["id_pais"] = map_paisui_a_id.get(pais_destino_ui)

        id_pais_destino = st.session_state["id_pais"]
        st.session_state["plan_guardado"] = False

        # ===============================
        # Provincia destino
        # ===============================
        st.subheader("Provincia destino")

        if id_pais_destino:

            df_prov = df_dropdown_provincias[
                df_dropdown_provincias["id_pais"] == id_pais_destino
            ].copy()

            lista_prov = sorted(df_prov["provincia_nombre"].dropna().unique().tolist())

            provincia_ui = st.selectbox(
                "Selecciona provincia",
                options=lista_prov,
                index=lista_prov.index(st.session_state["provincia_destino"])
                if st.session_state["provincia_destino"] in lista_prov else None,
                placeholder="— Selecciona provincia —",
            )

            if provincia_ui:

                st.session_state["provincia_destino"] = provincia_ui
                st.session_state["plan_guardado"] = False

                row = df_prov[df_prov["provincia_nombre"] == provincia_ui].head(1)

                if not row.empty:
                    st.session_state["id_provincia_destino"] = row["id_provincia"].iloc[0]

        else:
            st.info("Selecciona primero un país destino.")

    # ===============================
    # Columna derecha
    # ===============================
    with colB:

        st.subheader("Detalles del viaje")

        # Usuario / correo
        email = st.text_input(
            "Usuario / correo",
            value=st.session_state["email"],
            placeholder="usuario@email.com"
        )

        if email:
            st.session_state["email"] = email

        st.session_state["plan_guardado"] = False
        # ===============================
        # Fechas
        # ===============================
        fecha_ida = st.date_input("Fecha de ida", value=st.session_state["fecha_ida"])
        fecha_regreso = st.date_input("Fecha de regreso", value=st.session_state["fecha_regreso"])

        st.session_state["fecha_ida"] = fecha_ida
        st.session_state["plan_guardado"] = False
        st.session_state["fecha_regreso"] = fecha_regreso
        st.session_state["plan_guardado"] = False

        # ===============================
        # Cálculo de métricas de tiempo
        # ===============================
        hoy = date.today()

        dias_restantes = None
        duracion_dias = None

        if fecha_ida:
            dias_restantes = (fecha_ida - hoy).days

        if fecha_ida and fecha_regreso:
            duracion_dias = (fecha_regreso - fecha_ida).days

        # ===============================
        # Mostrar métricas
        # ===============================
        col_kpi1, col_kpi2 = st.columns(2)

        with col_kpi1:
            if dias_restantes is None:
                st.metric("Días restantes", "—")
            else:
                st.metric("Días restantes", f"{max(dias_restantes, 0)}")

        with col_kpi2:
            if duracion_dias is None:
                st.metric("Duración del viaje", "—")
            else:
                st.metric("Duración del viaje", f"{duracion_dias} días")
        if duracion_dias is not None and duracion_dias <= 0:
            st.warning("La fecha de regreso debe ser posterior a la fecha de ida.")

        # Presupuesto
        presupuesto = st.number_input(
            "Presupuesto estimado (€)",
            min_value=0,
            value=int(st.session_state["presupuesto"]),
            step=50
        )

        st.session_state["presupuesto"] = presupuesto
        st.session_state["plan_guardado"] = False

        # ===============================
        # Tipo hospedaje
        # ===============================
        lista_aloj = sorted(
            df_dropdown_cat_aloj["categoria_alojamiento"].dropna().unique().tolist()
        )

        tipo_aloj = st.selectbox(
            "Tipo hospedaje",
            options=lista_aloj,
            index=lista_aloj.index(st.session_state["categoria_alojamiento"])
            if st.session_state["categoria_alojamiento"] in lista_aloj else None,
            placeholder="— Selecciona hospedaje —",
        )

        if tipo_aloj:
            st.session_state["categoria_alojamiento"] = tipo_aloj
            st.session_state["plan_guardado"] = False

        # ===============================
        # Tipo viaje (solo opción)
        # ===============================
        st.selectbox(
            "Tipo viaje",
            options=["Solo"],
            index=0
        )

        st.session_state["tipo_viaje"] = "Solo"
        st.session_state["plan_guardado"] = False

        # ===============================
        # Actividades deseadas
        # ===============================
        lista_act = sorted(
            df_dropdown_cat_act["categoria"].dropna().unique().tolist()
        )

        categoria_act = st.selectbox(
            "Actividades deseadas",
            options=lista_act,
            index=lista_act.index(st.session_state["categoria_actividad"])
            if st.session_state["categoria_actividad"] in lista_act else None,
            placeholder="— Selecciona actividad —",
        )

        if categoria_act:
            st.session_state["categoria_actividad"] = categoria_act
            st.session_state["plan_guardado"] = False

    st.divider()

    # ===============================
    # Recomendaciones básicas->Refinamiento
    # ===============================
    st.subheader("⭐ Recomendaciones por provincia")

    id_pais = st.session_state["id_pais"]
    categoria_act = st.session_state["categoria_actividad"]
    categoria_aloj = st.session_state["categoria_alojamiento"]

    col1, col2 = st.columns(2)

    # ===============================
    # Actividades->Refinamiento
    # ===============================
    with col1:

        st.markdown("### Provincias con más actividades")

        if id_pais and categoria_act:

            df_top_act = df_rec_act[
                (df_rec_act["id_pais"] == id_pais) &
                (df_rec_act["categoria"] == categoria_act)
            ].copy()

            df_top_act = df_top_act.sort_values("n_registros", ascending=False).head(10)

            st.dataframe(
                df_top_act[
                    ["provincia_nombre", "n_registros", "avg_precio_entrada", "avg_gasto_total"]
                ],
                use_container_width=True
            )

        else:
            st.info("Selecciona una categoría de actividad.")

    # ===============================
    # Alojamiento->Refiamiento
    # ===============================
    with col2:

        st.markdown("### Provincias más económicas")

        if id_pais and categoria_aloj:

            df_top_aloj = df_rec_aloj[
                (df_rec_aloj["id_pais"] == id_pais) &
                (df_rec_aloj["categoria_alojamiento"] == categoria_aloj)
            ].copy()

            df_top_aloj = df_top_aloj.sort_values("precio_medio", ascending=True).head(10)

            st.dataframe(
                df_top_aloj[
                    ["provincia_nombre", "avg_semana", "avg_fin_semana", "precio_medio"]
                ],
                use_container_width=True
            )

        else:
            st.info("Selecciona un tipo de hospedaje.")

        # ===============================
        # Boton para Guardar/Aprobar Plan
        # ===============================

        st.divider()
        st.subheader("✅ Guardar / aprobar plan")

        col_save, col_next = st.columns([1, 1])

        with col_save:
            disabled_save = st.session_state.get("plan_guardado", False) or st.session_state.get("guardando", False)

            if st.button("Guardar plan ✅", use_container_width=True, disabled=disabled_save):

                # Lock inmediato (evita doble submit)
                st.session_state["guardando"] = True

                try:
                    plan_id = guardar_plan_db()
                finally:
                    # siempre liberar lock aunque falle
                    st.session_state["guardando"] = False

                if plan_id:
                    st.session_state["ultimo_plan_id"] = int(plan_id)
                    st.session_state["plan_guardado"] = True

                    # refrescar data cacheada para Pantalla 3
                    st.cache_data.clear()

                    st.success(f"Plan guardado con éxito. ID del plan: {plan_id}")

        with col_next:
            if st.button("Ir a Resumen (Pantalla 3) ➜", use_container_width=True):
                st.cache_data.clear()
                st.session_state["step"] = 3
                st.rerun()

def pantalla_3():
    st.header("📋 Resumen del plan")

    if df_plan_resumen.empty:
        st.info("Aún no hay planes guardados en la base de datos.")
        st.caption("Primero completa la Pantalla 2 y guarda un plan.")
        return

    # Tomamos el plan más reciente
    plan = df_plan_resumen.sort_values("created_at", ascending=False).iloc[0]
    plan_id = int(plan["id_plan"])

    st.subheader(f"ID del plan: {plan_id}")

    # ===============================
    # Resumen principal
    # ===============================
    c1, c2, c3 = st.columns(3)

    with c1:
        st.metric("Días de viaje", int(plan["dias_viaje"]))
        st.caption(f"Noches: {int(plan['noches_viaje'])}")

    with c2:
        st.metric("Presupuesto", f"€{float(plan['presupuesto_estimado']):,.2f}".replace(",", "X").replace(".", ",").replace("X", "."))

    with c3:
        st.metric("Tipo de viaje", str(plan["tipo_viaje"]))

    st.divider()

    # ===============================
    # Fechas y destinos
    # ===============================
    colA, colB = st.columns(2)

    with colA:
        st.markdown("### Fechas")
        st.write(f"**Fecha ida:** {plan['fecha_ida']}")
        st.write(f"**Fecha regreso:** {plan['fecha_regreso']}")

    with colB:
        st.markdown("### Destinos")
        st.write(f"**País origen:** {plan['pais_origen']}")
        st.write(f"**País destino:** {plan['pais_destino']}")
        provincia = plan.get("provincia_destino", None)
        if pd.isna(provincia) or provincia is None:
            st.write("**Provincia destino:** (no definida)")
        else:
            st.write(f"**Provincia destino:** {provincia}")

    st.divider()

    # ===============================
    # Categorías seleccionadas
    # ===============================
    st.markdown("### Categorías elegidas")
    st.write(f"**Hospedaje:** {plan['categoria_alojamiento']}")
    categorias = plan.get("categorias_actividad", "")
    if categorias:
        st.write(f"**Actividades:** {categorias}")
    else:
        st.write("**Actividades:** (no definidas)")

    st.divider()

    # ===============================
    # Costos estimados (desde view)
    # ===============================
    st.markdown("### Presupuesto estimado simple")

    row_costos = df_plan_costos[df_plan_costos["id_plan"] == plan_id]

    if row_costos.empty:
        st.info("No hay estimación de costos disponible para este plan.")
    else:
        costos = row_costos.iloc[0]

        alojamiento = float(costos["alojamiento_estimado"])
        actividades = float(costos["actividades_estimado"])
        total_estimado = alojamiento + actividades

        k1, k2, k3, k4 = st.columns(4)

        with k1:
            st.metric("Alojamiento", f"€{alojamiento:,.2f}".replace(",", "X").replace(".", ",").replace("X", "."))
        with k2:
            st.metric("Actividades", f"€{actividades:,.2f}".replace(",", "X").replace(".", ",").replace("X", "."))
        with k3:
            st.metric("Transporte", "No incluido")
        with k4:
            st.metric("Total estimado", f"€{total_estimado:,.2f}".replace(",", "X").replace(".", ",").replace("X", "."))

        # Comparación con presupuesto del usuario
        presupuesto = float(plan["presupuesto_estimado"])
        if presupuesto > 0:
            diff = presupuesto - total_estimado
            if diff >= 0:
                st.success(f"✅ Estás dentro del presupuesto. Margen aproximado: €{diff:,.2f}".replace(",", "X").replace(".", ",").replace("X", "."))
            else:
                st.warning(f"⚠️ Estás por encima del presupuesto. Diferencia: €{abs(diff):,.2f}".replace(",", "X").replace(".", ",").replace("X", "."))

    st.divider()

    # ===============================
    # Calendario simple
    # ===============================
    st.markdown("### Calendario simple del viaje")

    fecha_ida = pd.to_datetime(plan["fecha_ida"])
    dias = int(plan["dias_viaje"])

    # Tomamos la primera categoría si vienen varias
    categoria_principal = ""
    if categorias:
        categoria_principal = categorias.split(",")[0].strip()

    calendario = []
    for i in range(dias):
        fecha = (fecha_ida + pd.Timedelta(days=i)).date()

        if i == 0:
            actividad = f"Llegada + actividad {categoria_principal}" if categoria_principal else "Llegada + actividad cultural"
        elif i == dias - 1:
            actividad = "Cierre del viaje / paseo libre"
        else:
            actividad = f"Actividad tipo {categoria_principal}" if categoria_principal else "Actividad tipo cultural"

        calendario.append({
            "Día": i + 1,
            "Fecha": fecha,
            "Actividad sugerida": actividad
        })

    st.dataframe(pd.DataFrame(calendario), use_container_width=True)

def pantalla_4():
    st.subheader("Checklist / Ropa recomendada")
    st.checkbox("Pasaporte / Documentos de viaje")
    st.checkbox("Cargador / Powerbank")
    st.checkbox("Protector solar")

def pantalla_5():
    st.subheader("Presupuesto y ahorro")
    st.metric("Costo estimado del viaje", "€ 0 (placeholder)")
    st.slider("Ajustar presupuesto", 0, 5000, 500)

def pantalla_6():
    st.subheader("Actividades (filtros)")
    st.selectbox("Tipo de actividad", ["Todos", "Museo", "Tour cultural", "Naturaleza", "Gastronomía"])
    st.slider("Precio máximo", 0, 200, 50)

def pantalla_7():
    st.subheader("Resumen final")
    st.success("Checklist listo ✅ (placeholder)")
    st.success("Itinerario creado ✅ (placeholder)")
    st.button("Descargar PDF (placeholder)")
    st.button("Compartir (placeholder)")

# ===============================
# Router
# ===============================
if st.session_state.step == 1:
    pantalla_1()
elif st.session_state.step == 2:
    pantalla_2()
elif st.session_state.step == 3:
    pantalla_3()
elif st.session_state.step == 4:
    pantalla_4()
elif st.session_state.step == 5:
    pantalla_5()
elif st.session_state.step == 6:
    pantalla_6()
elif st.session_state.step == 7:
    pantalla_7()

