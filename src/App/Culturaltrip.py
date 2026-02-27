import streamlit as st
import pandas as pd
from pathlib import Path
import altair as alt

# =========================
# Configuración de la página
# =========================
st.set_page_config(page_title="CulturaTrip", layout="wide")

st.markdown(
    "<h1 style='text-align:center;font-size:100px; color:#2E8B57;'> 🗺️ CulturaTrip</h1>",
    unsafe_allow_html=True
)
st.markdown(
    "<p style='text-align:center; color:#6b7280; font-size:35px;'>"
    "Planificación inteligente de turismo cultural"
    "</p>",
    unsafe_allow_html=True
)

# CSS (selectbox grande)
st.markdown(
    """
    <style>
    div[data-baseweb="select"] > div { font-size: 30px !important; min-height: 55px; }
    div[data-baseweb="select"] span { font-size: 25px !important; }
    label { font-size: 30px !important; font-weight: 600; }
    </style>
    """,
    unsafe_allow_html=True
)

BASE_DIR = Path(__file__).resolve().parents[2]

# ===============================
# Rutas a datasets finales (data/clean)
# ===============================
DIM_PAIS_PATH = BASE_DIR / "data" / "clean" / "dim_pais.csv"
DIM_MUNICIPIO_PATH = BASE_DIR / "data" / "clean" / "dim_municipio_final.csv"
DIM_GEO_MUNI_OSM_PATH = BASE_DIR / "data" / "clean" / "dim_geografia_municipio_osm.csv"

FACT_ACTIVIDADES_PATH = BASE_DIR / "data" / "clean" / "fact_actividades_provincia_enriquecida.csv"
ALOJAMIENTOS_PATH = BASE_DIR / "data" / "clean" / "df_alojamientos.csv"

@st.cache_data(show_spinner=False)
def load_csv(path: Path) -> pd.DataFrame:
    return pd.read_csv(path)

# Cargar datasets
df_paises = load_csv(DIM_PAIS_PATH)
df_muni = load_csv(DIM_MUNICIPIO_PATH)
df_geo = load_csv(DIM_GEO_MUNI_OSM_PATH)

df_actividades = load_csv(FACT_ACTIVIDADES_PATH)
df_alojamientos = load_csv(ALOJAMIENTOS_PATH)

# Merge para tener lat/lon a nivel municipio
df_divgeo = df_muni.merge(df_geo[["id_municipio", "lat", "lon"]], on="id_municipio", how="left")

# ===============================
# Session state
# ===============================
def init_state():
    defaults = {
        "step": 1,
        "prev_step": 1,
        "pais": None,        # nombre país UI (ej: "España")
        "id_pais": None,     # código (ej: "ES")
        "pais_ui": None,     # valor selectbox (None si vacío)
    }
    for k, v in defaults.items():
        if k not in st.session_state:
            st.session_state[k] = v

init_state()

TOTAL_STEPS = 7

def next_step():
    if st.session_state.step < TOTAL_STEPS:
        st.session_state.prev_step = st.session_state.step
        st.session_state.step += 1

def prev_step():
    if st.session_state.step > 1:
        st.session_state.prev_step = st.session_state.step
        st.session_state.step -= 1

# Barra superior
col1, col2, col3 = st.columns([1, 6, 1])
with col1:
    st.button("⏪ Anterior", on_click=prev_step, disabled=(st.session_state.step == 1))
with col2:
    st.progress(st.session_state.step / TOTAL_STEPS)
    st.markdown(
        f"<div style='text-align:center;'>Paso {st.session_state.step} de {TOTAL_STEPS}</div>",
        unsafe_allow_html=True
    )
with col3:
    st.button("Siguiente ⏩", on_click=next_step, disabled=(st.session_state.step == TOTAL_STEPS))

st.divider()

if st.session_state.step != st.session_state.prev_step:
    st.balloons()
    st.session_state.prev_step = st.session_state.step

# ===============================
# Helpers
# ===============================
def pais_display(nombre_en_dim_pais: str) -> str:
    """dim_pais trae nombres en inglés/lower; aquí lo adaptamos a UI."""
    if not isinstance(nombre_en_dim_pais, str):
        return nombre_en_dim_pais
    base = nombre_en_dim_pais.strip().title()
    overrides = {"Spain": "España", "Italy": "Italia", "France": "Francia"}
    return overrides.get(base, base)

# Preparamos lista UI + mapa nombre->id_pais
df_paises["pais_ui"] = df_paises["pais"].apply(pais_display)
map_paisui_a_id = dict(zip(df_paises["pais_ui"], df_paises["id_pais"]))

# ===============================
# Pantalla 1
# ===============================
def pantalla_1():
    st.header("🌷 Bienvenidos")
    st.subheader("Descubre más allá de los destinos tradicionales")
    st.markdown(
        "<p style='text-align:left; color:#6b7280; font-size:20px;'>"
        "Diseña tu viaje ideal combinando cultura, presupuesto y experiencias locales. "
        "Este prototipo te guiará paso a paso para crear un itinerario personalizado."
        "</p>",
        unsafe_allow_html=True
    )

    st.subheader("Conoce Datos Curiosos del País a Visitar")
    st.markdown(
        "<p style='text-align:left; color:#6b7280; font-size:20px;'>"
        "Selecciona el país destino para personalizar tu experiencia y ver datos curiosos del país."
        "</p>",
        unsafe_allow_html=True
    )

    # --- Reset (modo test) ---
    col_reset_left, col_reset_right = st.columns([6, 2])
    with col_reset_right:
        if st.button("🔄 Reset país (test)", use_container_width=True):
            st.session_state["pais"] = None
            st.session_state["id_pais"] = None
            st.session_state["pais_ui"] = None
            st.rerun()

    # --- Dropdown País Destino (desde dim_pais) ---
    lista_paises_ui = sorted(df_paises["pais_ui"].dropna().unique().tolist())
    pais_guardado = st.session_state.get("pais")

    index_default = lista_paises_ui.index(pais_guardado) if pais_guardado in lista_paises_ui else None

    st.markdown("<h2 style='margin-bottom:0.3rem;'>🌍 País de destino</h2>", unsafe_allow_html=True)

    pais_ui = st.selectbox(
        label="",
        options=lista_paises_ui,
        index=index_default,
        placeholder="— Selecciona un país —",
        key="pais_ui"
    )

    # Persistencia (NO borres si el user no selecciona nada)
    if pais_ui is None:
        if pais_guardado is None:
            st.info("Elige un país para ver estadísticas y datos curiosos")
            return
        pais = pais_guardado
    else:
        st.session_state["pais"] = pais_ui
        st.session_state["id_pais"] = map_paisui_a_id.get(pais_ui)
        pais = pais_ui

    id_pais = st.session_state.get("id_pais")

    # ===============================
    # Datos del país (si existen en tu modelo)
    # ===============================
    df_p = df_divgeo[df_divgeo["id_pais"] == id_pais].copy() if id_pais else pd.DataFrame()

    if df_p.empty:
        # País sin datos de división política en tu modelo (por ahora)
        st.markdown(
            f"""
            <div style="padding:25px; font-size:20px; border-radius:12px; background:#F5F7F9;">
            <b>¿Sabías que…?</b> Aún no tenemos división política municipal cargada para <b>{pais}</b>.
            </div>
            """,
            unsafe_allow_html=True
        )
        # Mapa centroid (dim_pais)
        row = df_paises[df_paises["pais_ui"] == pais].head(1)
        if not row.empty:
            st.map(pd.DataFrame({"lat": [row["lat"].iloc[0]], "lon": [row["lon"].iloc[0]]}))
        return

    # Métricas (con nuevos nombres)
    n_municipios = df_p["id_municipio"].nunique()
    n_provincias = df_p["provincia_nombre"].nunique() if "provincia_nombre" in df_p.columns else 0
    n_ccaa = df_p["ccaa_nombre"].nunique() if "ccaa_nombre" in df_p.columns else 0
    n_islas = df_p["id_isla"].nunique() if "id_isla" in df_p.columns else df_p["isla"].nunique()

    # Solo mostramos el bloque “Sabías que…” (evitas duplicar info con métricas)
    st.markdown(
        f"""
        <div style="padding:25px; font-size:20px; border-radius:12px; background:#F5F7F9;">
        <b>¿Sabías que…?</b> <b>{pais}</b> contiene <b>{n_municipios:,}</b> municipios únicos,
        organizados en <b>{n_provincias:,}</b> provincias y <b>{n_ccaa:,}</b> comunidades autónomas.
        Además, se identifican <b>{n_islas:,}</b> islas distintas.
        </div>
        """.replace(",", "."),
        unsafe_allow_html=True
    )

    st.divider()

    # ===============================
    # Destacados culturales + imagen (2 columnas)
    # ===============================
    DATOS_CULTURALES = {
        "España": [
            {"titulo": "🏺 Antigüedad", "texto": "España fue uno de los primeros territorios europeos explotados por metales (oro, plata y cobre) por fenicios y romanos."},
            {"titulo": "🏰 Granada", "texto": "Fue el último reino musulmán de la Península Ibérica hasta 1492, cuando los Reyes Católicos culminaron la Reconquista con la toma de la Alhambra."},
            {"titulo": "🕌 Córdoba", "texto": "En el siglo X, Córdoba fue una de las ciudades más grandes y cultas del mundo occidental, con bibliotecas y alumbrado público."},
            {"titulo": "🏛️ Mérida", "texto": "Fue una de las capitales romanas más importantes fuera de Italia."},
            {"titulo": "🗡️ Toledo", "texto": "Convivieron durante siglos cristianos, judíos y musulmanes, lo que la convirtió en un gran centro cultural medieval."},
            {"titulo": "⛪ Santiago de Compostela", "texto": "El Camino de Santiago es una de las rutas de peregrinación más antiguas de Europa."},
            {"titulo": "🌍 Sevilla", "texto": "Desde Sevilla se gestionaba el comercio con América durante el Imperio español."},
            {"titulo": "🗣️ Idiomas", "texto": "Coexisten lenguas cooficiales como catalán, gallego y euskera (no emparentado con otras lenguas europeas)."},
            {"titulo": "🍷 Gastronomía", "texto": "Las tapas nacieron como una forma práctica de cubrir bebidas y hoy son un símbolo cultural."},
        ],
        "Italia": [{"titulo": "🏛️ Roma", "texto": "Fue el centro del Imperio Romano durante más de cinco siglos."}],
        "Francia": [{"titulo": "🗼 París", "texto": "Alta concentración de museos y referente histórico del arte europeo."}],
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
    # Provincias + mapa (2 columnas)
    # ===============================
    st.subheader("Explora el país: provincias y mapa")
    col_graf, col_mapa = st.columns([1.2, 1], gap="large")

    with col_graf:
        st.markdown("### Provincias con más municipios")
        if "provincia_nombre" in df_p.columns:
            top_n = 10
            df_count = (
                df_p.dropna(subset=["provincia_nombre"])
                .groupby("provincia_nombre")
                .size()
                .sort_values(ascending=False)
                .head(top_n)
                .reset_index(name="n_municipios")
            )

            chart = (
                alt.Chart(df_count)
                .mark_bar(color="#2E8B57")
                .encode(
                    x=alt.X(
                        "provincia_nombre:N",
                        sort="-y",
                        title="Provincia",
                        axis=alt.Axis(
                            labelAngle=-15,
                            labelFontSize=12,
                            labelColor="#111827",
                            labelLimit=240,
                            titleFontSize=16,
                            titleColor="#111827",
                        ),
                    ),
                    y=alt.Y(
                        "n_municipios:Q",
                        title="Número de municipios",
                        axis=alt.Axis(
                            labelFontSize=12,
                            labelColor="#111827",
                            titleFontSize=16,
                            titleColor="#111827",
                        ),
                    ),
                    tooltip=["provincia_nombre", "n_municipios"],
                )
                .properties(height=450)
            )

            st.altair_chart(chart, use_container_width=True)
            st.caption(f"Top {top_n} provincias por cantidad de municipios.")
        else:
            st.info("No encuentro la columna provincia_nombre para construir el gráfico.")

    with col_mapa:
        st.markdown("### Mapa general")
        if "lat" in df_p.columns and "lon" in df_p.columns:
            df_map = df_p[["lat", "lon"]].dropna().head(8000)
            st.map(df_map)
            st.caption("Puntos geográficos para visualizar la distribución territorial.")
        else:
            st.info("No encuentro lat/lon para mostrar el mapa.")

# ===============================
# Pantalla 2 (placeholder)
# ===============================
def pantalla_2():
    st.header("Pantalla 2")
    st.write("Aquí el usuario seleccionará destinos, fechas y tipo de viaje.")
    st.date_input("Fecha de ida")
    st.date_input("Fecha de regreso")
    pais_origen = st.text_input("País origen", placeholder="Ej. Costa Rica, México, España")
    if pais_origen:
        st.session_state["pais_origen"] = pais_origen
    st.selectbox("Tipo de viaje", ["Cultural", "Aventura", "Gastronómico", "Relax", "Mixto"])
    st.selectbox("Actividades sugeridas", ["(placeholder)"])

def pantalla_3():
    st.subheader("Itinerario sugerido")
    st.info("Pendiente de integrar lógica.")

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

