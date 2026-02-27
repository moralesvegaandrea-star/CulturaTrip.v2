import streamlit as st
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import date
import altair as alt


# Paso 1 configuración de la pagina
st.set_page_config(page_title="CulturaTrip", layout="wide")
st.markdown("<h1 style='text-align:center;font-size:100px; color:#2E8B57;'> 🗺️ CulturaTrip</h1>",
                unsafe_allow_html=True
                )
st.markdown(
        "<p style='text-align:center; color:#6b7280; font-size:35px;'>"
        "Planificación inteligente de turismo cultural"
        "</p>",
        unsafe_allow_html=True
    )

st.markdown(
    """
    <style>
    /* Label del selectbox */
    div[data-baseweb="select"] > div {
        font-size: 30px !important;
        min-height: 55px;
    }

    /* Texto seleccionado */
    div[data-baseweb="select"] span {
        font-size: 25px !important;
    }

    /* Label (País Destino) */
    label {
        font-size: 30px !important;
        font-weight: 600;
    }
    </style>
    """,
    unsafe_allow_html=True
)

BASE_DIR = Path(__file__).resolve().parents[1]

# ===============================
# Rutas a datasets finales
# ===============================

DIVISION_POLITICA_PATH = BASE_DIR / "outputs" / "division_politica_y_geoespacial.csv"
ACTIVIDADES_PATH = BASE_DIR / "outputs" / "df_actividades_geo.csv"
ALOJAMIENTOS_PATH = BASE_DIR / "outputs" / "df_alojamientos.csv"


@st.cache_data(show_spinner=False)
def load_csv(path: Path) -> pd.DataFrame:
    return pd.read_csv(path)

# Cargar datasets
df_ciudades_espana = load_csv(DIVISION_POLITICA_PATH)
df_actividades = load_csv(ACTIVIDADES_PATH)
df_lat = load_csv(DIVISION_POLITICA_PATH)


def init_state():
    defaults = {
        "step": 1,
        "prev_step": 1,
        "pais": None,      # país REAL seleccionado (ej: "España")
        "pais_ui": "— Selecciona un país —",  # valor del selectbox
    }
    for k, v in defaults.items():
        if k not in st.session_state:
            st.session_state[k] = v

init_state()

#Paso Estado session_state: memoria de la app
#Objetivo volver a ejecturar el script completo cada vez ue el usuario toca algo
#Se utiliza session_state para recordar cosas entre interacciones

if "step" not in st.session_state:
    st.session_state["step"] = 1 #paso inicial
TOTAL_STEPS = 7 # Cantidad total de pantallas/pasos

#Paso 3 Funciones de navegacion (anterior y siguiente)

def next_step():
    if st.session_state.step < TOTAL_STEPS:
        st.session_state.prev_step = st.session_state.step
        st.session_state.step += 1
def prev_step():
    if st.session_state.step > 1:
        st.session_state.prev_step = st.session_state.step
        st.session_state.step -= 1

#Paso 4 Crear barra superio con navegación y progreso
# crear columnas
col1, col2, col3 = st.columns([1,6,1])

#Definir acciones de cada columna
with col1:
    st.button("⏪ Anterior", on_click=prev_step,disabled=(st.session_state.step == 1))
with col2:
    st.progress(st.session_state.step/TOTAL_STEPS)
    st.markdown(f"<div style='text-align:center;'>Paso {st.session_state.step} de {TOTAL_STEPS}</div>",
                unsafe_allow_html=True)
with col3:
    st.button("Siguiente ⏩",on_click=next_step, disabled=(st.session_state.step == TOTAL_STEPS))

st.divider()

# Estado inicial
if st.session_state.step != st.session_state.prev_step:
    st.balloons()
    st.session_state.prev_step = st.session_state.step

#Pantallas
def pantalla_1():
    st.header("🌷Bienvenidos")
    st.subheader("Descubre más allá de los destinos tradicionales")
    st.markdown(
        "<p style='text-align:left; color:#6b7280; font-size:20px;'>"
        """
        Diseña tu viaje ideal combiando cultura, presupuesto y experiencias locales.
        Este prototipo te guiará paso a paso para crear un itinerario personalizado.
        """
        "</p>",
        unsafe_allow_html=True
    )
    st.subheader("Conoce Datos Curiosos del País a Visitar")
    st.markdown(
        "<p style='text-align:left; color:#6b7280; font-size:20px;'>"
        """
        Selecciona el país destino para personalizar tu experiencia y ver datos curiosos del país
        """
        "</p>",
        unsafe_allow_html=True
    )
    # ===============================
    # 🔄 Reset (modo test)
    # ===============================
    col_reset_left, col_reset_right = st.columns([6, 2])

    with col_reset_right:
        if st.button("🔄 Reset país (test)", use_container_width=True):
            st.session_state["pais"] = None
            st.session_state["pais_ui"] = None
            st.rerun()

    # ===============================
    # 🌍 Dropdown País Destino
    # ===============================
    lista_paises = sorted(df_lat["pais"].dropna().unique())

    # País real guardado (puede ser None)
    pais_guardado = st.session_state.get("pais")

    # Index por defecto: si hay país guardado, lo selecciona
    index_default = (
        lista_paises.index(pais_guardado)
        if pais_guardado in lista_paises
        else None
    )

    st.markdown(
        "<h2 style='margin-bottom:0.3rem;'>🌍 País de destino</h2>",
        unsafe_allow_html=True
    )

    pais_ui = st.selectbox(
        label="",
        options=lista_paises,
        index=index_default,  # ✅ persistencia real
        placeholder="— Selecciona un país —",
        key="pais_ui"
    )

    # ===============================
    # 🧠 Lógica de selección (CLAVE)
    # ===============================

    # Caso 1: el usuario NO seleccionó nada
    if pais_ui is None:
        # Si tampoco hay país guardado → no mostramos nada
        if pais_guardado is None:
            st.info("Elige un país para ver estadísticas y datos curiosos")
            return
        # Si había país guardado → seguimos usando ese
        pais = pais_guardado

    # Caso 2: el usuario seleccionó un país nuevo
    else:
        st.session_state["pais"] = pais_ui
        pais = pais_ui

    # Filtrar por país
    df_p = df_lat[df_lat["pais"] == pais].copy()

    #Metricas
    # Ajusta nombres de columnas si difieren: "comunidad_autonoma", "provincia", "ciudad", "id_municipio"
    n_municipios = df_p["id_municipio"].nunique() if "id_municipio" in df_p.columns else df_p["nombre"].nunique()
    n_provincias = df_p["provincia"].nunique() if "provincia" in df_p.columns else None
    n_ccaa = df_p["comunidad autonoma"].nunique() if "comunidad autonoma" in df_p.columns else None
    n_islas = df_p["isla"].nunique() if "isla" in df_p.columns else 0

    # "Sabias que...
    st.markdown(
        f"""
            <div style="padding:25px; font-size:20px; border-radius:12px; background:#F5F7F9;">
            <b>¿Sabías que…?</b> <b>{pais}</b> contiene <b>{n_municipios:,}</b> municipios únicos,
            organizados en <b>{(n_provincias or 0):,}</b> provincias y <b>{(n_ccaa or 0):,}</b> comunidades autónomas.
            Además, se identifican <b>{n_islas:,}</b> islas distintas.
            </div>
            """.replace(",", "."),
        unsafe_allow_html=True
    )

    st.divider()
#Crear lista de datos culturales
    DATOS_CULTURALES = {
        "España":[
         {"titulo": "🏺 Antigüedad","texto": "España fue uno de los primeros territorios europeos explotados por metales (oro, plata y cobre) por fenicios y romanos."},
         {"titulo": "🏰 Granada", "texto": "Fue el último reino musulmán de la Península Ibérica hasta 1492, cuando los Reyes Católicos culminaron la Reconquista con la toma de la Alhambra."},
         {"titulo": "🕌 Córdoba", "texto": "En el siglo X, Córdoba fue una de las ciudades más grandes y cultas del mundo occidental, con bibliotecas y alumbrado público."},
         {"titulo": "🏛️ Mérida", "texto": "Fue una de las capitales romanas más importantes fuera de Italia."},
        {"titulo": "🗡️ Toledo","texto": "Convivieron durante siglos cristianos, judíos y musulmanes, lo que la convirtió en un gran centro cultural medieval."},
        {"titulo": "⛪ Santiago de Compostela", "texto": "El Camino de Santiago es una de las rutas de peregrinación más antiguas de Europa."},
        {"titulo": "🌍 Sevilla", "texto": "Desde Sevilla se gestionaba el comercio con América durante el Imperio español."},
        {"titulo": "🗣️ Idiomas de España","texto": "España es uno de los pocos países de Europa donde varias lenguas romances conviven oficialmente: además del castellano, existen lenguas cooficiales como el catalán, el gallego y el euskera, este último no tiene parentesco con ninguna otra lengua europea conocida."},
        {"titulo": "🍷 Gastronomía de España","texto": "La costumbre de las tapas nació como una forma práctica de cubrir las bebidas con comida, y hoy se ha convertido en una de las tradiciones gastronómicas más representativas de España, donde compartir platos es parte esencial de la experiencia social."}
        ],
        "Italia": [
            {"titulo": "🏛️ Roma","texto": "Fue el centro del Imperio Romano durante más de cinco siglos y uno de los núcleos culturales más influyentes de Europa."}
        ],
        "Francia":[
            {"titulo": "🗼 París","texto": "Es la ciudad con mayor concentración de museos del mundo y un referente histórico del arte y la cultura europea."}
        ]
    }
    #Dividir esta vista en 2 columnas
    st.divider()
    st.subheader("Destacados culturales del país")
    col_izq, col_der = st.columns([2, 1], gap="medium")
    with col_izq:
        if pais in DATOS_CULTURALES:
            for d in DATOS_CULTURALES[pais]:
                st.markdown(
                    f"""
                    <div style="margin-bottom:14px;">
                        <div style="font-size:12px; font-weight:600;">
                            {d["titulo"]}
                        </div>
                        <div style="font-size:10px; line-height:1.6; color:#374151;">
                            {d["texto"]}
                        </div>
                    </div>
                    """,
                    unsafe_allow_html=True
                )

        else:
                st.info("No hay datos culturales para este páis todavía")
    with col_der:
        # Imagen por país desde carpeta llamada assets
        IMAGEN_PAIS = {
            "España": BASE_DIR/"assets/spain_map.jfif",
            "Italia": BASE_DIR/"assets/italia.jpg",
            "Francia": BASE_DIR/"assets/francia.jpg",
        }
        img_path = IMAGEN_PAIS.get(pais)
        if img_path and img_path.exists():
            st.image(str(img_path), use_container_width=True)
        else:
            st.info("Aún no hay imagen para este país.")

    # Ciudades con el mismo nombre (misma o distinta provincia)
    col_ciudad = "nombre"
    col_provincia = "provincia"

    if col_ciudad in df_p.columns and col_provincia in df_p.columns:
        #Normalizar texto por prevencion
        df_tmp = df_p.copy()
        df_tmp[col_ciudad] = df_tmp[col_ciudad].astype(str).str.strip().str.lower()
        df_tmp[col_provincia] = df_tmp[col_provincia].astype(str).str.strip()

        #a) Ciudades que aparecen más de una vez
        ciudad_counts = df_tmp[col_ciudad].value_counts()
        ciudades_repetidas = ciudad_counts[ciudad_counts > 1].index.tolist()

        if len(ciudades_repetidas) == 0:
            st.success("Este país no tiene ciudades con el mismo nombre")
        else:
            df_rep = df_tmp[df_tmp[col_ciudad].isin(ciudades_repetidas)].copy()

            #Cuando la ciudad es repetida, cuantas provincias distintas tiene

#Crear columnas para mostrar mapa del Pais y Cantidad de Municipios
    st.subheader("Explora el país: provincias y mapa")
    col_graf, col_mapa = st.columns([1.2, 1], gap="large")

    with col_graf:
        st.markdown("### Pronvincias con más municipios")
        if "provincia" in df_p.columns:
            top_n = 5 #top provincias esta variable se puede ajustar
            df_count = (
                df_p.dropna(subset = ["provincia"])
                .groupby("provincia")
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
                        "provincia:N",
                        sort="-y",
                        title="Provincia",
                        axis=alt.Axis(
                            labelAngle=-15,
                            labelFontSize=10,
                            labelColor="#111827",
                            labelLimit=200,
                            titleFontSize=16,
                            titleColor="#111827",
                        ),
                    ),
                    y=alt.Y(
                        "n_municipios:Q",
                        title="Número de municipios",
                        axis=alt.Axis(
                            labelFontSize=14,
                            labelColor="#111827",
                            titleFontSize=16,
                            titleColor="#111827",
                        ),
                    ),
                    tooltip=["provincia", "n_municipios"],
                )
                .properties(height=600)
            )

            st.altair_chart(chart, use_container_width=True)
            st.caption(f"Top {top_n} provincias por cantidad de municipios.")

        else:
            st.info("No encuentro la columna 'provincia' en el dataset para construir el gráfico.")
    with col_mapa:
        st.markdown("### Mapa general")
        #Intenta detectar columnas de lat/long
        posibles_lat = [c for c in df_p.columns if c.lower() in ["lat", "latitude", "latitud"]]
        posibles_lon = [c for c in df_p.columns if c.lower() in ["lon", "lng", "longitude", "longitud"]]
        if posibles_lat and posibles_lon:
            lat_col = posibles_lat[0]
            lon_col = posibles_lon[0]
            df_map = df_p[[lat_col, lon_col]].dropna().rename(columns={lat_col:"lat",lon_col:"lon"})
            df_map = df_map.head(8000)
            st.map(df_map)
            st.caption("Puntos geográficos para visualizar la distribución territorial.")
        else:
             st.info("No encuentro lat/lon en este dataset para mostrar el mapa.")
# sirve como limite para que no haga lento


# luego pantalla_2(), etc.
def pantalla_2():
    st.header("Pantalla 2")
    st.write("Aquí el usuario seleccionará destinos, fechas y tipo de viaje.")
    st.date_input("Fecha de ida")
    st.date_input("Fecha de regreso")
    pais_origen = st.text_input("Pais origen",
                                placeholder="Ej. Costa Rica, México, España"
                                )
    if pais_origen:
        st.session_state.pais_origen = pais_origen
    st.selectbox("Tipo de viaje", ["Cultural", "Aventura", "Gastronómico", "Relax", "Mixto"])
    st.selectbox("Actividades Sugeridas", ["Actividades sugeridad segun tipo de viaje"])
def pantalla_3():
    st.subheader(" Itinerario sugerido")
    st.write("Aquí irá el resumen por día y la edición de actividades (placeholder).")
    st.info("Itinerario sugerido (pendiente de integrar lógica).")

def pantalla_4():
    st.subheader("Checklist / Ropa recomendada")
    st.write("Aquí irá tu checklist estilo Excel/Canva.")
    st.checkbox("Pasaporte / Documentos de viaje")
    st.checkbox("Cargador / Powerbank")
    st.checkbox("Protector solar")

def pantalla_5():
    st.subheader(" Presupuesto y ahorro")
    st.write("Aquí irá el desglose: transporte, alojamiento, actividades, comidas, otros.")
    st.metric("Costo estimado del viaje", "€ 0 (placeholder)")
    st.slider("Ajustar presupuesto", 0, 5000, 500)

def pantalla_6():
    st.subheader(" Actividades (filtros)")
    st.write("Aquí irá: tipo, precio, ecoscore y selección.")
    st.selectbox("Tipo de actividad", ["Todos", "Museo", "Tour cultural", "Naturaleza", "Gastronomía"])
    st.slider("Precio máximo", 0, 200, 50)

def pantalla_7():
    st.subheader(" Resumen final")
    st.success("Checklist listo ✅ (placeholder)")
    st.success("Itinerario creado ✅ (placeholder)")
    st.button("Descargar PDF (placeholder)")
    st.button("Compartir (placeholder)")

# -----------------------------
# 6) Router: mostrar la pantalla según el paso actual
# -----------------------------
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