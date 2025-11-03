import os
import polars as pl
import streamlit as st
import plotly.express as px

# ------------------------------
# Cargar datos
# ------------------------------
@st.cache_data
def load_data(base_path):
    silver = pl.read_parquet(os.path.join(base_path, "data/silver/SpotifyFeatures_silver.parquet")).to_pandas()
    genre_gold = pl.read_parquet(os.path.join(base_path, "data/gold/genre_popularity.parquet")).to_pandas()
    artist_gold = pl.read_parquet(os.path.join(base_path, "data/gold/artist_features.parquet")).to_pandas()
    return silver, genre_gold, artist_gold


# ------------------------------
# Dashboard
# ------------------------------
def launch_dashboard(base_path):
    st.set_page_config(page_title="Spotify Medallón Dashboard", layout="wide")
    st.title("🎧 Dashboard Spotify - Silver + Gold")
    st.markdown("Análisis combinado entre niveles Silver (tracks) y Gold (agregaciones por género y artista).")

    silver, genre_gold, artist_gold = load_data(base_path)

    # ============================
    # 1️⃣ Distribución por género
    # ============================
    st.subheader("Popularidad promedio por género (Gold)")
    fig1 = px.bar(
        genre_gold.sort_values("avg_popularity", ascending=False),
        x="genre",
        y="avg_popularity",
        color="avg_popularity",
        color_continuous_scale="Viridis",
        title="Popularidad promedio por género"
    )
    st.plotly_chart(fig1, use_container_width=True)
    top_genre = genre_gold.loc[genre_gold['avg_popularity'].idxmax(), 'genre']
    st.caption(f"🎯 **Conclusión:** El género más popular en promedio es **{top_genre}**.")

    # ============================
    # 2️⃣ Características de artistas
    # ============================
    st.subheader("Energía vs Valencia por Artista (Gold)")
    fig2 = px.scatter(
        artist_gold,
        x="avg_energy",
        y="avg_valence",
        hover_name="artist_name",
        size="avg_danceability",
        color="avg_energy",
        color_continuous_scale="Plasma",
        title="Energía vs Valencia (felicidad) por artista"
    )
    st.plotly_chart(fig2, use_container_width=True)
    st.caption("🎵 **Conclusión:** Los artistas ubicados en la esquina superior derecha tienden a producir música más alegre y energética.")

    # ============================
    # 3️⃣ Canciones más movidas
    # ============================
    st.subheader("Danceability vs Energy (Silver)")
    fig3 = px.scatter(
        silver.sample(frac=0.2, random_state=42),  # muestreo para rendimiento
        x="danceability",
        y="energy",
        color="genre",
        hover_name="track_name",
        title="Relación Danceability vs Energy"
    )
    st.plotly_chart(fig3, use_container_width=True)
    st.caption("💃 **Conclusión:** Las canciones con alta energía y danceability son las más adecuadas para playlists activas o de fiesta.")

    # ============================
    # 4️⃣ Canciones felices vs tristes
    # ============================
    st.subheader("Distribución de canciones por valence y mode (Silver)")
    fig4 = px.scatter(
        silver.sample(frac=0.15, random_state=42),
        x="valence",
        y="mode",
        color="genre",
        hover_name="track_name",
        title="Felices (alta valence) vs Tristes (baja valence)"
    )
    st.plotly_chart(fig4, use_container_width=True)
    st.caption("😊 **Conclusión:** Las canciones con valence alto y mode = 1 suelen ser más alegres, mientras que las de valence bajo y mode = 0 tienden a ser melancólicas.")

    # ============================
    # 5️⃣ Comparación por género
    # ============================
    st.subheader("Promedio de Tempo y Loudness por Género (Silver)")
    agg = silver.groupby("genre")[["tempo", "loudness"]].mean().reset_index()
    fig5 = px.bar(
        agg.melt(id_vars="genre", var_name="Métrica", value_name="Valor"),
        x="genre",
        y="Valor",
        color="Métrica",
        barmode="group",
        title="Comparación de Tempo y Loudness por género"
    )
    st.plotly_chart(fig5, use_container_width=True)
    st.caption("📊 **Conclusión:** Los géneros con mayor tempo y loudness tienden a ser más dinámicos y potentes sonoramente.")

    # ============================
    # 6️⃣ Canciones instrumentales
    # ============================
    st.subheader("Canciones instrumentales (instrumentalness > 0.8)")
    inst = silver[silver["instrumentalness"] > 0.8]
    fig6 = px.histogram(
        inst,
        x="genre",
        title="Distribución de canciones instrumentales por género",
        color_discrete_sequence=["teal"]
    )
    st.plotly_chart(fig6, use_container_width=True)
    st.caption(f"🎧 **Conclusión:** Se detectaron {len(inst)} canciones instrumentales, ideales para concentración o estudio.")

# ------------------------------
# Ejecutar Streamlit
# ------------------------------
if __name__ == "__main__":
    base_path = r"C:\Users\germa\Desktop\Carpetas\Data_Engineer_Specialist\Spotify_Medallon"
    launch_dashboard(base_path)