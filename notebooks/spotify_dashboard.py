import os
import polars as pl
import streamlit as st
import plotly.express as px

# ------------------------------
# Cargar Gold con cache
# ------------------------------
@st.cache_data
def load_gold(gold_path):
    genre_df = pl.read_parquet(os.path.join(gold_path, "genre_popularity.parquet")).to_pandas()
    artist_df = pl.read_parquet(os.path.join(gold_path, "artist_features.parquet")).to_pandas()
    return genre_df, artist_df

# ------------------------------
# Dashboard
# ------------------------------
def launch_dashboard(gold_path):
    st.set_page_config(page_title="Dashboard Gold Spotify", layout="wide")
    st.title("Dashboard Gold Spotify")

    genre_df, artist_df = load_gold(gold_path)

    # Filtros
    selected_genres = st.multiselect(
        "Selecciona Género:",
        options=genre_df['genre'].unique(),
        default=genre_df['genre'].unique()
    )
    selected_metric = st.selectbox(
        "Selecciona Métrica:",
        options=[c for c in genre_df.columns if c.startswith('avg_')],
        index=0
    )

    # Filtrado
    df_genre = genre_df[genre_df['genre'].isin(selected_genres)] if selected_genres else genre_df
    df_artist = artist_df.copy()  # artistas no tienen 'genre'

    # Gráficos
    st.subheader("Popularidad promedio por género")
    fig1 = px.bar(df_genre, x='genre', y=selected_metric, color=selected_metric,
                  color_continuous_scale='Viridis', title=f'{selected_metric} por Género')
    st.plotly_chart(fig1, use_container_width=True)

    st.subheader("Distribución de Danceability")
    fig2 = px.histogram(df_artist, x='avg_danceability', nbins=30, color_discrete_sequence=['skyblue'])
    st.plotly_chart(fig2, use_container_width=True)

    st.subheader("Energía vs Valencia de Artistas")
    fig3 = px.scatter(df_artist, x='avg_energy', y='avg_valence',
                      size=selected_metric if selected_metric in df_artist.columns else None,
                      hover_name='artist_name',
                      color=selected_metric if selected_metric in df_artist.columns else None,
                      color_continuous_scale='Viridis',
                      title=f'Artistas: Energy vs Valence con {selected_metric}')
    st.plotly_chart(fig3, use_container_width=True)

# ------------------------------
# Ejecutar Streamlit
# ------------------------------
if __name__ == "__main__":
    gold_path = r"C:\Users\germa\Desktop\Carpetas\Data_Engineer_Specialist\Spotify_Medallon\data\gold"
    launch_dashboard(gold_path)