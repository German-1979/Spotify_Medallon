import os
import polars as pl

def aggregate_gold(silver_file: str, gold_path: str):
    """Genera las tablas Gold a partir de Silver."""
    os.makedirs(gold_path, exist_ok=True)

    genre_file = os.path.join(gold_path, "genre_popularity.parquet")
    artist_file = os.path.join(gold_path, "artist_features.parquet")

    # Si ya existen los archivos, no recalcular
    if os.path.exists(genre_file) and os.path.exists(artist_file):
        print("✅ Gold ya generado, archivos existentes encontrados.")
        return

    df = pl.read_parquet(silver_file)

    # Tabla 1: Popularidad promedio por género
    genre_popularity = (
        df.group_by("genre")
          .agg([pl.col("popularity").mean().alias("avg_popularity"),
                pl.count("track_id").alias("track_count")])
          .sort("avg_popularity", descending=True)
    )
    genre_popularity.write_parquet(genre_file)

    # Tabla 2: Promedio de características musicales por artista
    features_cols = ["acousticness", "danceability", "energy", "instrumentalness",
                     "liveness", "loudness", "speechiness", "valence"]
    artist_features = (
        df.group_by("artist_name")
          .agg([pl.col(col).mean().alias(f"avg_{col}") for col in features_cols])
    )
    artist_features.write_parquet(artist_file)

    print(f"✅ Gold generado y guardado en {gold_path}")

if __name__ == "__main__":
    silver_file = r"C:\Users\germa\Desktop\Carpetas\Data_Engineer_Specialist\Spotify_Medallon\data\silver\SpotifyFeatures_silver.parquet"
    gold_path = r"C:\Users\germa\Desktop\Carpetas\Data_Engineer_Specialist\Spotify_Medallon\data\gold"

    aggregate_gold(silver_file, gold_path)