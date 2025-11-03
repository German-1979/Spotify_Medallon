import os
import polars as pl

def show_gold_tables(gold_path: str | None = None):
    """
    Lee y muestra en consola las tablas Gold: genre_popularity y artist_features.

    Args:
        gold_path (str | None): Carpeta donde se encuentran los archivos Gold. 
                                Si es None, se determina automáticamente.
    """
    # Determinar base_path
    if gold_path is None:
        try:
            base_path = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
        except NameError:
            base_path = os.path.abspath(os.getcwd())
        gold_path = os.path.join(base_path, "data", "gold")

    genre_file = os.path.join(gold_path, "genre_popularity.parquet")
    artist_file = os.path.join(gold_path, "artist_features.parquet")

    # Verificar que existan
    if not os.path.exists(genre_file) or not os.path.exists(artist_file):
        print("❌ Archivos Gold no encontrados en", gold_path)
        return

    # Leer y mostrar las tablas
    print("🎵 Popularidad por género:")
    df_genre = pl.read_parquet(genre_file)
    print(df_genre)

    print("\n🎤 Promedio de características por artista:")
    df_artist = pl.read_parquet(artist_file)
    print(df_artist)


# Ejecutar si se corre como script
if __name__ == "__main__":
    show_gold_tables()