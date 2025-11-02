import polars as pl

bronze_file = r"C:\Users\germa\Desktop\Carpetas\Data_Engineer_Specialist\Spotify_Medallon\data\bronze\SpotifyFeatures_bronze.parquet"

# Leer el Parquet
df = pl.read_parquet(bronze_file)

# Imprimir nombres de columnas
print(df.columns)