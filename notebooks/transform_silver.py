import polars as pl
from datetime import datetime
import os

def transform_silver(bronze_file: str, silver_path: str):
    # 1️⃣ Leer dataset desde capa Bronze
    df = pl.read_parquet(bronze_file)

    # 2️⃣ Eliminar duplicados
    df = df.unique()

    # 3️⃣ Reemplazar nulos: strings -> "N/A", numéricas -> 0
    for col in df.columns:
        dtype = df[col].dtype
        if dtype == pl.Utf8:
            df = df.with_columns(pl.col(col).fill_null("N/A"))
        elif dtype in [pl.Int64, pl.Float64]:
            df = df.with_columns(pl.col(col).fill_null(0))

    # 4️⃣ Detectar dinámicamente columnas numéricas y convertir a Float64
    numeric_cols = [col for col, dtype in zip(df.columns, df.dtypes) if dtype in [pl.Int64, pl.Float64]]
    for col in numeric_cols:
        df = df.with_columns(pl.col(col).cast(pl.Float64))

    # 5️⃣ Normalizar strings (capitalizar cada palabra)
    string_cols_to_title = [col for col, dtype in zip(df.columns, df.dtypes) if dtype == pl.Utf8]
    for col in string_cols_to_title:
        df = df.with_columns(pl.col(col).str.to_titlecase())

    # 6️⃣ Agregar timestamp de procesamiento
    df = df.with_columns(
        pl.lit(datetime.now()).cast(pl.Datetime("us")).alias("processed_timestamp")
    )

    # 7️⃣ Crear carpeta Silver si no existe
    os.makedirs(silver_path, exist_ok=True)

    # 8️⃣ Guardar en Silver
    silver_file = os.path.join(silver_path, "SpotifyFeatures_silver.parquet")
    df.write_parquet(silver_file)

    print(f"✅ Transformación a Silver completada: {silver_file}")
    return df


if __name__ == "__main__":
    bronze_file = r"C:\Users\germa\Desktop\Carpetas\Data_Engineer_Specialist\Spotify_Medallon\data\bronze\SpotifyFeatures_bronze.parquet"
    silver_path = r"C:\Users\germa\Desktop\Carpetas\Data_Engineer_Specialist\Spotify_Medallon\data\silver"

    df_silver = transform_silver(bronze_file, silver_path)
    print(df_silver.head())
