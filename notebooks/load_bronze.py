import os
from datetime import datetime
import polars as pl

# ------------------------------
# 2️⃣ Load Bronze: leer CSV tal cual y agregar timestamp
# ------------------------------
def load_bronze(csv_path, bronze_path):
    """
    Lee un CSV desde raw, agrega timestamp de ingesta y guarda en bronze
    sin hacer ninguna transformación.

    Args:
        csv_path (str): Ruta del CSV original (raw).
        bronze_path (str): Carpeta donde se guardará el CSV/Parquet bronze.

    Returns:
        pl.DataFrame: DataFrame cargado con columna de timestamp.
    """
    os.makedirs(bronze_path, exist_ok=True)

    # Leer CSV usando Polars
    df = pl.read_csv(csv_path, use_pyarrow=True)

    # Agregar timestamp de ingesta como tipo nativo de Polars
    df = df.with_columns([
        pl.lit(datetime.now()).cast(pl.Datetime("us")).alias("ingest_timestamp")
    ])

    # Guardar en bronze como Parquet
    file_name = os.path.basename(csv_path).replace(".csv", "_bronze.parquet")
    bronze_file = os.path.join(bronze_path, file_name)
    df.write_parquet(bronze_file)  # ✅ no usar 'engine'

    print(f"[load_bronze] Guardado en bronze: {bronze_file} con {len(df)} filas")
    return df

# ------------------------------
# Ejecutar
# ------------------------------
if __name__ == "__main__":
    # Ruta del CSV original (raw)
    csv_path = r"C:\Users\germa\Desktop\Carpetas\Data_Engineer_Specialist\Spotify_Medallon\data\raw\SpotifyFeatures.csv"

    # Carpeta bronze
    bronze_path = r"C:\Users\germa\Desktop\Carpetas\Data_Engineer_Specialist\Spotify_Medallon\data\bronze"

    # Ejecutar la función
    df_bronze = load_bronze(csv_path, bronze_path)

    # Mostrar primeras filas
    print("\nPrimeras 5 filas de Bronze:")
    print(df_bronze.head())