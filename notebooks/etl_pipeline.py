#!/usr/bin/env python
# coding: utf-8

# ### 🔧 Parámetros dinámicos (papermill)

# In[1]:


# Parameters
raw_path = "../data/raw"
bronze_path = "../data/bronze"
silver_path = "../data/silver"
gold_path = "../data/gold"


# ### Importación de Librerías y Módulos

# In[2]:


import os
import shutil
import polars as pl
import kagglehub
from datetime import datetime
import plotly.express as px
import streamlit as st


# ### 📁 Validar rutas

# In[3]:


for p in [raw_path, bronze_path, silver_path, gold_path]:
    os.makedirs(p, exist_ok=True)
    
print("[setup] Carpetas validadas correctamente.")


# ### Extract único: descargar CSV a raw

# In[4]:


def extract_to_raw(base_path: str | None = None) -> list[str]:
    """
    Descarga el dataset 'Ultimate Spotify Tracks DB' desde Kaggle y
    copia los archivos CSV a la carpeta 'data/raw' dentro de base_path.

    Args:
        base_path (str | None): Ruta raíz del proyecto. Si es None, se determina automáticamente.
            - En script (.py): usa __file__.
            - En notebook: usa el directorio de trabajo actual.

    Returns:
        list[str]: Rutas completas de los CSV copiados a la carpeta 'data/raw'.
    """
    # Determinar base_path si no se pasa
    if base_path is None:
        try:
            base_path = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
        except NameError:
            # Estamos en un notebook
            base_path = os.path.abspath(os.path.join(os.getcwd(), ".."))

    raw_path = os.path.join(base_path, "data", "raw")  # <-- aquí se corrige
    os.makedirs(raw_path, exist_ok=True)
    print(f"[extract_to_raw] Carpeta raw lista en: {raw_path}")

    # Descargar dataset desde Kaggle (se guarda en cache local)
    cache_path = kagglehub.dataset_download("zaheenhamidani/ultimate-spotify-tracks-db")
    print(f"[extract_to_raw] Dataset descargado en cache: {cache_path}")

    csv_files = []

    # Copiar solo los CSV a raw, ignorando cualquier carpeta interna
    for root, dirs, files in os.walk(cache_path):
        for file_name in files:
            if file_name.endswith(".csv"):
                src = os.path.join(root, file_name)
                dst = os.path.join(raw_path, file_name)
                shutil.copy(src, dst)
                csv_files.append(dst)
                print(f"[extract_to_raw] Copiado a raw: {file_name}")

    print(f"[extract_to_raw] CSV disponibles en raw: {[os.path.basename(f) for f in csv_files]}")
    return csv_files


# ------------------------------
# 🚀 Ejecución directa (script o notebook)
# ------------------------------
if __name__ == "__main__":
    csv_files = extract_to_raw()  # base_path se determina automáticamente
    print("\nArchivos CSV descargados en raw:")
    for f in csv_files:
        print(" -", f)


# ### Load Bronze: leer CSV tal cual y agregar timestamp

# In[5]:


def load_bronze(csv_path: str | None = None, bronze_path: str | None = None) -> pl.DataFrame:
    """
    Lee un CSV desde raw, agrega timestamp de ingesta y guarda en bronze
    sin hacer ninguna transformación.

    Args:
        csv_path (str | None): Ruta del CSV original (raw). Si None, se determina automáticamente.
        bronze_path (str | None): Carpeta donde se guardará el Parquet bronze. Si None, se determina automáticamente.

    Returns:
        pl.DataFrame: DataFrame cargado con columna de timestamp.
    """
    # Determinar base_path si es necesario
    if csv_path is None or bronze_path is None:
        try:
            base_path = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
        except NameError:
            # Modo notebook
            base_path = os.path.abspath(os.path.join(os.getcwd(), ".."))

        if csv_path is None:
            csv_path = os.path.join(base_path, "data", "raw", "SpotifyFeatures.csv")
        if bronze_path is None:
            bronze_path = os.path.join(base_path, "data", "bronze")

    os.makedirs(bronze_path, exist_ok=True)

    # Leer CSV usando Polars
    df = pl.read_csv(csv_path, use_pyarrow=True, encoding="utf8")

    # Agregar timestamp de ingesta
    df = df.with_columns([
        pl.lit(datetime.now()).cast(pl.Datetime("us")).alias("ingest_timestamp")
    ])

    # Guardar en bronze como Parquet
    file_name = os.path.basename(csv_path).replace(".csv", "_bronze.parquet")
    bronze_file = os.path.join(bronze_path, file_name)
    df.write_parquet(bronze_file)

    print(f"✅ Guardado en bronze completado: {bronze_file} con {len(df)} filas")
    return df


# ------------------------------
# 🚀 Ejecución directa segura
# ------------------------------
if __name__ == "__main__" or ('raw_path' in globals() and 'bronze_path' in globals()):
    df_bronze = load_bronze()

    


# ### Transform Silver: limpieza y guardado incremental

# In[6]:


def transform_silver(bronze_file: str | None = None,
                     silver_path: str | None = None,
                     output_name: str = "SpotifyFeatures_silver.parquet") -> pl.DataFrame:
    """
    Limpieza y transformación de Bronze a Silver.

    Args:
        bronze_file (str | None): Ruta al archivo Parquet Bronze. Si None, se determina automáticamente.
        silver_path (str | None): Carpeta donde se guardará Silver. Si None, se determina automáticamente.
        output_name (str): Nombre del archivo Silver a guardar.

    Returns:
        pl.DataFrame: DataFrame transformado listo para Silver.
    """
    # Determinar base_path si es necesario
    if bronze_file is None or silver_path is None:
        try:
            base_path = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
        except NameError:
            # Modo notebook
            base_path = os.path.abspath(os.path.join(os.getcwd(), ".."))
        if bronze_file is None:
            bronze_file = os.path.join(base_path, "data", "bronze", "SpotifyFeatures_bronze.parquet")
        if silver_path is None:
            silver_path = os.path.join(base_path, "data", "silver")

    # 1️⃣ Leer dataset desde Bronze
    df = pl.read_parquet(bronze_file, use_pyarrow=True)

    # 2️⃣ Eliminar duplicados
    df = df.unique()

    # 3️⃣ Detectar columnas por tipo
    string_cols = [col for col, dtype in zip(df.columns, df.dtypes) if dtype == pl.Utf8]
    numeric_cols = [col for col, dtype in zip(df.columns, df.dtypes) if dtype in [pl.Int64, pl.Float64]]

    # 4️⃣ Reemplazar nulos y convertir tipos
    if string_cols:
        df = df.with_columns([pl.col(col).fill_null("N/A") for col in string_cols])
    if numeric_cols:
        df = df.with_columns([pl.col(col).cast(pl.Float64).fill_null(float("nan")) for col in numeric_cols])

    # 5️⃣ Normalizar strings (title case)
    if string_cols:
        df = df.with_columns([pl.col(col).str.to_titlecase() for col in string_cols])

    # 6️⃣ Transformaciones adicionales: timestamp + duration_s
    transformations = [pl.lit(datetime.now()).cast(pl.Datetime("us")).alias("processed_timestamp")]
    if "duration_ms" in df.columns:
        transformations.append((pl.col("duration_ms") / 1000).alias("duration_s"))

    df = df.with_columns(transformations)

    # Eliminar columna original duration_ms si existe
    if "duration_ms" in df.columns:
        df = df.drop("duration_ms")

    # Crear carpeta Silver si no existe
    os.makedirs(silver_path, exist_ok=True)

    # Guardar Parquet
    silver_file = os.path.join(silver_path, output_name)
    df.write_parquet(silver_file)

    print(f"✅ Transformación a Silver completada: {silver_file}")
    return df


# ------------------------------
# 🚀 Ejecución directa segura
# ------------------------------
if __name__ == "__main__" or ('silver_path' in globals() and 'bronze_path' in globals()):
    df_silver = transform_silver()


    


# ### Aggregate Gold: agregación incremental

# In[7]:


import os
import polars as pl

def aggregate_gold(silver_file: str | None = None,
                   gold_path: str | None = None,
                   genre_file_name: str = "genre_popularity.parquet",
                   artist_file_name: str = "artist_features.parquet") -> None:
    """
    Genera las tablas Gold a partir de Silver.

    Args:
        silver_file (str | None): Ruta al archivo Silver Parquet. Si None, se determina automáticamente.
        gold_path (str | None): Carpeta donde se guardará Gold. Si None, se determina automáticamente.
        genre_file_name (str): Nombre del archivo de popularidad por género.
        artist_file_name (str): Nombre del archivo de características por artista.
    """

    # 1️⃣ Determinar base_path según script o notebook
    try:
        base_path = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    except NameError:
        # Notebook
        base_path = os.path.abspath(os.path.join(os.getcwd(), ".."))

    # 2️⃣ Definir rutas por defecto si no se pasan
    if silver_file is None:
        silver_file = os.path.join(base_path, "data", "silver", "SpotifyFeatures_silver.parquet")
    if gold_path is None:
        gold_path = os.path.join(base_path, "data", "gold")

    # 3️⃣ Crear carpeta Gold si no existe
    os.makedirs(gold_path, exist_ok=True)

    genre_file = os.path.join(gold_path, genre_file_name)
    artist_file = os.path.join(gold_path, artist_file_name)

    # 4️⃣ Si ya existen, no recalcular
    if os.path.exists(genre_file) and os.path.exists(artist_file):
        print("✅ Gold ya generado, archivos existentes encontrados.")
        return

    # 5️⃣ Leer Silver
    df = pl.read_parquet(silver_file, use_pyarrow=True)

    # 6️⃣ Tabla 1: Popularidad promedio por género
    genre_popularity = (
        df.group_by("genre")
          .agg([
              pl.col("popularity").mean().alias("avg_popularity"),
              pl.count("track_id").alias("track_count")
          ])
          .sort("avg_popularity", descending=True)
    )
    genre_popularity.write_parquet(genre_file)

    # 7️⃣ Tabla 2: Promedio de características musicales por artista
    features_cols = ["acousticness", "danceability", "energy", "instrumentalness",
                     "liveness", "loudness", "speechiness", "valence"]
    artist_features = (
        df.group_by("artist_name")
          .agg([pl.col(col).mean().alias(f"avg_{col}") for col in features_cols])
    )
    artist_features.write_parquet(artist_file)

    print(f"✅ Gold generado y guardado en: {gold_path}")


# ------------------------------
# 🚀 Ejecución directa segura
# ------------------------------
if __name__ == "__main__" or ('gold_path' in globals() and 'silver_path' in globals()):
    aggregate_gold()


# ### Ejecución del Pipeline

# In[8]:


def run_pipeline():
    """
    Ejecuta todo el pipeline ETL de Spotify: raw -> bronze -> silver -> gold
    Validando carpetas y archivos en cada paso.
    Retorna los DataFrames finales de Bronze y Silver.
    """
    # ------------------------------
    # 1️⃣ Determinar base_path
    # ------------------------------
    try:
        base_path = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    except NameError:
        # Modo notebook
        base_path = os.path.abspath(os.path.join(os.getcwd(), ".."))

    # ------------------------------
    # 2️⃣ Definir rutas de carpetas
    # ------------------------------
    raw_path = os.path.join(base_path, "data", "raw")
    bronze_path = os.path.join(base_path, "data", "bronze")
    silver_path = os.path.join(base_path, "data", "silver")
    gold_path = os.path.join(base_path, "data", "gold")

    # Crear carpetas si no existen
    for p in [raw_path, bronze_path, silver_path, gold_path]:
        os.makedirs(p, exist_ok=True)

    # ------------------------------
    # 3️⃣ Extraer datos a raw
    # ------------------------------
    print("🚀 Extrayendo datos raw...")
    try:
        csv_files = extract_to_raw(base_path)
        if not csv_files:
            raise FileNotFoundError("No se encontraron CSVs en raw.")
    except Exception as e:
        print(f"❌ Error en extracción raw: {e}")
        return None, None

    # ------------------------------
    # 4️⃣ Cargar Bronze
    # ------------------------------
    print("🚀 Cargando Bronze...")
    df_bronze = None
    for csv_file in csv_files:
        try:
            df_bronze = load_bronze(csv_file, bronze_path)
        except Exception as e:
            print(f"❌ Error cargando Bronze desde {csv_file}: {e}")
            continue

    if df_bronze is None:
        print("❌ No se pudo cargar ningún archivo en Bronze.")
        return None, None

    # ------------------------------
    # 5️⃣ Transformar Silver
    # ------------------------------
    print("🚀 Transformando Silver...")
    bronze_file = os.path.join(bronze_path, "SpotifyFeatures_bronze.parquet")
    try:
        df_silver = transform_silver(bronze_file, silver_path)
    except Exception as e:
        print(f"❌ Error transformando Silver: {e}")
        return df_bronze, None

    # ------------------------------
    # 6️⃣ Agregar Gold
    # ------------------------------
    print("🚀 Agregando Gold...")
    silver_file = os.path.join(silver_path, "SpotifyFeatures_silver.parquet")
    try:
        aggregate_gold(silver_file, gold_path)
    except Exception as e:
        print(f"❌ Error generando Gold: {e}")

    print("✅ Pipeline completado.")
    return df_bronze, df_silver


# ------------------------------
# 🚀 Ejecutar automáticamente
# ------------------------------
if __name__ == "__main__":
    df_bronze, df_silver = run_pipeline()


