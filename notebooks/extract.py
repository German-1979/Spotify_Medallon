import os
import shutil
import kagglehub

# ------------------------------
# Extract único: descargar CSV a raw
# ------------------------------
def extract_to_raw(target_path):
    """
    Descarga el dataset de Kaggle 'Ultimate Spotify Tracks DB' y copia
    todos los archivos CSV a la carpeta 'raw' dentro de target_path.

    Args:
        target_path (str): Ruta donde se encuentra la carpeta 'data'.

    Returns:
        List[str]: Rutas completas de los CSV copiados en raw.
    """
    raw_path = os.path.join(target_path, "raw")
    os.makedirs(raw_path, exist_ok=True)
    print(f"[extract_to_raw] Carpeta raw lista en: {raw_path}")

    # Descargar dataset (se guarda en cache de kagglehub)
    cache_path = kagglehub.dataset_download("zaheenhamidani/ultimate-spotify-tracks-db")
    print(f"[extract_to_raw] Dataset descargado en cache: {cache_path}")

    csv_files = []
    for file_name in os.listdir(cache_path):
        if file_name.endswith(".csv"):
            src = os.path.join(cache_path, file_name)
            shutil.copy(src, raw_path)
            csv_files.append(os.path.join(raw_path, file_name))
            print(f"[extract_to_raw] Copiado a raw: {file_name}")

    print(f"[extract_to_raw] CSV en raw: {[os.path.basename(f) for f in csv_files]}")
    return csv_files

# ------------------------------
# Ejecutar
# ------------------------------
if __name__ == "__main__":
    target_path = r"C:\Users\germa\Desktop\Carpetas\Data_Engineer_Specialist\Spotify_Medallon\data"
    csv_files = extract_to_raw(target_path)
    print("\nArchivos CSV descargados en raw:", csv_files)