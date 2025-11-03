# 🎧 Spotify Medallón - Data Engineering Project


## 🚀 Objetivo del Proyecto

El propósito de este proyecto es construir un **pipeline ETL automatizado** siguiendo una **arquitectura medallón (Bronze → Silver → Gold)** para analizar datos musicales de **Spotify**.  
A través de este flujo se busca:

- Ingerir, transformar y limpiar datos de canciones, artistas y géneros.  
- Generar tablas analíticas optimizadas en la capa **Gold**.  
- Visualizar métricas clave mediante dashboards interactivos y reportes.


---

## 🧱 Estructura del Proyecto

![alt text](estructura_proyecto-1.png)

---

## ⚙️ Explicación del pipeline ETL (`etl_pipeline.ipynb`)

El notebook `etl_pipeline.ipynb` implementa todo el flujo ETL del proyecto, dividido en tres fases:

1. **Extract (Raw → Bronze)**  
   - Carga el dataset original de Spotify (`SpotifyFeatures.csv` o fuente externa).  
   - Genera un archivo parquet en bronze

2. **Transform (Bronze Silver)**  
   - Estandariza tipos de datos.  
   - Aplica reglas de limpieza, tipificación de columnas y validación de valores nulos.
   

3. **Load (Silver → Gold)**  
   - Ejecuta transformaciones agregadas mediante la función interna el `aggregate_gold.py`.  
   - Crea dos tablas principales:
     - `artist_features.parquet` → métricas promedio por artista.  
     - `genre_popularity.parquet` → métricas promedio por género.

📦 **Salida:**  
Los resultados procesados se almacenan automáticamente en `data/gold/`.

---

## 🧮 Visualización de Resultados

### 1️⃣ Mostrar tablas Gold en consola

Puedes revisar los resultados de la capa Gold directamente desde la terminal ejecutando:

   - `python show_table_gold.py`


👉 Este script lee los archivos .parquet generados en la carpeta data/gold/ y muestra en consola los primeros registros de:

   - `artist_features.parque`
   - `genre_popularity.parquet`


![Popularidad por género](./data/gold/popularidad_por_genero.png)


![Promedio de características por artista](./data/gold/promedio_caracteristicas_por_artista.png)

---


### 2️⃣ Dashboard interactivo (Streamlit)

El archivo spotify_dashboard.py permite explorar visualmente los resultados combinados de las capas Silver y Gold.
Para ejecutarlo:

   - `streamlit run spotify_dashboard.py`


Esto abrirá un localhost (por defecto en http://localhost:8501) con los siguientes gráficos:

   - Popularidad promedio por género
   - Energía vs Valencia por artista
   - Danceability vs Energy (canciones más movidas)
   - Canciones felices vs tristes
   - Tempo y Loudness por género
   - Distribución de canciones instrumentales

Cada gráfico incluye una breve conclusión automática

---


### 💻 Cómo ejecutar el proyecto localmente (desde Visual Studio Code)

### 1️⃣ Clonar el repositorio

En tu terminal o consola de VSCode, ejecuta:

   - `git clone https://github.com/<tu_usuario>/<tu_repositorio>.git` en:

   - `cd Spotify_Medallon`

(Reemplaza <tu_usuario> y <tu_repositorio> por tu nombre y repo reales).

---


### 2️⃣ Crear y activar entorno virtual

En Windows (PowerShell)

Crea el entorno virtual: `python -m venv venv`, y luego actívalo con: `venv\Scripts\activate`

---


### 3️⃣ Instalar dependencias

   - `pip install -r requirements.txt`

---


### 4️⃣ Ejecutar el pipeline ETL

Ejecuta en VSC:

   - `jupyter notebook notebooks/etl_pipeline.ipynb`

O, si prefieres ver el flujo completo y depurarlo paso a paso:

   - `jupyter notebook notebooks/etl_pipeline.ipynb`

---


### 5️⃣ Visualizar resultados

Ver tablas Gold en consola:

   - `python show_table_gold.py`

Abrir dashboard interactivo:

   - `streamlit run spotify_dashboard.py`
   

![Gráficos Streamlit](./data/gold/gráficos_streamlit.png)

---


### 🧰 Tecnologías utilizadas

| Herramienta        | Propósito                                                            |
| ------------------ | -------------------------------------------------------------------- |
| **Python**         | Lenguaje principal del proyecto                                      |
| **Polars**         | Procesamiento eficiente de datos (similar a pandas, pero más rápido) |
| **Streamlit**      | Creación del dashboard interactivo                                   |
| **Plotly Express** | Gráficos dinámicos y personalizables                                 |
| **Git & GitHub**   | Control de versiones y despliegue                                    |
| **Parquet**        | Almacenamiento columnar optimizado                                   |

---


### 📊 Estructura de Capas Medallón

| Capa       | Propósito                              | Formato    |
| ---------- | -------------------------------------- | ---------- |
| **Bronze** | Datos crudos sin transformar           | CSV / JSON |
| **Silver** | Datos limpios, tipificados y validados | Parquet    |
| **Gold**   | Datos agregados y métricas analíticas  | Parquet    |

---


### 📈 Resultado Final

El proyecto entrega una visión analítica y exploratoria de datos de Spotify, permitiendo comprender:

   - Qué géneros son más populares.
   - Qué artistas producen música más enérgica o alegre.
   - Cómo se distribuyen los estilos según danceability, energy, tempo y valence.

---


### 🧑‍💻 Autor

Germán Domínguez
Especialista en datos, análisis y transformación digital.
