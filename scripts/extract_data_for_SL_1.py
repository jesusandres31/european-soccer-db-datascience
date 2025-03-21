"""
Explicación:
- Cargamos las tablas 'Team', 'Team_Attributes' y 'Match'.
- Ordenamos 'Team_Attributes' por 'team_api_id' y 'date' en orden descendente para que el más reciente quede primero.
- Agrupamos por 'team_api_id' y seleccionamos el primer registro de cada grupo (el más reciente).
- Hacemos un 'left join' con 'Team' para unir los atributos más recientes de cada equipo.
- Guardamos el resultado en un CSV para usarlo en el entrenamiento del modelo de predicción.
- También exportamos la tabla 'Match' en un archivo CSV para su posterior análisis.

¿Por qué elegimos usar solo el registro más reciente de 'Team_Attributes'?
- Refleja mejor el estado actual del equipo en el momento más cercano a los partidos recientes.
- Evita introducir datos antiguos que pueden no ser representativos del rendimiento actual.
- Permite mantener una estructura de datos más simple con un solo registro por equipo.
"""

import os
import sqlite3

import pandas as pd

# Ruta de la base de datos SQLite
DB_PATH = "./db/database.sqlite"
OUTPUT_DIR = "./output"
os.makedirs(OUTPUT_DIR, exist_ok=True)  # Crear carpeta de salida si no existe


def load_table(db_path, table_name):
    # Carga una tabla específica de la base de datos SQLite en un DataFrame de Pandas.
    try:
        with sqlite3.connect(db_path) as conn:
            return pd.read_sql(f"SELECT * FROM {table_name}", conn)
    except sqlite3.Error as e:
        print(f"Error al cargar la tabla {table_name}: {e}")
        return None


# Cargar las tablas Team, Team_Attributes y Match
df_team = load_table(DB_PATH, "Team")
df_team_attr = load_table(DB_PATH, "Team_Attributes")
df_match = load_table(DB_PATH, "Match")

if df_team is not None and df_team_attr is not None:
    # Seleccionar el registro más reciente de atributos para cada equipo
    df_team_attr_latest = df_team_attr.sort_values(
        by=["team_api_id", "date"], ascending=[True, False]
    )
    df_team_attr_latest = (
        df_team_attr_latest.groupby("team_api_id").first().reset_index()
    )

    # Unir los atributos más recientes con la tabla Team
    df_merged = df_team.merge(df_team_attr_latest, on="team_api_id", how="left")

    # Eliminar columnas innecesarias
    df_merged.drop(
        columns=[
            "team_api_id",
            "team_fifa_api_id_y",
            "team_fifa_api_id_x",
            "id_y",
            "id_x",
        ],
        errors="ignore",
        inplace=True,
    )

    # Guardar el resultado en un archivo CSV
    output_path = os.path.join(OUTPUT_DIR, "Teams_Full_Latest.csv")
    df_merged.to_csv(output_path, index=False, encoding="utf-8")
    print(
        f"Tabla Team desnormalizada con los atributos más recientes guardada en {output_path}"
    )
else:
    print("Error al cargar las tablas.")

if df_match is not None:
    # Eliminar columnas innecesarias de la tabla Match
    df_match.drop(columns=["id_x"], errors="ignore", inplace=True)

    # Guardar la tabla Match en un archivo CSV
    match_output_path = os.path.join(OUTPUT_DIR, "Match.csv")
    df_match.to_csv(match_output_path, index=False, encoding="utf-8")
    print(f"Tabla Match guardada en {match_output_path}")
else:
    print("Error al cargar la tabla Match.")
