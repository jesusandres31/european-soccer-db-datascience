"""
Este script extrae información de la base de datos SQLite y genera un archivo CSV con la información de los jugadores,
manteniendo solo el último registro disponible de atributos para cada jugador.

Motivo: Queremos crear una red de agrupación de jugadores en niveles de calidad, por lo que necesitamos trabajar con los
valores más recientes de sus atributos en lugar de múltiples registros históricos.
"""

import os
import sqlite3

import pandas as pd

# Ruta del archivo SQLite
DB_PATH = "./db/database.sqlite"
OUTPUT_DIR = "./output"

# Crear la carpeta de salida si no existe
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Conectar a la base de datos
conn = sqlite3.connect(DB_PATH)

# Cargar las tablas
df_players = pd.read_sql("SELECT * FROM Player", conn)
df_attributes = pd.read_sql("SELECT * FROM Player_Attributes", conn)

# Obtener el último registro de atributos por jugador
df_attributes_sorted = df_attributes.sort_values(
    by=["player_api_id", "date"], ascending=[True, False]
)
df_last_attributes = df_attributes_sorted.drop_duplicates(
    subset=["player_api_id"], keep="first"
)

# Unir con la tabla de jugadores
df_final = df_players.merge(df_last_attributes, on="player_api_id", how="left")

# Eliminar columnas no necesarias
df_final.drop(
    columns=[
        "id_x",
        "id_y",
        "player_api_id",
        "player_fifa_api_id_x",
        "player_fifa_api_id_y",
        "date",
    ],
    inplace=True,
)

# Exportar a CSV
output_path = os.path.join(OUTPUT_DIR, "Players_Full_Latest.csv")
df_final.to_csv(output_path, index=False)

# Cerrar conexión
conn.close()

print(f"Exportación completada: {output_path}")
