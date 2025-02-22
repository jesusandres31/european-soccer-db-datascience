import os
import sqlite3

import pandas as pd

# Conectar a la base de datos SQLite
DB_PATH = "./db/database.sqlite"
OUTPUT_DIR = "./output"
os.makedirs(OUTPUT_DIR, exist_ok=True)  # Crear carpeta de salida si no existe

conn = sqlite3.connect(DB_PATH)

# 1. Cargar la tabla Match con solo las columnas necesarias
query_match = """
SELECT 
    m.id AS id_x,
    m.country_id, 
    m.league_id, 
    m.season, 
    m.stage, 
    m.date, 
    m.home_team_api_id, 
    m.away_team_api_id, 
    m.home_team_goal, 
    m.away_team_goal
FROM Match m
"""
df_match = pd.read_sql(query_match, conn)

# 2. Reemplazar country_id por el nombre del país (Country)
query_country = "SELECT id, name FROM Country"
df_country = pd.read_sql(query_country, conn)
df_match = df_match.merge(df_country, left_on="country_id", right_on="id", how="left")

df_match.rename(columns={"name": "country_name", "id": "id_y"}, inplace=True)

# 3. Reemplazar league_id por el nombre de la liga (League)
query_league = "SELECT id, name FROM League"
df_league = pd.read_sql(query_league, conn)
df_match = df_match.merge(df_league, left_on="league_id", right_on="id", how="left")

df_match.rename(columns={"name": "league_name"}, inplace=True)

# 4. Obtener los nombres de los equipos (Team)
query_team = "SELECT team_api_id, team_long_name FROM Team"
df_team = pd.read_sql(query_team, conn)

# Unir por home_team_api_id
df_match = df_match.merge(
    df_team, left_on="home_team_api_id", right_on="team_api_id", how="left"
)
df_match.rename(columns={"team_long_name": "home_team"}, inplace=True)

# Unir por away_team_api_id
df_match = df_match.merge(
    df_team, left_on="away_team_api_id", right_on="team_api_id", how="left"
)
df_match.rename(columns={"team_long_name": "away_team"}, inplace=True)

# 5. Obtener los últimos registros de Team_Attributes por equipo
query_team_attributes = """
SELECT ta.*
FROM Team_Attributes ta
JOIN (
    SELECT team_api_id, MAX(date) as last_date
    FROM Team_Attributes
    GROUP BY team_api_id
) latest
ON ta.team_api_id = latest.team_api_id AND ta.date = latest.last_date
"""
df_team_attr = pd.read_sql(query_team_attributes, conn)

# Seleccionar solo las columnas necesarias
cols_team_attr = [
    "team_api_id",
    "buildUpPlaySpeed",
    "buildUpPlaySpeedClass",
    "buildUpPlayDribbling",
    "buildUpPlayDribblingClass",
    "buildUpPlayPassing",
    "buildUpPlayPassingClass",
    "buildUpPlayPositioningClass",
    "chanceCreationPassing",
    "chanceCreationPassingClass",
    "chanceCreationCrossing",
    "chanceCreationCrossingClass",
    "chanceCreationShooting",
    "chanceCreationShootingClass",
    "chanceCreationPositioningClass",
    "defencePressure",
    "defencePressureClass",
    "defenceAggression",
    "defenceAggressionClass",
    "defenceTeamWidth",
    "defenceTeamWidthClass",
    "defenceDefenderLineClass",
]
df_team_attr = df_team_attr[cols_team_attr]

# Unir atributos de home_team
df_match = df_match.merge(
    df_team_attr, left_on="home_team_api_id", right_on="team_api_id", how="left"
)
df_match.rename(
    lambda x: f"home_{x}" if x in cols_team_attr else x, axis=1, inplace=True
)

# Unir atributos de away_team
df_match = df_match.merge(
    df_team_attr, left_on="away_team_api_id", right_on="team_api_id", how="left"
)
df_match.rename(
    lambda x: f"away_{x}" if x in cols_team_attr else x, axis=1, inplace=True
)

# Eliminar columnas innecesarias
df_match.drop(
    columns=[
        "id",
        "id_x",
        "id_y",
        "country_id",
        "league_id",
        "home_team_api_id",
        "away_team_api_id",
        "team_api_id",
    ],
    errors="ignore",
    inplace=True,
)

# Guardar en CSV
output_path = f"{OUTPUT_DIR}/Match_Full.csv"
df_match.to_csv(output_path, index=False)

# Cerrar conexión
conn.close()

print(f"Archivo guardado: {output_path}")
