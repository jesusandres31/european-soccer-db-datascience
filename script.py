import sqlite3

import pandas as pd

# Ruta del archivo SQLite en Google Colab
DB_PATH = "./db/database.sqlite"  # Asegúrate de subir la base de datos a /content/

# Conectar a la base de datos
conn = sqlite3.connect(DB_PATH)
cursor = conn.cursor()


# Obtener las tablas disponibles
def get_tables():
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cursor.fetchall()
    return [table[0] for table in tables]


# Cargar una tabla específica en un DataFrame
def load_table(table_name):
    query = f"SELECT * FROM {table_name}"
    return pd.read_sql(query, conn)


# Mostrar las tablas disponibles
tables = get_tables()
print("Tablas disponibles en la base de datos:")
print(tables)

# Cargar una tabla de ejemplo (por defecto 'Player')
table_name = "Player"  # Puedes cambiar esto por otra tabla
if table_name in tables:
    df = load_table(table_name)
    print(f"Mostrando las primeras filas de la tabla {table_name}:")
    print(df.head())
else:
    print(f"La tabla {table_name} no está en la base de datos.")

# Cerrar conexión
conn.close()
