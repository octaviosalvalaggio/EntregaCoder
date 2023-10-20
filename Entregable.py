import requests
import json
from sqlalchemy import create_engine
import pandas as pd
from decouple import config
from sqlalchemy.exc import IntegrityError  

# Función para obtener tasas de cambio de criptomonedas a través de la API
def obtener_tasas_de_cambio(criptos):
    url = 'http://api.exchangerate.host/live?access_key=aff6cf9aaecb91816550ecc09abb624b'
    params = {'symbols': ','.join(criptos)}
    
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error al hacer la solicitud a la API: {e}")
        return None

# Lista de símbolos de criptomonedas (10 Categorías)
symbols = ['BGN', 'BBD', 'EUR', 'BTN', 'AZN', 'KZT', 'KES', 'LYD', 'MDL', 'AMD']

# Se llama a la función para obtener las tasas de cambio
tasas_de_cambio = obtener_tasas_de_cambio(symbols)

# Verificar si las tasas de cambio se obtuvieron con éxito antes de continuar
if tasas_de_cambio is not None:
    if "motd" in tasas_de_cambio:
        del tasas_de_cambio["motd"]

    # Convertir los datos en un DataFrame de Pandas
    df = pd.DataFrame(tasas_de_cambio)

    # Imprimir el DataFrame
    print(df)
    
    # Credenciales desde el archivo .env
    username = config('DB_USERNAME')
    password = config('DB_PASSWORD')
    host = config('DB_HOST')
    port = config('DB_PORT')
    database_name = config('DB_NAME')

    # URL de conexión
    url = f'postgresql://{username}:{password}@{host}:{port}/{database_name}'

    # Conexión a la base de datos
    conn = create_engine(url)

    try:
        # Insertar los datos en la tabla 'tasas_de_cambio', evitando duplicados
        df.to_sql('tasas_de_cambio', conn, if_exists='replace', index=False)
        print("Datos insertados en la tabla 'tasas_de_cambio'")
    except IntegrityError as e:
        print("Error al insertar datos en la tabla 'tasas_de_cambio':", e)
else:
    print("No se obtuvieron tasas de cambio.")
