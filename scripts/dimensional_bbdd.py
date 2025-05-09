import pandas as pd
from minio import Minio
from io import BytesIO
import sqlalchemy
from sqlalchemy import create_engine

# Conexión con MinIO en localhost
client = Minio(
    "localhost:9000",  # Endpoint de MinIO en localhost
    access_key="minioadmin",  
    secret_key="minioadmin",  
    secure=False  
)

# Verificar si el bucket existe
if not client.bucket_exists("access-zone"):
    print("Bucket 'access-zone' no encontrado.")
else:
    # Leer el archivo Parquet desde el bucket
    try:
        # Descargar el archivo Parquet
        obj = client.get_object("access-zone", "analytics/rutas_users.parquet")
        
        # Leer el archivo Parquet en Pandas
        df = pd.read_parquet(BytesIO(obj.read()))
    except Exception as e:
        print(f"Error al leer el archivo Parquet: {e}")

engine = sqlalchemy.create_engine("postgresql://postgres:ejemplo@localhost:5432/madrid_sostenible")
sql2 = df.to_sql('rutas_users', con=engine, if_exists='replace', index=False)




# Verificar si el bucket existe
if not client.bucket_exists("access-zone"):
    print("Bucket 'access-zone' no encontrado.")
else:
    # Leer el archivo Parquet desde el bucket
    try:
        # Descargar el archivo Parquet
        obj = client.get_object("access-zone", "analytics/parkings_unidos.parquet")
        
        # Leer el archivo Parquet en Pandas
        df = pd.read_parquet(BytesIO(obj.read()))
    except Exception as e:
        print(f"Error al leer el archivo Parquet: {e}")

engine = create_engine("postgresql://postgres:ejemplo@localhost:5432/madrid_sostenible")
sql2 = df.to_sql('parkings_unidos', con=engine, if_exists='replace', index=False)


# Verificar si el bucket existe
if not client.bucket_exists("access-zone"):
    print("Bucket 'access-zone' no encontrado.")
else:
    # Leer el archivo Parquet desde el bucket
    try:
        # Descargar el archivo Parquet
        obj = client.get_object("access-zone", "analytics/parkings-visualizaciones.parquet")
        
        # Leer el archivo Parquet en Pandas
        df = pd.read_parquet(BytesIO(obj.read()))
    except Exception as e:
        print(f"Error al leer el archivo Parquet: {e}")

engine = create_engine("postgresql://postgres:ejemplo@localhost:5432/madrid_sostenible")
sql2 = df.to_sql('parkings_viusalizaciones', con=engine, if_exists='replace', index=False)