#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Script para la zona de acceso.
Crea vistas analíticas y conjuntos de datos optimizados para cada perfil de usuario.
"""

import pandas as pd
import boto3
import numpy as np
from datetime import datetime
from sqlalchemy import create_engine
from utils import setup_s3_client, log_operation, download_dataframe_from_minio

def prepare_access_zone():
    """Crea conjuntos de datos optimizados para análisis en la zona de acceso."""
    print("Preparando zona de acceso para análisis...")
    
    # Configurar cliente S3 (MinIO)
    s3_client = setup_s3_client()
    process_bucket = "process-zone"
    access_bucket = "access-zone"
    
    # 1. Crear vista analítica de tráfico para científicos de datos
    print("Creando vista analítica de tráfico...")
    try:
        traffic_df = download_dataframe_from_minio(process_bucket, "movilidad/trafico_procesado.parquet")
        
        # Crear análisis de congestión por hora y tipo de vehículo
        traffic_analysis = create_traffic_congestion_analysis(traffic_df)
        
        # Guardar en múltiples formatos para diferentes usuarios
        # Parquet para Python (científicos de datos)
        save_to_access_zone(traffic_analysis, "analytics/trafico_congestion.parquet", access_bucket)
        # CSV para usuarios menos técnicos
        csv_buffer = traffic_analysis.to_csv(index=False).encode()
        s3_client.put_object(
            Bucket=access_bucket,
            Key="analytics/trafico_congestion.csv",
            Body=csv_buffer
        )
    except Exception as e:
        print(f"Error al crear vista analítica de tráfico: {e}")
    
    # 2. Crear vista analítica de BiciMAD para gestores con SQL
    print("Creando vista analítica de BiciMAD...")
    try:
        bicimad_df = download_dataframe_from_minio(process_bucket, "movilidad/bicimad_procesado.parquet")
        
        # Crear análisis de rutas populares
        bicimad_routes = create_bicimad_route_analysis(bicimad_df)
        
        # Guardar para análisis
        save_to_access_zone(bicimad_routes, "analytics/bicimad_rutas.parquet", access_bucket)
        
        # Cargar en PostgreSQL para consultas SQL
        load_to_postgres(bicimad_routes, "bicimad_rutas")
    except Exception as e:
        print(f"Error al crear vista analítica de BiciMAD: {e}")
    
    # 3. Crear vista analítica de parkings para ciudadanos
    print("Creando vista analítica de parkings...")
    try:
        parkings_df = download_dataframe_from_minio(process_bucket, "movilidad/parkings_procesado.parquet")
        
        # Crear análisis de ocupación de parkings
        parking_occupancy = create_parking_occupancy_analysis(parkings_df)
        
        # Guardar para análisis y visualización
        save_to_access_zone(parking_occupancy, "analytics/parking_ocupacion.parquet", access_bucket)
        load_to_postgres(parking_occupancy, "parking_ocupacion")
    except Exception as e:
        print(f"Error al crear vista analítica de parkings: {e}")
    
    # 4. Crear vista integrada de infraestructuras y población
    print("Creando vista integrada de infraestructuras y población...")
    try:
        infra_df = download_dataframe_from_minio(process_bucket, "municipal/infraestructuras_procesado.parquet")
        
        # Crear análisis de relación entre infraestructuras y población
        infra_population = create_infrastructure_population_analysis(infra_df)
        
        # Guardar para análisis
        save_to_access_zone(infra_population, "analytics/infraestructuras_poblacion.parquet", access_bucket)
        load_to_postgres(infra_population, "infraestructuras_poblacion")
    except Exception as e:
        print(f"Error al crear vista integrada de infraestructuras: {e}")
    
    print("Zona de acceso preparada exitosamente.")

def save_to_access_zone(df, key, bucket_name):
    """Guarda un DataFrame en la zona de acceso."""
    try:
        # Crear buffer en memoria para el archivo parquet
        parquet_buffer = df.to_parquet()
        
        # Configurar cliente S3
        s3_client = setup_s3_client()
        
        # Subir archivo parquet
        s3_client.put_object(
            Bucket=bucket_name,
            Key=key,
            Body=parquet_buffer
        )
        
        log_operation(
            operation="ACCESS",
            source=f"process-zone",
            target=f"s3://{bucket_name}/{key}",
            rows=len(df),
            status="SUCCESS"
        )
        
        print(f"Vista analítica guardada en s3://{bucket_name}/{key}")
    except Exception as e:
        print(f"Error al guardar vista analítica: {e}")
        log_operation(
            operation="ACCESS",
            source=f"process-zone",
            target=f"s3://{bucket_name}/{key}",
            rows=len(df) if isinstance(df, pd.DataFrame) else 0,
            status="ERROR",
            error=str(e)
        )

def load_to_postgres(df, table_name):
    """Carga un DataFrame en PostgreSQL para análisis SQL."""
    try:
        # Configurar conexión a PostgreSQL
        engine = create_engine('postgresql://postgres:postgres@postgres:5432/madrid_sostenible')
        
        # Cargar DataFrame
        df.to_sql(table_name, engine, if_exists='replace', index=False)
        
        log_operation(
            operation="LOAD",
            source=f"access-zone",
            target=f"postgres/{table_name}",
            rows=len(df),
            status="SUCCESS"
        )
        
        print(f"Datos cargados en PostgreSQL: {table_name}")
    except Exception as e:
        print(f"Error al cargar datos en PostgreSQL: {e}")
        log_operation(
            operation="LOAD",
            source=f"access-zone",
            target=f"postgres/{table_name}",
            rows=len(df) if isinstance(df, pd.DataFrame) else 0,
            status="ERROR",
            error=str(e)
        )

# Funciones específicas para crear vistas analíticas

def create_traffic_congestion_analysis(df):
    """Crea análisis de congestión de tráfico por hora y tipo de vehículo."""
    # Implementar análisis específico
    # ...
    return df

def create_bicimad_route_analysis(df):
    """Crea análisis de rutas populares de BiciMAD."""
    # Implementar análisis específico
    # ...
    return df

def create_parking_occupancy_analysis(df):
    """Crea análisis de ocupación de parkings."""
    # Implementar análisis específico
    # ...
    return df

def create_infrastructure_population_analysis(df):
    """Crea análisis de relación entre infraestructuras y población."""
    # Implementar análisis específico
    # ...
    return df

if __name__ == "__main__":
    prepare_access_zone()
