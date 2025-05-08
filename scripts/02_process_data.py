#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Script para la zona de procesamiento.
Limpia, transforma y estandariza los datos de la zona Raw.
"""

import pandas as pd
import numpy as np
from datetime import datetime
import json
import boto3
from utils import setup_s3_client, log_operation, download_dataframe_from_minio

def process_data():
    """Procesa los datos de la zona Raw y los almacena en la zona de procesamiento."""
    print("Iniciando procesamiento de datos...")
    
    # Configurar cliente S3 (MinIO)
    s3_client = setup_s3_client()
    raw_bucket = "raw-ingestion-zone"
    process_bucket = "process-zone"
    
    # 1. Procesar datos de tráfico
    print("Procesando datos de tráfico...")
    traffic_key = get_latest_file_key(s3_client, raw_bucket, "movilidad/trafico")
    if traffic_key:
        traffic_df = download_dataframe_from_minio(raw_bucket, traffic_key)
        
        # Limpiar y transformar datos
        processed_traffic_df = process_traffic_data(traffic_df)
        
        # Guardar en formato parquet
        save_to_process_zone(processed_traffic_df, "movilidad/trafico_procesado.parquet", process_bucket)
    
    # 2. Procesar datos de BiciMAD
    print("Procesando datos de BiciMAD...")
    bicimad_key = get_latest_file_key(s3_client, raw_bucket, "movilidad/bicimad")
    if bicimad_key:
        bicimad_df = download_dataframe_from_minio(raw_bucket, bicimad_key)
        
        # Limpiar y transformar datos
        processed_bicimad_df = process_bicimad_data(bicimad_df)
        
        # Guardar en formato parquet
        save_to_process_zone(processed_bicimad_df, "movilidad/bicimad_procesado.parquet", process_bucket)
    
    # 3. Procesar datos de parkings
    print("Procesando datos de parkings...")
    parkings_key = get_latest_file_key(s3_client, raw_bucket, "movilidad/parkings")
    if parkings_key:
        parkings_df = download_dataframe_from_minio(raw_bucket, parkings_key)
        
        # Limpiar y transformar datos
        processed_parkings_df = process_parking_data(parkings_df)
        
        # Guardar en formato parquet
        save_to_process_zone(processed_parkings_df, "movilidad/parkings_procesado.parquet", process_bucket)
    
    # 4. Procesar datos de avisos ciudadanos
    print("Procesando datos de avisos ciudadanos...")
    avisa_key = "participacion/avisamadrid.json"
    try:
        response = s3_client.get_object(Bucket=raw_bucket, Key=avisa_key)
        avisa_data = json.loads(response['Body'].read().decode('utf-8'))
        
        # Convertir a DataFrame y transformar
        avisa_df = pd.DataFrame(avisa_data)
        processed_avisa_df = process_avisamadrid_data(avisa_df)
        
        # Guardar en formato parquet
        save_to_process_zone(processed_avisa_df, "participacion/avisamadrid_procesado.parquet", process_bucket)
    except Exception as e:
        print(f"Error al procesar avisamadrid.json: {e}")
    
    # 5. Procesar datos municipales
    for dataset in ["consumo_energetico", "infraestructuras", "zonas_verdes"]:
        print(f"Procesando datos de {dataset}...")
        file_key = get_latest_file_key(s3_client, raw_bucket, f"municipal/{dataset}")
        if file_key:
            df = download_dataframe_from_minio(raw_bucket, file_key)
            
            # Procesar según el tipo de datos
            if dataset == "consumo_energetico":
                processed_df = process_energy_data(df)
            elif dataset == "infraestructuras":
                processed_df = process_infrastructure_data(df)
            else:  # zonas_verdes
                processed_df = process_green_areas_data(df)
            
            # Guardar en formato parquet
            save_to_process_zone(processed_df, f"municipal/{dataset}_procesado.parquet", process_bucket)
    
    print("Procesamiento de datos completado exitosamente.")

def get_latest_file_key(s3_client, bucket_name, prefix):
    """Obtiene la clave del archivo más reciente en un prefijo dado."""
    try:
        response = s3_client.list_objects_v2(
            Bucket=bucket_name,
            Prefix=prefix
        )
        
        if 'Contents' in response:
            # Ordenar por fecha de última modificación (más reciente primero)
            sorted_contents = sorted(
                response['Contents'], 
                key=lambda x: x['LastModified'], 
                reverse=True
            )
            
            if sorted_contents:
                return sorted_contents[0]['Key']
    except Exception as e:
        print(f"Error al obtener el archivo más reciente: {e}")
    
    return None

def save_to_process_zone(df, key, bucket_name):
    """Guarda un DataFrame en la zona de procesamiento."""
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
            operation="PROCESS",
            source=f"raw-ingestion-zone",
            target=f"s3://{bucket_name}/{key}",
            rows=len(df),
            status="SUCCESS"
        )
        
        print(f"Datos procesados guardados en s3://{bucket_name}/{key}")
    except Exception as e:
        print(f"Error al guardar datos procesados: {e}")
        log_operation(
            operation="PROCESS",
            source=f"raw-ingestion-zone",
            target=f"s3://{bucket_name}/{key}",
            rows=len(df) if isinstance(df, pd.DataFrame) else 0,
            status="ERROR",
            error=str(e)
        )

# Funciones de procesamiento específicas para cada tipo de dato

def process_traffic_data(df):
    """Limpia y transforma datos de tráfico."""
    # Limpiar, formatear fechas, eliminar duplicados, etc.
    # ...
    return df

def process_bicimad_data(df):
    """Limpia y transforma datos de BiciMAD."""
    # Implementar procesamiento específico
    # ...
    return df

def process_parking_data(df):
    """Limpia y transforma datos de parkings."""
    # Implementar procesamiento específico
    # ...
    return df

def process_avisamadrid_data(df):
    """Limpia y transforma datos de avisos ciudadanos."""
    # Implementar procesamiento específico
    # ...
    return df

def process_energy_data(df):
    """Limpia y transforma datos de consumo energético."""
    # Implementar procesamiento específico
    # ...
    return df

def process_infrastructure_data(df):
    """Limpia y transforma datos de infraestructuras urbanas."""
    # Implementar procesamiento específico
    # ...
    return df

def process_green_areas_data(df):
    """Limpia y transforma datos de zonas verdes."""
    # Implementar procesamiento específico
    # ...
    return df

if __name__ == "__main__":
    process_data()
