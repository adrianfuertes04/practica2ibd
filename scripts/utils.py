#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Funciones de utilidad compartidas por los diferentes scripts.
"""

import boto3
import pandas as pd
import io
from datetime import datetime
import logging

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def setup_s3_client():
    """Configura y devuelve un cliente S3 conectado a MinIO."""
    s3_client = boto3.client(
        's3',
        endpoint_url='http://minio:9000',
        aws_access_key_id='minioadmin',
        aws_secret_access_key='minioadmin',
        region_name='us-east-1',  # Valor ficticio, MinIO no usa regiones
        use_ssl=False
    )
    return s3_client

def log_operation(operation, source, target, rows, status, error=None):
    """Registra una operación de datos en el log."""
    log_entry = {
        "timestamp": datetime.now().isoformat(),
        "operation": operation,
        "source": source,
        "target": target,
        "rows_affected": rows,
        "status": status
    }
    
    if error:
        log_entry["error"] = error
    
    logger.info(f"Data Operation: {log_entry}")

def download_dataframe_from_minio(bucket, key, format=None):
    """Descarga un archivo desde MinIO y lo convierte en DataFrame."""
    try:
        # Determinar formato basado en la extensión del archivo
        if format is None:
            if key.endswith('.parquet'):
                format = 'parquet'
            elif key.endswith('.csv'):
                format = 'csv'
            else:
                format = 'parquet'  # Default a parquet
        
        # Configurar cliente S3
        s3_client = setup_s3_client()
        
        # Obtener objeto
        response = s3_client.get_object(Bucket=bucket, Key=key)
        
        # Leer en un DataFrame según el formato
        if format == 'parquet':
            return pd.read_parquet(io.BytesIO(response['Body'].read()))
        elif format == 'csv':
            return pd.read_csv(io.BytesIO(response['Body'].read()))
        else:
            raise ValueError(f"Formato no soportado: {format}")
    except Exception as e:
        logger.error(f"Error al descargar el DataFrame desde MinIO: {e}")
        raise
