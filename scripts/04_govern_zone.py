#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Script para la zona de gobernanza.
Gestiona metadatos, calidad de datos, y políticas de seguridad.
"""

import json
import boto3
import pandas as pd
import yaml
from datetime import datetime
from utils import setup_s3_client, log_operation, download_dataframe_from_minio

def setup_governance():
    """Configura la gobernanza de datos para el data lake."""
    print("Configurando gobernanza de datos...")
    
    # Configurar cliente S3 (MinIO)
    s3_client = setup_s3_client()
    metadata_bucket = "govern-zone-metadata"
    security_bucket = "govern-zone-security"
    access_bucket = "access-zone"
    
    # 1. Crear metadatos para conjuntos de datos analíticos
    print("Creando metadatos para conjuntos de datos...")
    
    # Metadatos para análisis de tráfico
    create_dataset_metadata(
        s3_client,
        metadata_bucket,
        {
            "dataset": "trafico_congestion",
            "description": "Análisis de congestión de tráfico por hora y tipo de vehículo",
            "purpose": "Identificar patrones de congestión para planificación urbana",
            "refresh_frequency": "Diaria",
            "target_users": "Científicos de datos, Planificadores urbanos",
            "uploaded_at": datetime.now().isoformat(),
            "format": "parquet, csv",
            "source_bucket": "access-zone",
            "object_name": "analytics/trafico_congestion.parquet"
        }
    )
    
    # Metadatos para análisis de BiciMAD
    create_dataset_metadata(
        s3_client,
        metadata_bucket,
        {
            "dataset": "bicimad_rutas",
            "description": "Análisis de rutas populares de BiciMAD",
            "purpose": "Optimizar ubicación de estaciones y disponibilidad de bicicletas",
            "refresh_frequency": "Diaria",
            "target_users": "Gestores de movilidad, Analistas SQL",
            "uploaded_at": datetime.now().isoformat(),
            "format": "parquet, sql",
            "source_bucket": "access-zone",
            "object_name": "analytics/bicimad_rutas.parquet"
        }
    )
    
    # Metadatos para análisis de parkings
    create_dataset_metadata(
        s3_client,
        metadata_bucket,
        {
            "dataset": "parking_ocupacion",
            "description": "Análisis de ocupación de parkings públicos",
            "purpose": "Informar a ciudadanos sobre disponibilidad y planificar expansiones",
            "refresh_frequency": "Diaria",
            "target_users": "Ciudadanos, Asociaciones vecinales",
            "uploaded_at": datetime.now().isoformat(),
            "format": "parquet, visualización",
            "source_bucket": "access-zone",
            "object_name": "analytics/parking_ocupacion.parquet"
        }
    )
    
    # 2. Crear registros de linaje de datos
    print("Creando registros de linaje de datos...")
    
    create_data_lineage(
        s3_client,
        metadata_bucket,
        {
            "timestamp": datetime.now().isoformat(),
            "source": {
                "bucket": "raw-ingestion-zone",
                "object": "movilidad/trafico"
            },
            "target": {
                "bucket": "process-zone",
                "object": "movilidad/trafico_procesado.parquet"
            },
            "transformation": "Limpieza, normalización de fechas, y agregación de datos de tráfico"
        }
    )
    
    # 3. Realizar verificaciones de calidad de datos
    print("Realizando verificaciones de calidad de datos...")
    
    try:
        # Verificar calidad de datos de tráfico
        traffic_df = download_dataframe_from_minio(access_bucket, "analytics/trafico_congestion.parquet")
        quality_checks = {
            "dataset": "trafico_congestion",
            "timestamp": datetime.now().isoformat(),
            "row_count": len(traffic_df),
            "checks": [
                {
                    "check": "no_nulls",
                    "column": "hora",
                    "passed": traffic_df["hora"].notnull().all(),
                    "details": "0 valores nulos encontrados" if traffic_df["hora"].notnull().all() else f"{traffic_df['hora'].isnull().sum()} valores nulos encontrados"
                },
                {
                    "check": "range_check",
                    "column": "num_vehiculos",
                    "passed": (traffic_df["num_vehiculos"] >= 0).all(),
                    "details": "Todos los valores son no negativos" if (traffic_df["num_vehiculos"] >= 0).all() else "Se encontraron valores negativos"
                }
                # Más verificaciones según sea necesario
            ]
        }
        
        # Guardar resultados de calidad
        s3_client.put_object(
            Bucket=metadata_bucket,
            Key=f"quality/trafico_congestion_{datetime.now().strftime('%Y%m%d%H%M%S')}.json",
            Body=json.dumps(quality_checks).encode()
        )
        
    except Exception as e:
        print(f"Error al verificar calidad de datos: {e}")
    
    # 4. Definir políticas de seguridad
    print("Definiendo políticas de seguridad...")
    
    security_policy = {
        "zones": {
            "raw-ingestion-zone": {
                "description": "Almacenamiento para datos sin procesar",
                "access_levels": {
                    "read": ["data_engineer", "data_scientist", "admin"],
                    "write": ["data_engineer", "admin", "system_integrator"],
                    "delete": ["admin"]
                },
                "encryption": "required",
                "retention_policy": "90 days"
            },
            "process-zone": {
                "description": "Almacenamiento para datos procesados y transformados",
                "access_levels": {
                    "read": ["data_engineer", "data_scientist", "data_analyst", "admin"],
                    "write": ["data_engineer", "admin"],
                    "delete": ["admin"]
                },
                "encryption": "required",
                "retention_policy": "180 days"
            },
            "access-zone": {
                "description": "Datos analíticos listos para consumo",
                "access_levels": {
                    "read": ["data_engineer", "data_scientist", "data_analyst", "business_user", "admin", "public_dashboards"],
                    "write": ["data_engineer", "admin"],
                    "delete": ["admin"]
                },
                "encryption": "required",
                "retention_policy": "365 days"
            }
        }
    }
    
    # Guardar política de seguridad
    s3_client.put_object(
        Bucket=security_bucket,
        Key="policies/datalake_security_policy.yaml",
        Body=yaml.dump(security_policy).encode()
    )
    
    print("Configuración de gobernanza completada exitosamente.")

def create_dataset_metadata(s3_client, bucket, metadata):
    """Crea y almacena metadatos para un conjunto de datos."""
    try:
        s3_client.put_object(
            Bucket=bucket,
            Key=f"metadata/{metadata['dataset']}.json",
            Body=json.dumps(metadata).encode()
        )
        
        print(f"Metadatos creados para {metadata['dataset']}")
    except Exception as e:
        print(f"Error al crear metadatos: {e}")

def create_data_lineage(s3_client, bucket, lineage):
    """Crea y almacena información de linaje de datos."""
    try:
        source_obj = lineage["source"]["object"].replace("/", "_")
        target_obj = lineage["target"]["object"].replace("/", "_")
        
        s3_client.put_object(
            Bucket=bucket,
            Key=f"lineage/{source_obj}_to_{target_obj}.json",
            Body=json.dumps(lineage).encode()
        )
        
        print(f"Linaje creado para {source_obj} -> {target_obj}")
    except Exception as e:
        print(f"Error al crear linaje de datos: {e}")

if __name__ == "__main__":
    setup_governance()
