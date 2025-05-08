#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Script con ejemplos de consultas al data lake.
Muestra cómo los diferentes perfiles pueden acceder a los datos.
"""

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from sqlalchemy import create_engine
from utils import setup_s3_client, download_dataframe_from_minio

def query_with_pandas():
    """Demostración de acceso y análisis de datos con Pandas (para científicos de datos)."""
    print("Consultando la zona de acceso con Pandas...")
    
    # Cargar conjuntos de datos desde la zona de acceso
    print("Cargando conjuntos de datos desde access-zone...")
    
    # Análisis 1: Congestión de tráfico por hora y tipo de vehículo
    traffic_df = download_dataframe_from_minio("access-zone", "analytics/trafico_congestion.parquet")
    
    print("\nAnálisis de congestión de tráfico por hora:")
    print(traffic_df.groupby(['hora', 'tipo_vehiculo'])['num_vehiculos'].mean().reset_index())
    
    # Visualización
    plt.figure(figsize=(12, 6))
    sns.lineplot(data=traffic_df, x='hora', y='num_vehiculos', hue='tipo_vehiculo')
    plt.title('Congestión de tráfico por hora y tipo de vehículo')
    plt.xlabel('Hora del día')
    plt.ylabel('Número medio de vehículos')
    plt.tight_layout()
    plt.savefig('/app/data/traffic_congestion.png')
    print("Gráfico guardado en /app/data/traffic_congestion.png")
    
    # Análisis 2: Rutas de BiciMAD
    bicimad_df = download_dataframe_from_minio("access-zone", "analytics/bicimad_rutas.parquet")
    
    print("\nRutas más populares de BiciMAD:")
    print(bicimad_df.sort_values('num_viajes', ascending=False).head(10))
    
    # Análisis 3: Ocupación de parkings
    parking_df = download_dataframe_from_minio("access-zone", "analytics/parking_ocupacion.parquet")
    
    print("\nVaración de ocupación de parkings:")
    print(parking_df.groupby(['nombre_parking', 'dia_semana'])['porcentaje_ocupacion'].mean().reset_index())

def query_with_sql():
    """Demuestración de consultas SQL (para gestores con conocimientos SQL)."""
    print("\nConsultando con SQL a través de PostgreSQL...")
    
    # Configurar conexión a PostgreSQL
    engine = create_engine('postgresql://postgres:postgres@postgres:5432/madrid_sostenible')
    
    # Consulta 1: Rutas populares de BiciMAD
    print("\nRutas más populares de BiciMAD por SQL:")
    query = """
    SELECT 
        estacion_origen, 
        estacion_destino, 
        tipo_usuario,
        COUNT(*) as num_viajes,
        AVG(duracion_minutos) as duracion_media
    FROM 
        bicimad_rutas
    GROUP BY 
        estacion_origen, estacion_destino, tipo_usuario
    ORDER BY 
        num_viajes DESC
    LIMIT 10;
    """
    result = pd.read_sql(query, engine)
    print(result)
    
    # Consulta 2: Relación entre infraestructura de transporte y densidad de población
    print("\nRelación entre infraestructura y población:")
    query = """
    SELECT 
        distrito,
        densidad_poblacion,
        num_paradas_metro,
        num_paradas_bus,
        num_estaciones_bicimad,
        (num_paradas_metro + num_paradas_bus + num_estaciones_bicimad) as total_infraestructura,
        (num_paradas_metro + num_paradas_bus + num_estaciones_bicimad) / densidad_poblacion as infraestructura_per_capita
    FROM 
        infraestructuras_poblacion
    ORDER BY 
        infraestructura_per_capita DESC;
    """
    result = pd.read_sql(query, engine)
    print(result)

def visualize_parking_data():
    """Demostración de visualizaciones para usuarios no técnicos."""
    print("\nCreando visualizaciones para usuarios no técnicos...")
    
    # Cargar datos de parkings
    parking_df = download_dataframe_from_minio("access-zone", "analytics/parking_ocupacion.parquet")
    
    # Visualización 1: Ocupación media por día de la semana
    plt.figure(figsize=(10, 6))
    sns.barplot(data=parking_df, x='dia_semana', y='porcentaje_ocupacion')
    plt.title('Ocupación media de parkings por día de la semana')
    plt.xlabel('Día de la semana')
    plt.ylabel('Porcentaje de ocupación')
    plt.tight_layout()
    plt.savefig('/app/data/parking_occupancy_by_day.png')
    print("Gráfico guardado en /app/data/parking_occupancy_by_day.png")
    
    # Visualización 2: Variación de ocupación por hora
    plt.figure(figsize=(12, 6))
    sns.lineplot(data=parking_df, x='hora', y='porcentaje_ocupacion', hue='nombre_parking')
    plt.title('Variación de ocupación de parkings por hora')
    plt.xlabel('Hora del día')
    plt.ylabel('Porcentaje de ocupación')
    plt.tight_layout()
    plt.savefig('/app/data/parking_occupancy_by_hour.png')
    print("Gráfico guardado en /app/data/parking_occupancy_by_hour.png")
    
    # Visualización 3: Mapa de calor de ocupación por día y hora
    pivot_df = parking_df.pivot_table(
        index='dia_semana', 
        columns='hora', 
        values='porcentaje_ocupacion', 
        aggfunc='mean'
    )
    
    plt.figure(figsize=(14, 8))
    sns.heatmap(pivot_df, cmap='YlGnBu', annot=True, fmt=".1f")
    plt.title('Mapa de calor de ocupación de parkings por día y hora')
    plt.xlabel('Hora del día')
    plt.ylabel('Día de la semana')
    plt.tight_layout()
    plt.savefig('/app/data/parking_heatmap.png')
    print("Mapa de calor guardado en /app/data/parking_heatmap.png")

if __name__ == "__main__":
    print("Ejemplos de consulta y análisis de datos:")
    print("=========================================")
    
    # Ejemplos para científicos de datos (Python)
    query_with_pandas()
    
    # Ejemplos para gestores con conocimientos SQL
    query_with_sql()
    
    # Ejemplos para ciudadanos (visualizaciones)
    visualize_parking_data()
