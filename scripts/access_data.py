from utils import (
    download_dataframe_from_minio,
    upload_dataframe_to_minio,
    log_data_transformation,
    execute_trino_query
)
import pandas as pd

def create_trafico_congestion_summary():
    """
    Crea un resumen por hora del nivel de congestión y vehículos predominantes.
    """
    print("Creando resumen de congestión de tráfico por hora...")

    # Descargar datos procesados
    df = download_dataframe_from_minio(
        'process-zone',
        'traf/trafico.parquet',
        format='parquet'
    )

    # Calcular promedio por hora y nivel de congestión
    resumen = df.groupby(['hour', 'congestion_level']).agg({
        'total_vehicles': 'sum',
        'cars': 'sum',
        'motorcycles': 'sum',
        'trucks': 'sum',
        'buses': 'sum',
        'avg_speed_kmh': 'mean'
    }).reset_index()

    # Calcular tipo de vehículo predominante
    resumen['vehiculo_predominante'] = resumen[['cars', 'motorcycles', 'trucks', 'buses']].idxmax(axis=1)


    return resumen


def rutes_users_popularity():
    df_bicimad = download_dataframe_from_minio(
        'process-zone',
        'data/bicimad.parquet',
        format='parquet'
    )

    grouped_df = df_bicimad.groupby(
    ['station_origin_id', 'station_dest_id', 'user_type']
        ).agg(
    total_viajes=('user_id', 'count'),
    avg_duration_seconds=('duration_seconds', 'mean'),
    avg_distance_km=('distance_km', 'mean'),
    total_users=('user_id', 'nunique')
        ).reset_index()

    return grouped_df

def clean_and_merge_parkings():
    # Descargar los datos parquet de MinIO usando la función proporcionada
    parkings = download_dataframe_from_minio('process-zone', 'invent/parkings.parquet', format='parquet')
    ubicaciones = download_dataframe_from_minio('process-zone', 'apar/aparcamientos.parquet', format='parquet')

    # Limpieza básica
    parkings = parkings.dropna()  # Eliminar filas con valores nulos
    ubicaciones = ubicaciones.dropna()

    # Normalizar nombres de columnas
    parkings.columns = parkings.columns.str.lower()
    ubicaciones.columns = ubicaciones.columns.str.lower()

    # Unir los dataframes por la columna común (asumiendo 'id_parking')
    merged = pd.merge(parkings, ubicaciones, on='parking_id', how='left')

    return merged

def main():
    print("Starting data preparation for the Access Zone...")

    # 1. Crear dataset de aparcamientos limpio y unido
    parkings_unidos = clean_and_merge_parkings()

    # 2. Subir el dataset unido a la zona access en formato parquet
    meta_parkings = {
        'description': 'Datos limpios y unidos de aparcamientos públicos con ubicación',
        'purpose': 'Visualización y análisis para ciudadanos',
        'refresh_frequency': 'Daily',
        'target_users': 'Ciudadanos y asociaciones vecinales',
    }

    upload_dataframe_to_minio(
        parkings_unidos,
        'access-zone',
        'analytics/parkings_unidos.parquet',
        format='parquet',
        metadata=meta_parkings
    )

    log_data_transformation(
        'process-zone', 'parkings_rotacion.parquet',
        'access-zone', 'analytics/parkings_unidos.parquet',
        'Datos limpios y unidos de aparcamientos públicos con ubicación'
    )

    print("Access Zone preparation complete!")

    print("Starting data preparation for the Access Zone...")

    # 1. Create analytics-ready datasets
    conegestion_hora = create_trafico_congestion_summary()
    rutas_users = rutes_users_popularity()

    # 2. Upload to access-zone
    print("\nUploading analytics-ready data to access-zone...")

    sales_meta = {
        'description': 'Congestion summary by hour',
        'purpose': 'Traffic analysis and reporting',
        'refresh_frequency': 'Daily',
        'target_users': 'Traffic analysts, city planners',
    }
    upload_dataframe_to_minio(
        conegestion_hora,
        'access-zone',
        'analytics/congestion_by_hour.parquet',
        format='parquet',
        metadata=sales_meta
    )
    log_data_transformation(
        'process-zone', 'traf/trafico.parquet',
        'access-zone', 'analytics/congestion_by_hour.parquet',
        'Congestion summary by hour',
    )

    # 3. Create additional datasets
    meta2 = {
        'description': 'Usos de rutas por tipo de usuario',
        'purpose': 'Traffic analysis and reporting',
        'refresh_frequency': 'Daily',
        'target_users': 'Traffic analysts, city planners',
    }
    upload_dataframe_to_minio(
        rutas_users,
        'access-zone',
        'analytics/rutas_users.parquet',
        format='parquet',
        metadata=meta2
    )
    log_data_transformation(
        'process-zone', 'data/bicimad.parquet',
        'access-zone', 'analytics/rutas_users.parquet',
        'Rutas populares por tipo de usuario',
    )




    print("\nAccess Zone preparation complete!")
    print("Note: The Access Zone now contains analytics-ready datasets optimized for:")
    print("  - Business Intelligence dashboards")
    print("  - Data analysis and visualization")
    print("  - Machine learning model development")
    print("  - Executive reporting")
    print("\nThese datasets are structured for easy consumption by various tools and users.")

if __name__ == "__main__":
    main()