import pandas as pd
from utils import download_dataframe_from_minio, upload_dataframe_to_minio, log_data_transformation, validate_data_quality,download_file_from_minio
import json
def standardize_bicimad_usos(df):
    # Renombrado de columnas y tipos
    df = df.rename(columns={
        'id': 'id',
        'usuario_id': 'user_id',
        'tipo_usuario': 'user_type',
        'estacion_origen': 'station_origin_id',
        'estacion_destino': 'station_dest_id',
        'fecha_hora_inicio': 'start_time',
        'fecha_hora_fin': 'end_time',
        'duracion_segundos': 'duration_seconds',
        'distancia_km': 'distance_km',
        'calorias_estimadas': 'estimated_calories',
        'co2_evitado_gramos': 'co2_saved_grams'
    })
    # Normalización de tipo de usuario
    df['user_type'] = df['user_type'].str.lower().replace({'anual': 'annual', 'ocasional': 'occasional'})
    # Fechas a datetime
    df['start_time'] = pd.to_datetime(df['start_time'])
    df['end_time'] = pd.to_datetime(df['end_time'])
    # Derivar campos temporales
    df['year'] = df['start_time'].dt.year
    df['month'] = df['start_time'].dt.month
    df['day'] = df['start_time'].dt.day
    df['hour'] = df['start_time'].dt.hour
    df['weekday'] = df['start_time'].dt.weekday
    # Tipos
    int_cols = ['id', 'user_id', 'station_origin_id', 'station_dest_id', 'duration_seconds', 'estimated_calories', 'co2_saved_grams', 'year', 'month', 'day', 'hour', 'weekday']
    float_cols = ['distance_km']
    df[int_cols] = df[int_cols].astype('Int64')
    df[float_cols] = df[float_cols].astype(float)
    return df

def standardize_aparcamientos_info(df):
    df = df.rename(columns={
        'aparcamiento_id': 'parking_id',
        'nombre': 'name',
        'direccion': 'address',
        'capacidad_total': 'total_capacity',
        'plazas_movilidad_reducida': 'reduced_mobility_spaces',
        'plazas_vehiculos_electricos': 'ev_spaces',
        'tarifa_hora_euros': 'hourly_rate_eur',
        'horario': 'schedule',
        'latitud': 'latitude',
        'longitud': 'longitude'
    })
    df['hourly_rate_eur'] = df['hourly_rate_eur'].astype(float)
    df['latitude'] = df['latitude'].astype(float)
    df['longitude'] = df['longitude'].astype(float)
    df['total_capacity'] = df['total_capacity'].astype('Int64')
    df['reduced_mobility_spaces'] = df['reduced_mobility_spaces'].astype('Int64')
    df['ev_spaces'] = df['ev_spaces'].astype('Int64')
    # Normalizar horario
    df['schedule'] = df['schedule'].str.lower().replace({'24 horas': '24h'})
    return df

def standardize_parkings_rotacion(df):
    df = df.rename(columns={
        'aparcamiento_id': 'parking_id',
        'fecha': 'date',
        'hora': 'hour',
        'plazas_ocupadas': 'occupied_spaces',
        'plazas_libres': 'free_spaces',
        'porcentaje_ocupacion': 'occupancy_pct'
    })
    # Unir fecha y hora en timestamp
    df['timestamp'] = pd.to_datetime(df['date'] + ' ' + df['hour'].astype(str).str.zfill(2) + ':00:00')
    df['year'] = df['timestamp'].dt.year
    df['month'] = df['timestamp'].dt.month
    df['day'] = df['timestamp'].dt.day
    df['weekday'] = df['timestamp'].dt.weekday
    # Tipos
    df['parking_id'] = df['parking_id'].astype('Int64')
    df['occupied_spaces'] = df['occupied_spaces'].astype('Int64')
    df['free_spaces'] = df['free_spaces'].astype('Int64')
    df['occupancy_pct'] = df['occupancy_pct'].astype(float)
    df['hour'] = df['hour'].astype('Int64')
    return df[['parking_id', 'timestamp', 'occupied_spaces', 'free_spaces', 'occupancy_pct', 'year', 'month', 'day', 'hour', 'weekday']]

def standardize_trafico_horario(df):
    df = df.rename(columns={
        'sensor_id': 'sensor_id',
        'fecha_hora': 'timestamp',
        'total_vehiculos': 'total_vehicles',
        'coches': 'cars',
        'motos': 'motorcycles',
        'camiones': 'trucks',
        'buses': 'buses',
        'velocidad_media_kmh': 'avg_speed_kmh',
        'nivel_congestion': 'congestion_level'
    })
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    # Normalizar nivel de congestión
    df['congestion_level'] = df['congestion_level'].str.lower().replace({'baja': 'low', 'moderada': 'moderate', 'alta': 'high'})
    # Tipos
    int_cols = ['sensor_id', 'total_vehicles', 'cars', 'motorcycles', 'trucks', 'buses']
    df[int_cols] = df[int_cols].astype('Int64')
    df['avg_speed_kmh'] = df['avg_speed_kmh'].astype(float)
    # Derivar campos temporales
    df['year'] = df['timestamp'].dt.year
    df['month'] = df['timestamp'].dt.month
    df['day'] = df['timestamp'].dt.day
    df['hour'] = df['timestamp'].dt.hour
    df['weekday'] = df['timestamp'].dt.weekday
    return df

def standardize_avisa(df):
    """Enrich and standardize customer data."""
    # Make a copy to avoid modifying the original
    processed_df = df.copy()

    # Convert date string to datetime if needed
    if not pd.api.types.is_datetime64_any_dtype(processed_df['fecha_reporte']):
        processed_df['fecha_reporte'] = pd.to_datetime(processed_df['fecha_reporte'])
   


    return processed_df



def main():
    # Leer datos desde raw-ingestion-zone
    bicimad_df = download_dataframe_from_minio('raw-ingestion-zone', 'data/bicimad.csv', format='csv')
    aparcamientos_df = download_dataframe_from_minio('raw-ingestion-zone', 'apar/ext_aparcamientos_info.csv', format='csv')
    parkings_df = download_dataframe_from_minio('raw-ingestion-zone', 'invent/parkings-rotacion.csv', format='csv')
    trafico_df = download_dataframe_from_minio('raw-ingestion-zone', 'traf/trafcio-horario.csv', format='csv')
    download_file_from_minio('raw-ingestion-zone', 'avisos/avisamadrid.json', '/avisos/avisamadrid.json')
    # Estandarización

    bicimad_std = standardize_bicimad_usos(bicimad_df)
    aparcamientos_std = standardize_aparcamientos_info(aparcamientos_df)
    parkings_std = standardize_parkings_rotacion(parkings_df)
    trafico_std = standardize_trafico_horario(trafico_df)
    avisa_df = pd.read_json('/avisos/avisamadrid.json', encoding='utf-8')
    avisa_std = standardize_avisa(avisa_df)
    
    # Validación de calidad (puedes ajustar las reglas)
    validate_data_quality(avisa_std, 'avisa_process', rules={'no_nulls': ['id', 'fecha', 'tipo'], 'unique': ['id']})
    validate_data_quality(bicimad_std, 'bicimad_process', rules={'no_nulls': ['id', 'user_id', 'start_time'], 'unique': ['id']})
    validate_data_quality(aparcamientos_std, 'aparcamientos_process', rules={'no_nulls': ['parking_id', 'name'], 'unique': ['parking_id']})
    validate_data_quality(parkings_std, 'parkings_process', rules={'no_nulls': ['parking_id', 'timestamp'], 'unique': []})
    validate_data_quality(trafico_std, 'trafico_process', rules={'no_nulls': ['sensor_id', 'timestamp'], 'unique': []})
    
    # Subir a process-zone en formato parquet
    upload_dataframe_to_minio(bicimad_std, 'process-zone', 'data/bicimad.parquet', format='parquet')
    log_data_transformation('raw-ingestion-zone', 'data/bicimad.csv', 'process-zone', 'data/bicimad.parquet', 'Estandarización de BiciMAD usos y enriquecimiento temporal')
    
    upload_dataframe_to_minio(aparcamientos_std, 'process-zone', 'apar/aparcamientos.parquet', format='parquet')
    log_data_transformation('raw-ingestion-zone', 'apar/ext_aparcamientos_info.csv', 'process-zone', 'apar/aparcamientos.parquet', 'Estandarización de información de aparcamientos')
    
    upload_dataframe_to_minio(parkings_std, 'process-zone', 'invent/parkings.parquet', format='parquet')
    log_data_transformation('raw-ingestion-zone', 'invent/parkings-rotacion.csv', 'process-zone', 'invent/parkings.parquet', 'Estandarización y enriquecimiento temporal de parkings de rotación')
    
    upload_dataframe_to_minio(trafico_std, 'process-zone', 'traf/trafico.parquet', format='parquet')
    log_data_transformation('raw-ingestion-zone', 'traf/trafcio-horario.csv', 'process-zone', 'traf/trafico.parquet', 'Estandarización y enriquecimiento temporal de tráfico horario')
    
    upload_dataframe_to_minio(avisa_std, 'process-zone', 'avisa/avisos.parquet', format='parquet')
    log_data_transformation('raw-ingestion-zone', 'avisos/avisamadrid.json', 'process-zone', 'avisa/avisos.parquet', 'Estandarización y enriquecimiento de avisos del portal Avisa Madrid')

    print("Procesamiento y subida a process-zone completados.")

if __name__ == "__main__":
    main()

