import pandas as pd
import os
from utils import upload_dataframe_to_minio, get_minio_client, upload_json_to_minio, upload_file_to_minio
import json
def main():

    # Save sample data to local CSV files
    bicimad_df = pd.read_csv('/data/raw/bicimad-usos.csv')
    trafico_df = pd.read_csv('/data/raw/trafico-horario.csv')
    parkings_df = pd.read_csv('/data/raw/parkings-rotacion.csv')

    aparcamiento_df = pd.read_csv('/data/raw/ext_aparcamientos_info.csv')

    upload_dataframe_to_minio(
        bicimad_df,
        'raw-ingestion-zone',
        'data/bicimad.csv'
    )

    upload_dataframe_to_minio(
        trafico_df,
        'raw-ingestion-zone',
        'traf/trafcio-horario.csv',
    )

    upload_dataframe_to_minio(
        parkings_df,
        'raw-ingestion-zone',
        'invent/parkings-rotacion.csv',
    )

    upload_dataframe_to_minio(
        aparcamiento_df,
        'raw-ingestion-zone',
        'apar/ext_aparcamientos_info.csv',
    )

    with open('../data/raw/avisamadrid.json', 'r', encoding='utf-8') as f:
        avisos_data = f.read()

    upload_json_to_minio(
    avisos_data,
    'raw-ingestion-zone',
    'avisos/avisamadrid.json'
    )

    upload_file_to_minio('/data/raw/dump-bbdd-municipal.sql', 'raw-ingestion-zone', 'db/avisos.sql')

    # Verify that the files were uploaded
    client = get_minio_client()
    print("\nVerifying uploaded files in raw-ingestion-zone:")

    for prefix in ['data/', 'traf/', 'invent/', 'apar/', 'avisos/', 'db/']:
        objects = list(client.list_objects('raw-ingestion-zone', prefix=prefix))
        if objects:
            print(f"Files in {prefix}: {[obj.object_name for obj in objects]}")
        else:
            print(f"No objects found in {prefix}")

    print("\nData ingestion into raw-ingestion-zone complete!")
    print("Note: The data in this zone is stored in its original format without modifications.")

if __name__ == "__main__":
    main()