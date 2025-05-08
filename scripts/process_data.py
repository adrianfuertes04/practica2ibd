import pandas as pd
import numpy as np
from utils import (
    download_dataframe_from_minio,
    upload_dataframe_to_minio,
    log_data_transformation,
    validate_data_quality
)


df_trafico = pd.read_csv('../data/raw/trafico-horario.csv')
df_parkings = pd.read_csv('../data/raw/parkings-rotacion.csv')
df_bicis = pd.read_csv('../data/raw/bicimad-usos.csv')
df_avisa = pd.read_json("../data/raw/avisamadrid.json")

def standardize_data(df):
    """Standardize and clean transaction data."""
    # Make a copy to avoid modifying the original
    processed_df = df.copy()

    # Convert date string to datetime if needed
    if not pd.api.types.is_datetime64_any_dtype(processed_df['fecha_hora']):
        processed_df['fecha_hora'] = pd.to_datetime(processed_df['fecha_hora'])

    # Extract date components for analysis
    processed_df['year'] = processed_df['fecha_hora'].dt.year
    processed_df['month'] = processed_df['fecha_hora'].dt.month
    processed_df['day'] = processed_df['fecha_hora'].dt.day
    processed_df['day_of_week'] = processed_df['fecha_hora'].dt.dayofweek
    processed_df['hora'] = processed_df['fecha_hora'].dt.hour

    # Categorize velocidad_media_kmh into 'Baja', 'Media', 'Elevada'
    def categorize_velocidad(amount):
        if amount < 35:
            return 'Baja'
        elif amount < 60:
            return 'Media'
        else:
            return 'Elevada'
    processed_df['velocidad_media'] = processed_df['velocidad_media_kmh'].apply(categorize_velocidad)
    processed_df.drop(columns=['fecha_hora'], inplace=True)
    return processed_df

df_t = standardize_data(df_trafico)

def opt_data(df):
    """Enrich and standardize customer data."""
    # Make a copy to avoid modifying the original
    processed_df = df.copy()

    # Convert date string to datetime if needed
    if not pd.api.types.is_datetime64_any_dtype(processed_df['fecha']):
        processed_df['fecha'] = pd.to_datetime(processed_df['fecha'])



    # Group ocupacion into categories
    def ocupacion_group(ocupacion):
        if ocupacion < 5:
            return 'Vacio'
        elif ocupacion >= 5 and ocupacion < 25:
            return 'Semivacio'
        elif ocupacion >= 25 and ocupacion < 75:
            return 'Medio'
        elif ocupacion >= 75 and ocupacion < 95:
            return 'Semilleno'
        elif ocupacion >= 95:  
            return 'LLeno'
    processed_df['ocupacion'] = processed_df['porcentaje_ocupacion'].apply(ocupacion_group)



    return processed_df

df_p = opt_data(df_parkings)

def opt_data(df):
    """Enrich and standardize customer data."""
    # Make a copy to avoid modifying the original
    processed_df = df.copy()

    # Convert date string to datetime if needed
    if not pd.api.types.is_datetime64_any_dtype(processed_df['fecha']):
        processed_df['fecha'] = pd.to_datetime(processed_df['fecha'])



    # Group ocupacion into categories
    def ocupacion_group(ocupacion):
        if ocupacion < 5:
            return 'Vacio'
        elif ocupacion >= 5 and ocupacion < 25:
            return 'Semivacio'
        elif ocupacion >= 25 and ocupacion < 75:
            return 'Medio'
        elif ocupacion >= 75 and ocupacion < 95:
            return 'Semilleno'
        elif ocupacion >= 95:  
            return 'LLeno'
    processed_df['ocupacion'] = processed_df['porcentaje_ocupacion'].apply(ocupacion_group)



    return processed_df

df_p = opt_data(df_parkings)
def standardize_bicis(df):
    """Standardize and enrich product data."""
    # Make a copy to avoid modifying the original
    processed_df = df.copy()

    # Standardize categories
    if not pd.api.types.is_datetime64_any_dtype(processed_df['fecha_hora_inicio']):
        processed_df['fecha_hora_inicio'] = pd.to_datetime(processed_df['fecha_hora_inicio'])
    processed_df['hora'] = processed_df['fecha_hora_inicio'].dt.hour

    def duracion_group(days):
        if days < 1000:
            return 'Corta'
        elif days < 1300:
            return 'Promedio'
        else:
            return 'Larga'
    # Convert duration from seconds to a more readable format


    processed_df['duracion'] = processed_df['duracion_segundos'].apply(duracion_group)


    return processed_df
df_bicis = standardize_bicis(df_bicis)

def main():
    print("Starting data processing stage...")

    # 1. Download data from raw-ingestion-zone
    print("\nDownloading data from raw-ingestion-zone...")
    try:
        transactions_df = download_dataframe_from_minio('raw-ingestion-zone', 'sales/transactions.csv')
        customers_df = download_dataframe_from_minio('raw-ingestion-zone', 'crm/customers.csv')
        products_df = download_dataframe_from_minio('raw-ingestion-zone', 'inventory/products.csv')
        print(f"Downloaded {len(transactions_df)} transactions, {len(customers_df)} customers, {len(products_df)} products")
    except Exception as e:
        print(f"Error downloading data: {e}")
        return

    # 2. Process and transform the data
    print("\nTransforming data...")

    # Process transactions
    processed_transactions = standardize_transaction_data(transactions_df)
    print("Transaction data processed and standardized")

    # Validate transaction data quality
    transaction_rules = {
        'no_nulls': ['transaction_id', 'customer_id', 'product_id', 'amount', 'payment_method'],
        'unique': ['transaction_id']
    }
    validate_data_quality(processed_transactions, 'processed_transactions', transaction_rules)

    # Process customers
    processed_customers = enrich_customer_data(customers_df)
    print("Customer data processed and enriched")

    # Validate customer data quality
    customer_rules = {
        'no_nulls': ['customer_id', 'email'],
        'unique': ['customer_id', 'email']
    }
    validate_data_quality(processed_customers, 'processed_customers', customer_rules)

    # Process products
    processed_products = standardize_product_data(products_df)
    print("Product data processed and standardized")

    # Validate product data quality
    product_rules = {
        'no_nulls': ['product_id', 'product_name', 'category'],
        'unique': ['product_id']
    }
    validate_data_quality(processed_products, 'processed_products', product_rules)

    # Create transaction-product view
    transaction_product_view = create_transaction_product_view(processed_transactions, processed_products)
    print("Created transaction-product integrated view")

    # 3. Upload to process-zone in Parquet format (columnar storage for better performance)
    print("\nUploading processed data to process-zone...")

    # Upload processed transactions
    transaction_meta = {
        'description': 'Standardized transaction data with derived fields',
        'primary_keys': ['transaction_id'],
        'foreign_keys': ['customer_id', 'product_id'],
        'transformations': 'Added date components, standardized payment methods, added amount categories'
    }
    upload_dataframe_to_minio(
        processed_transactions,
        'process-zone',
        'sales/transactions.parquet',
        format='parquet',
        metadata=transaction_meta
    )
    log_data_transformation(
        'raw-ingestion-zone', 'sales/transactions.csv',
        'process-zone', 'sales/transactions.parquet',
        'Standardized transaction data and converted to Parquet format'
    )

    # Upload processed customers
    customer_meta = {
        'description': 'Enriched customer data with derived fields',
        'primary_keys': ['customer_id'],
        'transformations': 'Added tenure calculation, customer segments, standardized countries, added regions'
    }
    upload_dataframe_to_minio(
        processed_customers,
        'process-zone',
        'crm/customers.parquet',
        format='parquet',
        metadata=customer_meta
    )
    log_data_transformation(
        'raw-ingestion-zone', 'crm/customers.csv',
        'process-zone', 'crm/customers.parquet',
        'Enriched customer data and converted to Parquet format'
    )

    # Upload processed products
    product_meta = {
        'description': 'Standardized product data with derived fields',
        'primary_keys': ['product_id'],
        'transformations': 'Added price tiers, standardized categories, improved availability status'
    }
    upload_dataframe_to_minio(
        processed_products,
        'process-zone',
        'inventory/products.parquet',
        format='parquet',
        metadata=product_meta
    )
    log_data_transformation(
        'raw-ingestion-zone', 'inventory/products.csv',
        'process-zone', 'inventory/products.parquet',
        'Standardized product data and converted to Parquet format'
    )

    # Upload transaction-product view
    view_meta = {
        'description': 'Integrated view of transactions and products',
        'source_tables': ['transactions', 'products'],
        'join_keys': ['product_id'],
        'transformations': 'Joined transaction data with product information'
    }
    upload_dataframe_to_minio(
        transaction_product_view,
        'process-zone',
        'integrated/transaction_product_view.parquet',
        format='parquet',
        metadata=view_meta
    )
    log_data_transformation(
        'multiple', 'multiple',
        'process-zone', 'integrated/transaction_product_view.parquet',
        'Created integrated view joining transactions and products'
    )

    print("\nData processing complete!")
    print("Note: In the Process Zone, data has been cleaned, standardized, and enriched.")
    print("The data is now stored in Parquet format for better query performance.")
    print("Metadata and transformation logs have been recorded in the govern-zone.")

if __name__ == "__main__":
    main()