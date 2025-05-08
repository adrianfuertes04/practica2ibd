FROM python:3.9

WORKDIR /app

# Instalar dependencias
RUN pip install pandas boto3 s3fs pyarrow fsspec sqlalchemy psycopg2-binary matplotlib seaborn plotly minio pytest

# Hacer que el contenedor siga ejecutándose
CMD ["tail", "-f", "/dev/null"]
