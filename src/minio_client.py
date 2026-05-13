import os
import boto3
import botocore.config
from botocore.exceptions import BotoCoreError, ClientError
from logging import Logger, FileHandler, Formatter, DEBUG
from datetime import datetime
from dotenv import load_dotenv

# Cargar variables de entorno desde .env
load_dotenv()

logger = Logger("minio_client")
file_handler = FileHandler("logs.log")
file_handler.setLevel(DEBUG)
formatter = Formatter('%(asctime)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)


def get_minio_client() -> boto3.client:
    """Crea y retorna un cliente S3 apuntando a MinIO."""
    endpoint = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
    access_key = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
    secret_key = os.getenv("MINIO_SECRET_KEY", "minioadmin")

    client = boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        config=botocore.config.Config(
            signature_version="s3v4",
            s3={"addressing_style": "path"},
        ),
    )
    logger.info(f"Cliente MinIO creado para endpoint: {endpoint}")
    return client


def ensure_bucket_exists(client: boto3.client, bucket_name: str) -> None:
    """Crea el bucket si no existe."""
    try:
        client.head_bucket(Bucket=bucket_name)
        logger.info(f"Bucket '{bucket_name}' ya existe.")
    except ClientError as e:
        error_code = e.response["Error"]["Code"]
        if error_code in ("404", "NoSuchBucket"):
            client.create_bucket(Bucket=bucket_name)
            logger.info(f"Bucket '{bucket_name}' creado exitosamente.")
        else:
            logger.error(f"Error verificando bucket '{bucket_name}': {e}")
            raise


def upload_file_to_minio(file_path: str) -> None:
    """Sube un archivo CSV a MinIO."""
    bucket_name = os.getenv("MINIO_BUCKET_NAME", "whale-alert")

    try:
        client = get_minio_client()
        ensure_bucket_exists(client, bucket_name)

        with open(file_path, "rb") as f:
            client.upload_fileobj(f, bucket_name, file_path)

        logger.info(f"Archivo '{file_path}' subido correctamente al bucket '{bucket_name}'.")
        print(f"[OK] Archivo '{file_path}' subido a MinIO bucket '{bucket_name}'.")

    except FileNotFoundError as e:
        logger.error(f"No se encontró el archivo '{file_path}': {e}")
        print(f"[ERROR] Archivo no encontrado: {file_path}")
    except (BotoCoreError, ClientError) as e:
        logger.error(f"Error al subir el archivo '{file_path}': {e}")
        print(f"[ERROR] MinIO: {e}")


if __name__ == "__main__":
    file_path = f"data/whales_{datetime.now().strftime('%Y-%m-%d')}.csv"
    upload_file_to_minio(file_path)