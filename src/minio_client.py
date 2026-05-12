

import boto3
import botocore.config
from botocore.client import BaseClient
from botocore.exceptions import BotoCoreError, ClientError
from logging import Logger, FileHandler, Formatter, DEBUG
from datetime import datetime

logger = Logger("minio_client")
file_handler = FileHandler("logs.log")
file_handler.setLevel(DEBUG)
formatter = Formatter('%(asctime)s - %(levelname)s - %(message)s')
logger.addHandler(file_handler)

try: 
    client = boto3.client(
                    "s3",
                    endpoint_url="http://localhost:9000",
                    aws_access_key_id="minioadmin",
                    aws_secret_access_key="minioadmin",
                    config=botocore.config.Config(
                        signature_version="s3v4",
                        s3={"addressing_style": "path"},
                    ),
                )
    
    today_str = datetime.now().strftime('%Y-%m-%d')
    file_path = f"data/whales_alert_{today_str}.csv"
    client.upload_fileobj(open(file_path, "rb"), "test-bucket", file_path)
    
    logger.info(f"Archivo {file_path} subido correctamente")
except FileNotFoundError as e:
    logger.error(f"No se encontro el archivo {file_path}: {e}")
except ClientError as e:
    logger.error(f"Error al subir el archivo {file_path}: {e}")


