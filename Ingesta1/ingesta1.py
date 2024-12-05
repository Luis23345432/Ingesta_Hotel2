import boto3
import csv
import os
import time
import argparse
import logging
from datetime import datetime

# Configure logging
LOG_DIR = "/app/logs"
CONTAINER_NAME = "ingesta1"
if not os.path.exists(LOG_DIR):
    os.makedirs(LOG_DIR)
log_file = os.path.join(LOG_DIR, f"{CONTAINER_NAME}.log")

logging.basicConfig(
    filename=log_file,
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(name)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S.%f"
)
logger = logging.getLogger(CONTAINER_NAME)

# Configuración de argparse para obtener parámetros
parser = argparse.ArgumentParser(description="Script para ejecutar la ingesta de datos")

# Parámetros de entrada
parser.add_argument("--stage", required=True, help="Indica el stage (por ejemplo, dev, prod)")
parser.add_argument("--bucket", required=True, help="Indica el nombre del bucket S3")

# Parsear los argumentos
args = parser.parse_args()

# Usamos los valores de los argumentos
stage = args.stage
nombre_bucket = args.bucket

dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
s3 = boto3.client("s3", region_name="us-east-1")
glue = boto3.client("glue", region_name="us-east-1")

tabla_dynamo = f"{stage}-hotel-users"  # Tabla de usuarios
archivo_csv = f"{stage}-usuarios.csv"
glue_database = f"{stage}-glue-database"  # Base de datos de Glue
glue_table_name = f"{stage}-usuarios-table"  # Tabla de Glue


def exportar_dynamodb_a_csv(tabla_dynamo, archivo_csv):
    logger.info(f"Exportando datos desde DynamoDB ({tabla_dynamo})...")
    tabla = dynamodb.Table(tabla_dynamo)
    scan_kwargs = {}
    registros_procesados = 0

    try:
        with open(archivo_csv, "w", newline="") as archivo:
            escritor_csv = csv.writer(archivo)

            while True:
                respuesta = tabla.scan(**scan_kwargs)
                items = respuesta.get("Items", [])
                for item in items:
                    registros_procesados += 1
                    row = [
                        item.get("tenant_id", ""),
                        item.get("user_id", ""),
                        item.get("nombre", ""),
                        item.get("email", ""),
                        item.get("password_hash", ""),
                        item.get("fecha_registro", ""),
                    ]
                    escritor_csv.writerow(row)

                if "LastEvaluatedKey" in respuesta:
                    scan_kwargs["ExclusiveStartKey"] = respuesta["LastEvaluatedKey"]
                else:
                    break

        logger.info(f"Datos exportados a {archivo_csv}. Total registros procesados: {registros_procesados}")
    except Exception as e:
        logger.error(f"Error al exportar datos desde DynamoDB: {e}")
        raise


def subir_csv_a_s3(archivo_csv, nombre_bucket):
    carpeta_destino = "usuarios/"
    archivo_s3 = f"{carpeta_destino}{archivo_csv}"
    logger.info(f"Subiendo {archivo_csv} al bucket S3 ({nombre_bucket}) en la carpeta 'usuarios'...")

    try:
        s3.upload_file(archivo_csv, nombre_bucket, archivo_s3)
        logger.info(f"Archivo subido exitosamente a S3: {archivo_s3}")
        return True
    except Exception as e:
        logger.error(f"Error al subir el archivo a S3: {e}")
        return False


def crear_base_de_datos_en_glue(glue_database):
    logger.info(f"Verificando la existencia de la base de datos Glue ({glue_database})...")
    try:
        glue.get_database(Name=glue_database)
        logger.info(f"La base de datos {glue_database} ya existe.")
    except glue.exceptions.EntityNotFoundException:
        logger.info(f"La base de datos {glue_database} no existe. Creando base de datos...")
        try:
            glue.create_database(
                DatabaseInput={
                    "Name": glue_database,
                    "Description": "Base de datos para almacenamiento de usuarios en Glue.",
                }
            )
            logger.info(f"Base de datos {glue_database} creada exitosamente.")
        except Exception as e:
            logger.error(f"Error al crear la base de datos en Glue: {e}")
            return False
    except Exception as e:
        logger.error(f"Error al verificar la base de datos en Glue: {e}")
        return False
    return True


def registrar_datos_en_glue(glue_database, glue_table_name, nombre_bucket, archivo_csv):
    logger.info(f"Registrando datos en Glue Data Catalog...")
    input_path = f"s3://{nombre_bucket}/usuarios/"

    try:
        glue.create_table(
            DatabaseName=glue_database,
            TableInput={
                "Name": glue_table_name,
                "StorageDescriptor": {
                    "Columns": [
                        {"Name": "tenant_id", "Type": "string"},
                        {"Name": "user_id", "Type": "string"},
                        {"Name": "nombre", "Type": "string"},
                        {"Name": "email", "Type": "string"},
                        {"Name": "password_hash", "Type": "string"},
                        {"Name": "fecha_registro", "Type": "timestamp"},
                    ],
                    "Location": input_path,
                    "InputFormat": "org.apache.hadoop.mapred.TextInputFormat",
                    "OutputFormat": "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
                    "Compressed": False,
                    "SerdeInfo": {
                        "SerializationLibrary": "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe",
                        "Parameters": {"field.delim": ","},
                    },
                },
                "TableType": "EXTERNAL_TABLE",
                "Parameters": {"classification": "csv"},
            },
        )
        logger.info(f"Tabla {glue_table_name} registrada exitosamente en Glue.")
    except Exception as e:
        logger.error(f"Error al registrar datos en Glue: {e}")


if __name__ == "__main__":
    inicio_proceso = datetime.now()
    logger.info("Inicio del proceso de ingesta.")
    try:
        if crear_base_de_datos_en_glue(glue_database):
            exportar_dynamodb_a_csv(tabla_dynamo, archivo_csv)
            if subir_csv_a_s3(archivo_csv, nombre_bucket):
                registrar_datos_en_glue(glue_database, glue_table_name, nombre_bucket, archivo_csv)
            else:
                logger.error("No se pudo completar el proceso: Error al subir archivo a S3.")
        else:
            logger.error("Error en la creación/verificación de la base de datos Glue.")
    except Exception as e:
        logger.critical(f"Proceso detenido debido a un error crítico: {e}")
    finally:
        fin_proceso = datetime.now()
        duracion = (fin_proceso - inicio_proceso).total_seconds()
        logger.info(f"Fin del proceso de ingesta. Duración: {duracion} segundos.")
