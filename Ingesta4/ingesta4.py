import boto3
import csv
import os
import time
import argparse
from loguru import logger
from datetime import datetime

# Configuración de argparse para obtener parámetros
parser = argparse.ArgumentParser(description='Script para ejecutar la ingesta de datos')

# Parámetros de entrada
parser.add_argument('--stage', required=True, help="Indica el stage (por ejemplo, dev, prod)")
parser.add_argument('--bucket', required=True, help="Indica el nombre del bucket S3")

# Parsear los argumentos
args = parser.parse_args()

# Usamos los valores de los argumentos
stage = args.stage
nombre_bucket = args.bucket

# Configuración del Logger
LOG_DIRECTORY = "/logs"
CONTAINER_NAME = "Ingesta4"

# Asegurarse de que el directorio de logs existe
os.makedirs(LOG_DIRECTORY, exist_ok=True)

LOG_FILE = os.path.join(LOG_DIRECTORY, f"{CONTAINER_NAME}.log")

# Configurar el formato de log
logger.remove()  # Eliminar cualquier configuración previa
logger.add(
    LOG_FILE,
    format="{time:YYYY-MM-DD HH:mm:ss.SSS} | {level} | {extra[container]} | {message}",
    level="INFO",
    enqueue=True,
    backtrace=True,
    diagnose=True,
)

# Agregar información adicional (nombre del contenedor)
logger = logger.bind(container=CONTAINER_NAME)

# Inicializar los clientes de AWS
dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
s3 = boto3.client('s3', region_name='us-east-1')
glue = boto3.client('glue', region_name='us-east-1')

tabla_dynamo = f"{stage}-hotel-reservations"  # Tabla de reservas
archivo_csv = f"{stage}-reservations.csv"    # Archivo CSV para reservas
glue_database = f"{stage}-glue-database"    # Base de datos de Glue
glue_table_name = f"{stage}-reservations-table"  # Tabla de Glue

def exportar_dynamodb_a_csv(tabla_dynamo, archivo_csv):
    logger.info(f"Exportando datos desde DynamoDB ({tabla_dynamo})...")
    tabla = dynamodb.Table(tabla_dynamo)
    scan_kwargs = {}

    try:
        with open(archivo_csv, 'w', newline='') as archivo:
            escritor_csv = csv.writer(archivo)

            while True:
                try:
                    respuesta = tabla.scan(**scan_kwargs)
                    items = respuesta.get('Items', [])
                except Exception as e:
                    logger.error(f"Error al escanear la tabla DynamoDB: {e}")
                    break

                if not items:
                    logger.info("No se encontraron más elementos en la tabla DynamoDB.")
                    break

                for item in items:
                    try:
                        reservation_id = item.get('reservation_id', '')
                        service_ids = item.get('service_ids', [])
                        # Convertir la lista de 'service_ids' a una cadena separada por ';' para evitar problemas con comas
                        service_ids_str = ';'.join(service_ids) if isinstance(service_ids, list) else service_ids
                    except ValueError:
                        reservation_id = ''
                        service_ids_str = ''

                    row = [
                        item.get('tenant_id', ''),
                        reservation_id,
                        item.get('user_id', ''),
                        item.get('room_id', ''),
                        service_ids_str,  # Usar ';' como delimitador para service_ids
                        item.get('start_date', ''),
                        item.get('end_date', ''),
                        item.get('status', '')
                    ]

                    escritor_csv.writerow(row)
                    logger.debug(f"Escribiendo fila: {row}")

                if 'LastEvaluatedKey' in respuesta:
                    scan_kwargs['ExclusiveStartKey'] = respuesta['LastEvaluatedKey']
                    logger.info("Continuando con la siguiente página de resultados en DynamoDB.")
                else:
                    logger.info("Finalizando el escaneo de la tabla DynamoDB.")
                    break

        logger.info(f"Datos exportados a {archivo_csv}")
    except Exception as e:
        logger.error(f"Error al exportar datos a CSV: {e}")

def subir_csv_a_s3(archivo_csv, nombre_bucket):
    carpeta_destino = 'reservations/'
    archivo_s3 = f"{carpeta_destino}{archivo_csv}"
    logger.info(f"Subiendo {archivo_csv} al bucket S3 ({nombre_bucket}) en la carpeta 'reservations'...")

    try:
        s3.upload_file(archivo_csv, nombre_bucket, archivo_s3)
        logger.info(f"Archivo subido exitosamente a S3 en la carpeta 'reservations'.")
        return True
    except Exception as e:
        logger.error(f"Error al subir el archivo a S3: {e}")
        return False

def crear_base_de_datos_en_glue(glue_database):
    """Crear base de datos en Glue si no existe."""
    try:
        glue.get_database(Name=glue_database)
        logger.info(f"La base de datos {glue_database} ya existe.")
    except glue.exceptions.EntityNotFoundException:
        logger.warning(f"La base de datos {glue_database} no existe. Creando base de datos...")
        try:
            glue.create_database(
                DatabaseInput={
                    'Name': glue_database,
                    'Description': 'Base de datos para almacenamiento de reservas en Glue.'
                }
            )
            logger.info(f"Base de datos {glue_database} creada exitosamente.")
        except Exception as e:
            logger.error(f"Error al crear la base de datos en Glue: {e}")
            return False
    except Exception as e:
        logger.error(f"Error al verificar o crear la base de datos en Glue: {e}")
        return False
    return True

def registrar_datos_en_glue(glue_database, glue_table_name, nombre_bucket, archivo_csv):
    """Registrar datos en Glue Data Catalog."""
    logger.info(f"Registrando datos en Glue Data Catalog...")
    input_path = f"s3://{nombre_bucket}/reservations/"

    try:
        glue.create_table(
            DatabaseName=glue_database,
            TableInput={
                'Name': glue_table_name,
                'StorageDescriptor': {
                    'Columns': [
                        {'Name': 'tenant_id', 'Type': 'string'},
                        {'Name': 'reservation_id', 'Type': 'string'},
                        {'Name': 'user_id', 'Type': 'string'},
                        {'Name': 'room_id', 'Type': 'string'},
                        {'Name': 'service_ids', 'Type': 'string'},  # Cambio: 'service_ids' es una cadena de IDs
                        {'Name': 'start_date', 'Type': 'string'},
                        {'Name': 'end_date', 'Type': 'string'},
                        {'Name': 'status', 'Type': 'string'}
                    ],
                    'Location': input_path,
                    'InputFormat': 'org.apache.hadoop.mapred.TextInputFormat',
                    'OutputFormat': 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat',
                    'Compressed': False,
                    'SerdeInfo': {
                        'SerializationLibrary': 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe',
                        'Parameters': {'field.delim': ','}  # Asegúrate de que el delimitador es ',' para todo
                    }
                },
                'TableType': 'EXTERNAL_TABLE',
                'Parameters': {'classification': 'csv'}
            }
        )
        logger.info(f"Tabla {glue_table_name} registrada exitosamente en la base de datos {glue_database}.")
    except glue.exceptions.AlreadyExistsException:
        logger.warning(f"La tabla {glue_table_name} ya existe en la base de datos {glue_database}.")
    except Exception as e:
        logger.error(f"Error al registrar la tabla en Glue: {e}")

if __name__ == "__main__":
    logger.info("Inicio del proceso de ingesta.")
    if crear_base_de_datos_en_glue(glue_database):
        exportar_dynamodb_a_csv(tabla_dynamo, archivo_csv)

        if subir_csv_a_s3(archivo_csv, nombre_bucket):
            registrar_datos_en_glue(glue_database, glue_table_name, nombre_bucket, archivo_csv)
        else:
            logger.error("No se pudo completar el proceso porque hubo un error al subir el archivo a S3.")
    else:
        logger.error("Error en la creación de la base de datos Glue. No se continuará con el proceso.")

    logger.info("Fin del proceso de ingesta.")
    print("Proceso completado.")
