import boto3
import csv
import os
import time
import argparse
import logging
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
container_name = "ingesta4"

# Configuración de logs
log_dir = os.path.expanduser("~/logs")
os.makedirs(log_dir, exist_ok=True)
log_file = os.path.join(log_dir, f"{container_name}.log")

logging.basicConfig(
    filename=log_file,
    level=logging.INFO,
    format='%(asctime)s.%(msecs)03d [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# Iniciar log
logging.info(f"{container_name}: Inicio del proceso")

dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
s3 = boto3.client('s3', region_name='us-east-1')
glue = boto3.client('glue', region_name='us-east-1')

tabla_dynamo = f"{stage}-hotel-reservations"
archivo_csv = f"{stage}-reservations.csv"
glue_database = f"{stage}-glue-database"
glue_table_name = f"{stage}-reservations-table"


def exportar_dynamodb_a_csv(tabla_dynamo, archivo_csv):
    logging.info(f"{container_name}: Exportando datos desde DynamoDB ({tabla_dynamo})...")
    tabla = dynamodb.Table(tabla_dynamo)
    scan_kwargs = {}

    with open(archivo_csv, 'w', newline='') as archivo:
        escritor_csv = csv.writer(archivo)
        total_registros = 0

        while True:
            respuesta = tabla.scan(**scan_kwargs)
            items = respuesta['Items']

            if not items:
                break

            for item in items:
                try:
                    reservation_id = item.get('reservation_id', '')
                    service_ids = item.get('service_ids', [])
                    service_ids_str = ';'.join(service_ids) if isinstance(service_ids, list) else service_ids
                except ValueError:
                    reservation_id = ''
                    service_ids_str = ''

                row = [
                    item.get('tenant_id', ''),
                    reservation_id,
                    item.get('user_id', ''),
                    item.get('room_id', ''),
                    service_ids_str,
                    item.get('start_date', ''),
                    item.get('end_date', ''),
                    item.get('status', '')
                ]
                escritor_csv.writerow(row)
                total_registros += 1

            if 'LastEvaluatedKey' in respuesta:
                scan_kwargs['ExclusiveStartKey'] = respuesta['LastEvaluatedKey']
            else:
                break

    logging.info(f"{container_name}: Exportación completada, {total_registros} registros procesados.")
    return total_registros


def subir_csv_a_s3(archivo_csv, nombre_bucket):
    carpeta_destino = 'reservations/'
    archivo_s3 = f"{carpeta_destino}{archivo_csv}"
    logging.info(f"{container_name}: Subiendo {archivo_csv} al bucket S3 ({nombre_bucket}) en la carpeta 'reservations'...")

    try:
        s3.upload_file(archivo_csv, nombre_bucket, archivo_s3)
        logging.info(f"{container_name}: Archivo subido exitosamente a S3.")
        return True
    except Exception as e:
        logging.error(f"{container_name}: Error al subir el archivo a S3: {e}")
        return False


def crear_base_de_datos_en_glue(glue_database):
    try:
        glue.get_database(Name=glue_database)
        logging.info(f"{container_name}: La base de datos {glue_database} ya existe.")
    except glue.exceptions.EntityNotFoundException:
        logging.info(f"{container_name}: La base de datos {glue_database} no existe. Creando base de datos...")
        try:
            glue.create_database(
                DatabaseInput={
                    'Name': glue_database,
                    'Description': 'Base de datos para almacenamiento de reservas en Glue.'
                }
            )
            logging.info(f"{container_name}: Base de datos {glue_database} creada exitosamente.")
        except Exception as e:
            logging.error(f"{container_name}: Error al crear la base de datos Glue: {e}")
            return False
    return True


def registrar_datos_en_glue(glue_database, glue_table_name, nombre_bucket, archivo_csv):
    logging.info(f"{container_name}: Registrando datos en Glue Data Catalog...")
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
                        {'Name': 'service_ids', 'Type': 'string'},
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
                        'Parameters': {'field.delim': ','}
                    }
                },
                'TableType': 'EXTERNAL_TABLE',
                'Parameters': {'classification': 'csv'}
            }
        )
        logging.info(f"{container_name}: Tabla {glue_table_name} registrada exitosamente en Glue.")
    except Exception as e:
        logging.error(f"{container_name}: Error al registrar la tabla en Glue: {e}")


if __name__ == "__main__":
    start_time = datetime.now()
    if crear_base_de_datos_en_glue(glue_database):
        total_registros = exportar_dynamodb_a_csv(tabla_dynamo, archivo_csv)

        if subir_csv_a_s3(archivo_csv, nombre_bucket):
            registrar_datos_en_glue(glue_database, glue_table_name, nombre_bucket, archivo_csv)
        else:
            logging.error(f"{container_name}: Error al subir el archivo a S3.")
    else:
        logging.error(f"{container_name}: Error al crear la base de datos Glue.")

    end_time = datetime.now()
    logging.info(f"{container_name}: Proceso completado. Hora de inicio: {start_time}, Hora de fin: {end_time}")
