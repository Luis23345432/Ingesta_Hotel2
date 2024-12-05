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

# Configurar logging
LOG_DIR = "/app/logs"
os.makedirs(LOG_DIR, exist_ok=True)
LOG_FILE = os.path.join(LOG_DIR, "ingesta5.log")

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()
    ]
)

dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
s3 = boto3.client('s3', region_name='us-east-1')
glue = boto3.client('glue', region_name='us-east-1')

tabla_dynamo = f"{stage}-hotel-comments"  # Tabla de comentarios
archivo_csv = f"{stage}-comments.csv"    # Archivo CSV para comentarios
glue_database = f"{stage}-glue-database" # Base de datos de Glue
glue_table_name = f"{stage}-comments-table"  # Tabla de Glue


def exportar_dynamodb_a_csv(tabla_dynamo, archivo_csv):
    logging.info(f"Inicio de exportación de DynamoDB ({tabla_dynamo})")
    tabla = dynamodb.Table(tabla_dynamo)
    scan_kwargs = {}

    registros_procesados = 0

    with open(archivo_csv, 'w', newline='') as archivo:
        escritor_csv = csv.writer(archivo)

        while True:
            respuesta = tabla.scan(**scan_kwargs)
            items = respuesta['Items']

            if not items:
                break

            for item in items:
                try:
                    comment_id = item.get('comment_id', '')
                    created_at = item.get('created_at', '')
                    comment_text = item.get('comment_text', '').replace('\n', ' ').replace('\r', '')
                except ValueError:
                    comment_id, created_at, comment_text = '', '', ''

                row = [
                    item.get('tenant_id', ''),
                    comment_id,
                    item.get('room_id', ''),
                    item.get('user_id', ''),
                    comment_text,
                    created_at
                ]
                escritor_csv.writerow(row)
                registros_procesados += 1

            if 'LastEvaluatedKey' in respuesta:
                scan_kwargs['ExclusiveStartKey'] = respuesta['LastEvaluatedKey']
            else:
                break

    logging.info(f"Exportación completada con {registros_procesados} registros procesados")
    return registros_procesados


def subir_csv_a_s3(archivo_csv, nombre_bucket):
    carpeta_destino = 'comments/'
    archivo_s3 = f"{carpeta_destino}{archivo_csv}"
    logging.info(f"Subiendo {archivo_csv} al bucket S3 ({nombre_bucket}) en {archivo_s3}")

    try:
        s3.upload_file(archivo_csv, nombre_bucket, archivo_s3)
        logging.info(f"Archivo subido exitosamente a {archivo_s3}")
        return True
    except Exception as e:
        logging.error(f"Error al subir el archivo a S3: {e}")
        return False


def crear_base_de_datos_en_glue(glue_database):
    logging.info(f"Verificando o creando base de datos Glue: {glue_database}")
    try:
        glue.get_database(Name=glue_database)
        logging.info(f"La base de datos {glue_database} ya existe.")
    except glue.exceptions.EntityNotFoundException:
        glue.create_database(
            DatabaseInput={
                'Name': glue_database,
                'Description': 'Base de datos para almacenamiento de comentarios en Glue.'
            }
        )
        logging.info(f"Base de datos {glue_database} creada exitosamente.")
    except Exception as e:
        logging.error(f"Error al crear/verificar la base de datos Glue: {e}")
        return False
    return True


def registrar_datos_en_glue(glue_database, glue_table_name, nombre_bucket, archivo_csv):
    input_path = f"s3://{nombre_bucket}/comments/"
    logging.info(f"Registrando datos en Glue Data Catalog en la tabla {glue_table_name} en {input_path}")

    try:
        glue.create_table(
            DatabaseName=glue_database,
            TableInput={
                'Name': glue_table_name,
                'StorageDescriptor': {
                    'Columns': [
                        {'Name': 'tenant_id', 'Type': 'string'},
                        {'Name': 'comment_id', 'Type': 'string'},
                        {'Name': 'room_id', 'Type': 'string'},
                        {'Name': 'user_id', 'Type': 'string'},
                        {'Name': 'comment_text', 'Type': 'string'},
                        {'Name': 'created_at', 'Type': 'timestamp'}
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
        logging.info(f"Tabla {glue_table_name} registrada exitosamente.")
    except Exception as e:
        logging.error(f"Error al registrar la tabla en Glue: {e}")


if __name__ == "__main__":
    start_time = datetime.now()
    logging.info("Iniciando proceso de ingesta")

    if crear_base_de_datos_en_glue(glue_database):
        registros_procesados = exportar_dynamodb_a_csv(tabla_dynamo, archivo_csv)

        if subir_csv_a_s3(archivo_csv, nombre_bucket):
            registrar_datos_en_glue(glue_database, glue_table_name, nombre_bucket, archivo_csv)
        else:
            logging.error("Proceso finalizado con errores al subir archivo a S3.")
    else:
        logging.error("Error al crear/verificar la base de datos Glue.")

    end_time = datetime.now()
    logging.info(f"Proceso completado. Tiempo total: {end_time - start_time}")
