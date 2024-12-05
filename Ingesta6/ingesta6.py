import boto3
import csv
import os
import time
import argparse
import logging
from datetime import datetime

# Configuración del logger
log_dir = "/app/logs"
os.makedirs(log_dir, exist_ok=True)
log_file = os.path.join(log_dir, "ingesta6.log")

logging.basicConfig(
    filename=log_file,
    level=logging.INFO,
    format="%(asctime)s.%(msecs)03d %(levelname)s [ingesta6] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

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

tabla_dynamo = f"{stage}-hotel-payments"  # Tabla de pagos
archivo_csv = f"{stage}-payments.csv"  # Archivo CSV para pagos
glue_database = f"{stage}-glue-database"  # Base de datos de Glue
glue_table_name = f"{stage}-payments-table"  # Tabla de Glue


def log_info(message):
    """Log a message with INFO level."""
    logging.info(message)


def log_error(message):
    """Log a message with ERROR level."""
    logging.error(message)


def exportar_dynamodb_a_csv(tabla_dynamo, archivo_csv):
    log_info(f"Inicio de exportación desde DynamoDB: {tabla_dynamo}")
    tabla = dynamodb.Table(tabla_dynamo)
    scan_kwargs = {}

    registros_procesados = 0

    try:
        with open(archivo_csv, "w", newline="") as archivo:
            escritor_csv = csv.writer(archivo)
            while True:
                respuesta = tabla.scan(**scan_kwargs)
                items = respuesta.get("Items", [])
                registros_procesados += len(items)

                for item in items:
                    payment_id = item.get("payment_id", "")
                    monto_pago = item.get("monto_pago", "")
                    created_at = item.get("created_at", "")
                    created_at = str(created_at) if created_at else ""

                    row = [
                        item.get("tenant_id", ""),
                        payment_id,
                        item.get("reservation_id", ""),
                        monto_pago,
                        created_at,
                        item.get("status", ""),
                    ]
                    escritor_csv.writerow(row)

                if "LastEvaluatedKey" in respuesta:
                    scan_kwargs["ExclusiveStartKey"] = respuesta["LastEvaluatedKey"]
                else:
                    break
        log_info(f"Datos exportados correctamente a {archivo_csv}. Registros procesados: {registros_procesados}")
    except Exception as e:
        log_error(f"Error al exportar datos desde DynamoDB: {e}")
        raise


def subir_csv_a_s3(archivo_csv, nombre_bucket):
    carpeta_destino = "payments/"
    archivo_s3 = f"{carpeta_destino}{archivo_csv}"
    log_info(f"Subiendo {archivo_csv} al bucket S3 ({nombre_bucket}) en la carpeta 'payments'...")
    try:
        s3.upload_file(archivo_csv, nombre_bucket, archivo_s3)
        log_info(f"Archivo subido exitosamente a S3: {archivo_s3}")
    except Exception as e:
        log_error(f"Error al subir el archivo a S3: {e}")
        raise


def crear_base_de_datos_en_glue(glue_database):
    log_info(f"Verificando existencia de la base de datos Glue: {glue_database}")
    try:
        glue.get_database(Name=glue_database)
        log_info(f"La base de datos {glue_database} ya existe.")
    except glue.exceptions.EntityNotFoundException:
        log_info(f"La base de datos {glue_database} no existe. Creando base de datos...")
        try:
            glue.create_database(
                DatabaseInput={
                    "Name": glue_database,
                    "Description": "Base de datos para almacenamiento de pagos en Glue.",
                }
            )
            log_info(f"Base de datos {glue_database} creada exitosamente.")
        except Exception as e:
            log_error(f"Error al crear la base de datos Glue: {e}")
            raise


def registrar_datos_en_glue(glue_database, glue_table_name, nombre_bucket, archivo_csv):
    input_path = f"s3://{nombre_bucket}/payments/"
    log_info(f"Registrando datos en Glue Data Catalog: {glue_table_name}")
    try:
        glue.create_table(
            DatabaseName=glue_database,
            TableInput={
                "Name": glue_table_name,
                "StorageDescriptor": {
                    "Columns": [
                        {"Name": "tenant_id", "Type": "string"},
                        {"Name": "payment_id", "Type": "string"},
                        {"Name": "reservation_id", "Type": "string"},
                        {"Name": "monto_pago", "Type": "decimal"},
                        {"Name": "created_at", "Type": "timestamp"},
                        {"Name": "status", "Type": "string"},
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
        log_info(f"Tabla {glue_table_name} registrada exitosamente en Glue.")
    except Exception as e:
        log_error(f"Error al registrar la tabla en Glue: {e}")
        raise


if __name__ == "__main__":
    inicio = datetime.now()
    log_info("Inicio del proceso de ingesta6.py")

    try:
        crear_base_de_datos_en_glue(glue_database)
        exportar_dynamodb_a_csv(tabla_dynamo, archivo_csv)
        subir_csv_a_s3(archivo_csv, nombre_bucket)
        registrar_datos_en_glue(glue_database, glue_table_name, nombre_bucket, archivo_csv)
    except Exception as e:
        log_error(f"Proceso fallido: {e}")
    finally:
        fin = datetime.now()
        log_info(f"Fin del proceso de ingesta6.py. Duración total: {fin - inicio}")
