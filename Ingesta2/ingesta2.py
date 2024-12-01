import boto3
import csv
import os
import time

dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
s3 = boto3.client('s3', region_name='us-east-1')
glue = boto3.client('glue', region_name='us-east-1')

tabla_dynamo = 'dev-hotel-services'  # Tabla de servicios
nombre_bucket = 'ingesta-hotel-stage-dev'
archivo_csv = 'stage-prod-services.csv'
glue_database = 'stage-prod'
glue_table_name = 'stage-prod-services'


def exportar_dynamodb_a_csv(tabla_dynamo, archivo_csv):
    print(f"Exportando datos desde DynamoDB ({tabla_dynamo})...")
    tabla = dynamodb.Table(tabla_dynamo)
    scan_kwargs = {}

    with open(archivo_csv, 'w', newline='') as archivo:
        escritor_csv = csv.writer(archivo)

        while True:
            respuesta = tabla.scan(**scan_kwargs)
            items = respuesta['Items']

            if not items:
                break

            for item in items:
                try:
                    service_id = item.get('service_id', '')
                except ValueError:
                    service_id = ''

                row = [
                    item.get('tenant_id', ''),
                    service_id,
                    item.get('service_category', ''),
                    item.get('service_name', ''),
                    item.get('descripcion', ''),
                    item.get('precio', '')
                ]

                escritor_csv.writerow(row)

            if 'LastEvaluatedKey' in respuesta:
                scan_kwargs['ExclusiveStartKey'] = respuesta['LastEvaluatedKey']
            else:
                break

    print(f"Datos exportados a {archivo_csv}")


def subir_csv_a_s3(archivo_csv, nombre_bucket):
    carpeta_destino = 'services/'
    archivo_s3 = f"{carpeta_destino}{archivo_csv}"
    print(f"Subiendo {archivo_csv} al bucket S3 ({nombre_bucket}) en la carpeta 'services'...")

    try:
        s3.upload_file(archivo_csv, nombre_bucket, archivo_s3)
        print(f"Archivo subido exitosamente a S3 en la carpeta 'services'.")
        return True
    except Exception as e:
        print(f"Error al subir el archivo a S3: {e}")
        return False


def crear_base_de_datos_en_glue(glue_database):
    """Crear base de datos en Glue si no existe."""
    try:
        glue.get_database(Name=glue_database)
        print(f"La base de datos {glue_database} ya existe.")
    except glue.exceptions.EntityNotFoundException:
        print(f"La base de datos {glue_database} no existe. Creando base de datos...")
        glue.create_database(
            DatabaseInput={
                'Name': glue_database,
                'Description': 'Base de datos para almacenamiento de servicios en Glue.'
            }
        )
        print(f"Base de datos {glue_database} creada exitosamente.")
    except Exception as e:
        print(f"Error al verificar o crear la base de datos en Glue: {e}")
        return False
    return True


def registrar_datos_en_glue(glue_database, glue_table_name, nombre_bucket, archivo_csv):
    """Registrar datos en Glue Data Catalog."""
    print(f"Registrando datos en Glue Data Catalog...")
    input_path = f"s3://{nombre_bucket}/services/"

    try:
        glue.create_table(
            DatabaseName=glue_database,
            TableInput={
                'Name': glue_table_name,
                'StorageDescriptor': {
                    'Columns': [
                        {'Name': 'tenant_id', 'Type': 'string'},
                        {'Name': 'service_id', 'Type': 'string'},
                        {'Name': 'service_category', 'Type': 'string'},
                        {'Name': 'service_name', 'Type': 'string'},
                        {'Name': 'descripcion', 'Type': 'string'},
                        {'Name': 'precio', 'Type': 'string'}
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
        print(f"Tabla {glue_table_name} registrada exitosamente en la base de datos {glue_database}.")
    except Exception as e:
        print(f"Error al registrar la tabla en Glue: {e}")


if __name__ == "__main__":
    if crear_base_de_datos_en_glue(glue_database):
        exportar_dynamodb_a_csv(tabla_dynamo, archivo_csv)

        if subir_csv_a_s3(archivo_csv, nombre_bucket):
            registrar_datos_en_glue(glue_database, glue_table_name, nombre_bucket, archivo_csv)
        else:
            print("No se pudo completar el proceso porque hubo un error al subir el archivo a S3.")
    else:
        print("Error en la creación de la base de datos Glue. No se continuará con el proceso.")

    print("Proceso completado.");
