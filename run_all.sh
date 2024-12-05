#!/bin/bash

# Solicitar al usuario el stage y el bucket
read -p "Ingrese el stage (por ejemplo, dev, test, prod): " stage
read -p "Ingrese el nombre del bucket de S3: " bucket

# Definir las carpetas y las imágenes Docker (diccionario)
declare -A carpetas
carpetas=(
    ["Ingesta1"]="ingesta1"
    ["Ingesta2"]="ingesta2"
    ["Ingesta3"]="ingesta3"
    ["Ingesta4"]="ingesta4"
    ["Ingesta5"]="ingesta5"
    ["Ingesta6"]="ingesta6"
)

# Crear el directorio de logs si no existe
LOG_HOST_DIR="/home/ubuntu/logs"
if [ ! -d "$LOG_HOST_DIR" ]; then
    echo "Creando el directorio de logs en $LOG_HOST_DIR..."
    mkdir -p "$LOG_HOST_DIR"
    if [ $? -ne 0 ]; then
        echo "Error: No se pudo crear el directorio de logs en $LOG_HOST_DIR."
        exit 1
    fi
fi

# Recorrer todas las carpetas del diccionario
for carpeta in "${!carpetas[@]}"; do
  imagen="${carpetas[$carpeta]}" # Obtener el nombre de la imagen correspondiente

  echo "---------------------------------------------"
  echo "Construyendo la imagen Docker para $carpeta (imagen: $imagen)..."

  # Cambiar al directorio de la carpeta correspondiente
  if [ -d "$carpeta" ]; then
      cd "$carpeta" || { echo "Error: No se pudo cambiar al directorio $carpeta."; exit 1; }
  else
      echo "Error: El directorio $carpeta no existe."
      exit 1
  fi

  # Construir la imagen Docker
  echo "Construyendo la imagen Docker '$imagen'..."
  docker build -t "$imagen" .
  if [ $? -ne 0 ]; then
      echo "Error: La construcción de la imagen Docker '$imagen' falló."
      exit 1
  fi

  # Detener y eliminar el contenedor si ya existe
  if docker ps -a --format '{{.Names}}' | grep -Eq "^${imagen}$"; then
      echo "Deteniendo y eliminando el contenedor existente '$imagen'..."
      docker rm -f "$imagen"
      if [ $? -ne 0 ]; then
          echo "Error: No se pudo detener y eliminar el contenedor '$imagen'."
          exit 1
      fi
  fi

  # Ejecutar el contenedor Docker con los argumentos --stage y --bucket
  echo "Corriendo el contenedor para $carpeta con la imagen '$imagen'..."
  docker run -d \
    -v /home/ubuntu/.aws/credentials:/root/.aws/credentials \
    -v "$LOG_HOST_DIR":/logs \
    --name "$imagen" \
    "$imagen" --stage "$stage" --bucket "$bucket"

  if [ $? -eq 0 ]; then
      echo "Contenedor '$imagen' ejecutado exitosamente."
  else
      echo "Error: No se pudo ejecutar el contenedor '$imagen'."
      exit 1
  fi

  # Volver al directorio anterior
  cd .. || { echo "Error: No se pudo cambiar al directorio anterior."; exit 1; }

  echo "Proceso completado para $carpeta."
done

echo "---------------------------------------------"
echo "¡Todos los procesos de build y run han sido completados!"
