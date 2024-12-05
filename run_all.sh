#!/bin/bash

# Solicitar al usuario el stage y el bucket
read -p "Ingrese el stage (por ejemplo, dev, test, prod): " stage
read -p "Ingrese el nombre del bucket de S3: " bucket

# Validar que ambos valores no estén vacíos
if [[ -z "$stage" || -z "$bucket" ]]; then
  echo "Ambos valores (stage y bucket) son requeridos. Intente nuevamente."
  exit 1
fi

# Definir las carpetas y las imágenes Docker (diccionario)
declare -A carpetas
carpetas=(
    ["Ingesta1"]="ingesta1.py"
    ["Ingesta2"]="ingesta2.py"
    ["Ingesta3"]="ingesta3.py"
    ["Ingesta4"]="ingesta4.py"
    ["Ingesta5"]="ingesta5.py"
    ["Ingesta6"]="ingesta6.py"
)

# Crear un directorio para los logs en la máquina virtual si no existe
LOGS_DIR="/home/ubuntu/logs"
mkdir -p "$LOGS_DIR"

# Recorrer todas las carpetas del diccionario
for carpeta in "${!carpetas[@]}"; do
  imagen="${carpeta,,}"  # Convertir a minúsculas para usar como nombre de la imagen
  script="${carpetas[$carpeta]}"  # Obtener el script correspondiente

  echo "Construyendo la imagen Docker para $carpeta (imagen: $imagen)..."

  # Cambiar al directorio de la carpeta correspondiente
  cd "$carpeta"

  # Construir la imagen Docker
  docker build -t "$imagen" .

  # Ejecutar el contenedor Docker con el script y los argumentos
  echo "Corriendo el contenedor para $carpeta con la imagen $imagen..."
  docker run \
    -v /home/ubuntu/.aws/credentials:/root/.aws/credentials \
    -v "$LOGS_DIR:/app/logs" \
    "$imagen" "$script" --stage "$stage" --bucket "$bucket"

  # Volver al directorio anterior
  cd ..

  echo "Proceso completado para $carpeta."
done

echo "¡Todos los procesos de build y run han sido completados!"

