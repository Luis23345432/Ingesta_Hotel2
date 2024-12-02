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

# Recorrer todas las carpetas del diccionario
for carpeta in "${!carpetas[@]}"; do
  imagen="${carpetas[$carpeta]}" # Obtener el nombre de la imagen correspondiente

  echo "Construyendo la imagen Docker para $carpeta (imagen: $imagen)..."

  # Cambiar al directorio de la carpeta correspondiente
  cd $carpeta

  # Construir la imagen Docker
  docker build -t $imagen .

  # Ejecutar el contenedor Docker con los argumentos --stage y --bucket
  echo "Corriendo el contenedor para $carpeta con la imagen $imagen..."
  docker run -v /home/ubuntu/.aws/credentials:/root/.aws/credentials $imagen --stage "$stage" --bucket "$bucket"

  # Volver al directorio anterior
  cd ..

  echo "Proceso completado para $carpeta."
done

echo "¡Todos los procesos de build y run han sido completados!"
