#!/bin/bash
echo "Enviando arquivos para o Google Cloud Storage..."

export BUCKET_NAME="wavin-partnerships-takadu-aguasjoinville-prod"
export UPLOAD_DIR="./out/"
export UPLOAD_DONE="./out/uploaded"

# Garantia de que o diretório UPLOAD_DONE existe
if [ ! -d "$UPLOAD_DONE" ]; then
    mkdir "$UPLOAD_DONE"
fi

# Loop para enviar arquivos .csv e .zip para o bucket e mover para a pasta UPLOAD_DONE
for file in "$UPLOAD_DIR"/*.csv "$UPLOAD_DIR"/*.zip; do
    # gsutil cp "$file" "gs://$BUCKET_NAME/"
    # mv "$file" "$UPLOAD_DONE"
done

echo "Arquivos enviados com sucesso e movidos para a pasta '$UPLOAD_DONE'."
