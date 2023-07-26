@echo off
echo Enviando arquivos para o Google Cloud Storage...

set BUCKET_NAME=wavin-partnerships-takadu-aguasdejoinville-prod
set UPLOAD_DIR=C:\Users\muril\Documents\script-subir-bucket
set UPLOAD_DONE=C:\Users\muril\Documents\script-subir-bucket\uploaded

rem Garantia de que o diretório UPLOAD_DONE existe
if not exist "%UPLOAD_DONE%" mkdir "%UPLOAD_DONE%"

rem Loop para enviar arquivos .csv e .zip para o bucket e mover para a pasta UPLOAD_DONE
for %%F in ("%UPLOAD_DIR%\*.csv" "%UPLOAD_DIR%\*.zip") do (
    gsutil cp "%%F" gs://%BUCKET_NAME%/
    move "%%F" "%UPLOAD_DONE%"
)

echo Arquivos enviados com sucesso e movidos para a pasta '%UPLOAD_DONE%'.