@echo off
echo Enviando arquivos para o Google Cloud Storage...

set BUCKET_NAME=wavin-partnerships-takadu-aguasjoinville-prod
set UPLOAD_DIR=./out_micro/
set UPLOAD_DONE=./out_micro/uploaded

rem Garantia de que o diretório UPLOAD_DONE existe
if not exist "%UPLOAD_DONE%" mkdir "%UPLOAD_DONE%"

rem Loop para enviar arquivos .csv e .zip para o bucket e mover para a pasta UPLOAD_DONE
for %%F in ("%UPLOAD_DIR%\*.csv" "%UPLOAD_DIR%\*.zip") do (
    gsutil cp "%%F" gs://%BUCKET_NAME%/comsumption/
    @REM move "%%F" "%UPLOAD_DONE%"
)

echo Arquivos enviados com sucesso e movidos para a pasta '%UPLOAD_DONE%'.