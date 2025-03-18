# Define as variáveis de ambiente
$env:PG_USER="ensitec"
$env:PG_PASSWORD="3ns1@7ec"

# Confirmação de que as variáveis foram definidas
Write-Host "Variáveis de ambiente definidas com sucesso:"
Write-Host "AZURE_DB_USERNAME: $($Env:PG_USER)"
Write-Host "AZURE_DB_PASSWORD: $($Env:PG_PASSWORD)"

python ../main.py -t 30 -dr 2