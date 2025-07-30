# Detalhes Técnicos do Sistema TaKadu

## 1. Adicionando Novos Sensores

### 1.1 Estrutura do Arquivo sensors.csv
O arquivo `sensors.csv` deve conter as seguintes colunas:
type;Subnet;Sensor
- type: Tipo do sensor (AGUA ou ESGOTO)
- Subnet: ID do subnet correspondente ao sensor
- Sensor: ID do sensor no formato SEN_[ID]

Exemplo:
```csv
type;Subnet;Sensor
AGUA;SUB_675;SEN_92734
AGUA;SUB_675;SEN_92733
ESGOTO;SUB_108;SEN_80927
ESGOTO;SUB_108;SEN_80928
```

### 1.2 Processo de Adição
1. Adicione o novo sensor no arquivo `sensors.csv`
2. Verifique se o sensor existe no banco de dados Telelog
3. Certifique-se que o sensor está ativo e coletando dados
4. O script está configurando para buscar os sensores disponiveis no arquivo URL_FILE_ID_SENSORS

## 2. Configuração do GSUTIL

### 2.1 Instalação
1. Baixe e instale o Google Cloud SDK: https://cloud.google.com/sdk/docs/install
2. Abra o terminal e execute:
```bash
gcloud init
```

### 2.2 Autenticação
1. Execute o comando:
```bash
gcloud auth login
```
2. Siga as instruções para autenticar com sua conta Google
3. Configure o projeto:
```bash
gcloud config set project [ID_DO_PROJETO]
```

## 3. Script de Envio para TaKadu (uploadWavin)

### 3.1 Estrutura do Script
O script `uploadWavin` está disponível em duas versões:
- Windows: `uploadWavin.bat`
- Linux/Mac: `uploadWavin.sh`

### 3.2 Configuração
O script utiliza as seguintes variáveis:
```bash
BUCKET_NAME=wavin-partnerships-takadu-aguasjoinville-prod
UPLOAD_DIR=./out/
UPLOAD_DONE=./out/uploaded
```

### 3.3 Funcionamento
1. O script monitora a pasta `./out/` por arquivos .zip
2. Quando encontra arquivos:
   - Envia para o bucket do Google Cloud Storage
   - Move os arquivos processados para a pasta `./out/uploaded/`
3. Logs de envio são exibidos no terminal


## 4. Fluxo de Dados

### 4.1 Coleta
1. Sistema consulta o banco Telelog (ÁGUA e ESGOTO)
2. Dados são agregados em intervalos de 2 minutos
3. Valores negativos são convertidos para 0

### 4.2 Processamento
1. Separação por tipo (água/esgoto)
2. Geração de arquivos CSV
3. Compactação em ZIP

### 4.3 Envio
1. Envio para Google Cloud Storage (através dos scripts da uploadWavin)
2. Remoção dos arquivos da pasta "out"

## 5. Tratamento de Erros

### 5.1 Logs
- Logs são salvos em `app.log`
- Notificações de erro são enviadas por email
- Logs incluem timestamps e detalhes do erro
