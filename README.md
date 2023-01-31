<div class="markdown prose w-full break-words dark:prose-invert dark">
<h1>Configuração do Banco de Dados PostgreSQL</h1>
<p>Este repositório fornece um exemplo de como configurar e se conectar a um banco de dados PostgreSQL.</p>

<h2>Pré-requisitos</h2>
<ul><li>Python 3.x</li>
<li>Módulo <code>psycopg2</code> para conexão com o banco de dados PostgreSQL</li>
<li>Módulo <code>pandas</code> para manipulação de dados</li>
<li>Módulo <code>rocketry</code></li>
<li>Módulo <code>redmail</code></li>
<li>Arquivo de configuração <code>database.ini</code> contendo as informações de conexão</li></ul>

<h2>Configuração do Arquivo config.ini</h2>
<p>Crie um arquivo <code>database.ini</code> na raiz do seu projeto com as seguintes informações de conexão:</p>

```text
[postgresql]
database = your_database_name
user = your_user
password = your_password
host = your_host
port = your_port
```
<p> Para as notificações de erro funcionarem será necessário definir variáveis de ambiente.</p>

Abra o Prompt de Comando e execute o seguinte comando:
Windows:
```shell
setx EMAIL_USERNAME "your_email_username"
setx EMAIL_PASSWORD "your_email_password"
```
Linux e MacOS:
```shell
export EMAIL_USERNAME="your_email_username"
export EMAIL_PASSWORD="your_email_password"
```

Observe que essas variáveis de ambiente só estarão disponíveis no terminal atual. Se você abrir um novo terminal, precisará definir as variáveis de ambiente novamente.