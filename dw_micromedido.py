import argparse
import datetime
import os
import platform
import subprocess
import zipfile
import csv
import configparser
import psycopg2
import pandas as pd  # noqa: F401
import logging

from pathlib import Path


from rocketry import Rocketry
from rocketry.args import Arg

app = Rocketry(execution='async')

############# LOGGING

logger = logging.getLogger("rocketry.scheduler")
logger.setLevel(logging.INFO)

handler = logging.FileHandler("app_micromedido.log")
handler.setLevel(logging.INFO)

formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
handler.setFormatter(formatter)

logger.addHandler(handler)
logger.addHandler(logging.StreamHandler())
#######################
global PATH_FOLDER_OUT
global PATH_FILE_ID_SENSORS
#######################


def initial_config(config_file_path) -> bool:
    """
    Initializes the application configuration by reading the values from the provided configuration file.

    Parameters:
        config_file_path (str): The path to the configuration file.

    Returns:
        bool: True if the configuration was successfully loaded, False otherwise.

    Raises:
        Exception: If any required environment variables (PG_USER, PG_PASSWORD, FTP_USER, FTP_PASSWORD)
                   or configuration settings are missing in the config file.

    Note:
        - This function sets various environment variables and configures the logging system for error notification.
        - It assumes that the configuration file is in the format expected by configparser.ConfigParser.

    Example:
        Assuming the configuration file is located at "config.ini", you can call the function like this:
        >>> if initial_config("config.ini"):
        ...     print("Configuration loaded successfully.")
        ... else:
        ...     print("Failed to load configuration.")
    """
    try:
        config = configparser.ConfigParser()
        config.read(config_file_path)

        # Postgres Config
        os.environ['PGDW_DATABASE'] = config['dw']['database']
        os.environ['PGDW_HOST'] = config['dw']['host']
        os.environ['PGDW_PORT'] = config['dw']['port']

        if not os.environ.get('PGDW_USER'):
            raise Exception("Variável de ambiente `PGDW_USER` não foi definida.")
        if not os.environ.get('PGDW_PASSWORD'):
            raise Exception("Variável de ambiente `PGDW_PASSWORD` não foi definida.")


        global PATH_FOLDER_OUT
        global PATH_FILE_ID_SENSORS
        PATH_FOLDER_OUT = config['default']['PATH_FOLDER_OUT']

        global BASE_DIR
        BASE_DIR = Path(__file__).resolve().parent

        return True
    except Exception as e:
        logger.error("ERROR IN THE LOADING OF THE ENVIRONMENT:", e)
        return False

def connect_to_postgres():
    try:
        database = os.environ.get("PGDW_DATABASE")
        user = os.environ.get("PGDW_USER")
        password = os.environ.get("PGDW_PASSWORD")
        host = os.environ.get("PGDW_HOST")
        port = os.environ.get("PGDW_PORT")

        conn = psycopg2.connect(
            database=database,
            user=user,
            password=password,
            host=host,
            port=port
        )
        return conn
    except (Exception, psycopg2.Error) as e:
        logger.error("ERROR while connecting to PostgreSQL", e)
        return None


def _fetchall_to_csv(rows, header, csv_file):
    with open(csv_file, 'w', newline="") as f:
        writer = csv.writer(f, delimiter=";")
        writer.writerow(header)
        writer.writerows(rows)


def load_data_from_db(conn, ref_date) -> tuple:
    """
    Carrega dados do banco para sensores e intervalo informado.
    """
    if conn and ref_date:

        if isinstance(ref_date, datetime.datetime):
            ref_date_fmt = ref_date.strftime("%Y%m%d")
        else:
            ref_date_fmt = str(ref_date)

        try:
            with conn.cursor() as cursor:
                query = f"""
                SELECT 
                    datetime
                    , tag
                    , value
                    , profit 
                    , connections
                FROM marts_takadu.rpt_micromedido_takadu
                where ref_date = '{ref_date_fmt}'
                """

                cursor.execute(query)
                rows = cursor.fetchall()
                header = [desc[0] for desc in cursor.description]
                if len(rows) > 0:
                    return rows, header
                raise Exception("No data found in the last 30 days")
        except (Exception, psycopg2.Error) as error:
            logger.critical("Error while fetching data from PostgreSQL", error)
            return False, False


def save_list_to_csv_and_zip(data_list: list, 
                             header: list, 
                             ref_date: datetime.date,
                             data_source_name="CAJ_COMSUMPTION",
                             destination_folder=".\\out_micro",
                             zip_file=True,
                             run_script=False):
    """
    Save a list of data to a .csv file, and optionally compress it in a .zip file. 
    The .csv file will have a dynamic name with the following format: "DataSourceName_YYYYMMDDHH24MMSS.csv".

    Parameters:
        data_list (list): A list containing data in the form of tuples (e.g., returned from cursor.fetchall()).
        header (list): A list containing the column headers for the data in the same order as the data_list tuples.
        data_source_name (str): Name of the data source that will be used as a prefix in the .csv file name.
                                 Default is "CAJ_COMSUMPTION".
        destination_folder (str): The folder path where the .zip file will be saved. Default is ".\\out_micro".
        zip_file (bool): If True, compress the .csv file into a .zip file. Default is True.
        run_script (bool): .

    Raises:
        - Exception: If data_list is not a list of tuples.
        - Exception: If the .csv file is not found for zipping (only if zip_file is True).

    Example:
        Assuming data_list contains the data and header contains the column names, you can call the function like this:
        >>> save_list_to_csv_and_zip(data_list, header, _type="AGUA", data_source_name="SOME_DATASOURCE")
    """
    if not all(isinstance(i, tuple) for i in data_list):
         raise Exception("data_list is not an instance of 'list' containing tuples.")

    if not os.path.exists(destination_folder):
        os.makedirs(destination_folder)

    ref_date = ref_date.strftime('%Y_%m_%d')
    csv_file = f"{data_source_name}_{ref_date}"
    path_to_save_csv = os.path.join(destination_folder, csv_file + ".csv")
    _fetchall_to_csv(rows=data_list, header=header, csv_file=path_to_save_csv)

    if zip_file:
        if not os.path.isfile(path_to_save_csv):
            logger.error("save_list_to_csv_and_zip: File .csv not found.")
            raise Exception("save_list_to_csv_and_zip: File .csv not found.")

        zip_file_name = f"{csv_file}.zip"
        path_to_save_zip = os.path.join(destination_folder, zip_file_name)
        with zipfile.ZipFile(path_to_save_zip, 'w', zipfile.ZIP_DEFLATED) as myzip:
            myzip.write(path_to_save_csv, arcname=csv_file + ".csv")

        os.remove(path_to_save_csv)

    if run_script:
        run_batch_script()
        delete_files_in_folder(destination_folder)


def delete_files_in_folder(folder_path: str):
    """
    Delete all files in the specified folder.

    Parameters:
        folder_path (str): The path to the folder containing the files to be deleted.

    Returns:
        None

    Raises:
        OSError: If an error occurs while attempting to delete the files.

    Note:
        - Only files in the folder will be deleted. Subdirectories, if any, will remain untouched.
        - If the specified folder does not exist or is empty, no files will be deleted.

    Example:
        Assuming you want to delete all files in the folder "data_files", you can call the function like this:
        >>> delete_files_in_folder("data_files")
    """
    try:
        file_list = os.listdir(folder_path)
        for file_name in file_list:
            file_path = os.path.join(folder_path, file_name)
            if os.path.isfile(file_path):
                os.remove(file_path)
        print(f"Todos os arquivos em {folder_path} foram deletados.")
    except OSError as e:
        print(f"Erro ao deletar os arquivos em {folder_path}: {e}")


def run_batch_script():
    """
    Execute a batch script (.bat) on Windows or a shell script (.sh) on Linux.

    Parameters:
        file_path (str): The path to the batch (.bat) or shell (.sh) script.

    Returns:
        None

    Raises:
        OSError: If the file is not found or cannot be executed.
        ValueError: If the platform is not recognized (neither Windows nor Linux).

    Example:
        Assuming you want to execute a script named "myscript.bat" or "myscript.sh",
        you can call the function like this:
        >>> run_batch_script("myscript.bat")  # On Windows
        >>> run_batch_script("myscript.sh")   # On Linux
    """
    system = platform.system()

    folder_path = 'scripts\\uploadWavinMicromedido'
    if system == "Windows":
        sufix_file = ".bat"
    elif system == "Linux":
        sufix_file = ".sh"
    full_file_path = os.path.join(BASE_DIR, folder_path + sufix_file)
    
    if not os.path.exists(full_file_path):
        raise OSError(f"File not found: {full_file_path}")

    try:
        if system == "Windows":
            subprocess.run(full_file_path, shell=True)
        elif system == "Linux":
            subprocess.run(["bash", full_file_path])
        else:
            raise ValueError("Unsupported platform: Only Windows and Linux are supported.")
    except OSError as e:
        raise OSError(f"Error executing the script: {e}")


def download_and_save(conn, ref_date):
    data_micromedido, header_micromedido = load_data_from_db(conn, ref_date=ref_date)
    if data_micromedido:
        logger.info("Save file for new data [data_micromedido]")
        save_list_to_csv_and_zip(data_list=data_micromedido, ref_date=ref_date, header=header_micromedido, run_script=True)

@app.task()
async def run_app(month_ref=Arg('month_ref')):
    logger.info("Iniciando carregamento dos dados...")

    start_time = datetime.datetime.now()

    conn = connect_to_postgres()

    # Define o intervalo do mês de referência
    if isinstance(month_ref, str):
        try:
            ref_date = datetime.datetime.strptime(month_ref, '%Y-%m-%d')
        except Exception:
            try:
                ref_date = datetime.datetime.strptime(month_ref, '%Y-%m-%d %H:%M:%S')
            except Exception:
                ref_date = datetime.datetime.strptime(month_ref, '%d/%m/%Y')
    else:
        ref_date = month_ref

    print(f"Extraindo dados de {ref_date.strftime('%d/%m/%Y')}")
    download_and_save(conn, ref_date)

    end_time = datetime.datetime.now()
    exec_time = round((end_time - start_time).total_seconds(), 2)
    logger.info(f"Tempo para carregar os dados: {exec_time} segundos")


def main():
    print("Serviço TaKaDu Load Data iniciado...")

    # Criação do objeto ArgumentParser
    parser = argparse.ArgumentParser(description='TaKadu load data')

    # Adiciona os argumentos de linha de comando
    parser.add_argument('-t', '--tempo', type=int, default=None, help='Tempo em minutos para executar (opcional, se não informado executa agendado)')
    parser.add_argument('-m', '--mes', type=str, default=None, help='Mês de referência no formato MM/YYYY ou DD/MM/YYYY (opcional)')

    # Parse dos argumentos fornecidos
    args = parser.parse_args()

    # Processa o mês de referência
    if args.mes:
        try:
            # Aceita MM/YYYY ou DD/MM/YYYY
            if len(args.mes.split('/')) == 2:
                ref_date = datetime.datetime.strptime('01/' + args.mes, '%d/%m/%Y')
            else:
                ref_date = datetime.datetime.strptime(args.mes, '%d/%m/%Y')
        except Exception as e:
            print(f"Erro ao interpretar o mês de referência: {e}")
            return
    else:
        # Usa mês anterior ao atual
        today = datetime.datetime.now()
        first_day_this_month = today.replace(day=1)
        ref_date = (
            first_day_this_month - datetime.timedelta(days=1)
        ).replace(day=1, hour=0, minute=0, second=0, microsecond=0)

    print(f'Mês de referência: {ref_date.strftime("%m/%Y")}')

    # Define a condição de execução
    if args.tempo:
        run_time_loop = f'every {args.tempo} minutes'
        print(f'Será executado a cada {args.tempo} minutos')
    else:
        # Executa todo dia 1 às 01h
        # run_time_loop = 'cron 0 1 1 * *'
        run_time_loop = 'cron 0 * * * *'
        print('Será executado todo dia 1 às 01h')

    # Getting a task instance
    task = app.session[run_app]
    task.start_cond = run_time_loop

    # Passa o mês de referência para o Rocketry
    app.params(month_ref=ref_date)
    app.run()


if __name__ == '__main__':
    if initial_config('config.ini'):
        main()
