import argparse
import datetime
import os
import platform
import subprocess
from tracemalloc import start
import zipfile
import csv
import configparser
import psycopg2
import pandas as pd
import logging

from pathlib import Path
from typing import Any
from logging import Handler

from redmail import EmailSender
from rocketry import Rocketry
from rocketry.args import Arg

app = Rocketry(execution='async')

############# LOGGING

logger = logging.getLogger("rocketry.scheduler")
logger.setLevel(logging.INFO)

handler = logging.FileHandler("app.log")
handler.setLevel(logging.INFO)

formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
handler.setFormatter(formatter)

logger.addHandler(handler)
logger.addHandler(logging.StreamHandler())
#######################
global PATH_FOLDER_OUT
global PATH_FILE_ID_SENSORS
#######################



class EmaillNotifyHandler(Handler):

    def __init__(self, level: int = logging.NOTSET, email=None):
        Handler.__init__(self, level)

        if not email:
            logger.error("EmaillNotifyHandler: attr 'email' não informado!")
            raise Exception("attr 'email' não informado!")
        self._email = email

    def emit(self, record: logging.LogRecord):
        self._email.send(
            subject=f'Takadu Service - Log Record: {record.levelname}',
            text=f"Logging level: {record.levelname}\nMessage: {record.message}",
            html=f"<ul><li>Logging level: {record.levelname}</li><li>Message: {record.message}</li></ul>",
            attachments={
                'app.log': Path("app.log")
            }
        )


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
        os.environ['PG_DATABASE'] = config['postgresql']['database']
        os.environ['PG_HOST'] = config['postgresql']['host']
        os.environ['PG_PORT'] = config['postgresql']['port']

        if not os.environ.get('PG_USER'):
            raise Exception("Variável de ambiente `PG_USER` não foi definida.")
        if not os.environ.get('PG_PASSWORD'):
            raise Exception("Variável de ambiente `PG_PASSWORD` não foi definida.")

        # FTP Config
        os.environ['FTP_DIR_AGUA'] = config['google_cloud']['agua_dir']
        os.environ['FTP_DIR_ESGOTO'] = config['google_cloud']['esgoto_dir']


        PATH_FOLDER_OUT = config['default']['PATH_FOLDER_OUT']
        PATH_FILE_ID_SENSORS = config['default']['PATH_FILE_ID_SENSORS']

        global URL_FILE_ID_SENSORS
        URL_FILE_ID_SENSORS = config['default']['URL_FILE_ID_SENSORS']
        global BASE_DIR
        BASE_DIR = Path(__file__).resolve().parent

        error_notify_email_username = os.environ.get('EMAIL_USERNAME')
        error_notify_email_password = os.environ.get('EMAIL_PASSWORD')

        email = EmailSender(
            host="smtp.office365.com",
            port=587,
            # use_starttls=True,
            username=error_notify_email_username,
            password=error_notify_email_password,
        )

        email.receivers = config['notify_outlook']['receivers']

        erro_handler = EmaillNotifyHandler(
            level=logging.ERROR,
            email=email
        )

        logger.addHandler(erro_handler)
        return True
    except Exception as e:
        logger.error("ERROR IN THE LOADING OF THE ENVIRONMENT:", e)
        return False


def connect_to_postgres():
    try:
        database = os.environ.get("PG_DATABASE")
        user = os.environ.get("PG_USER")
        password = os.environ.get("PG_PASSWORD")
        host = os.environ.get("PG_HOST")
        port = os.environ.get("PG_PORT")

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


def load_csv_list_sensors(path_file: str, delimiter: str = ";") -> tuple[Any, Any]:
    df_sensors = pd.read_csv(URL_FILE_ID_SENSORS, delimiter=delimiter)
    if df_sensors.empty:
        df_sensors = pd.read_csv(path_file, delimiter=delimiter)    
    if "Sensor" not in df_sensors.columns:
        logger.error(f"'Sensor' column not found in file: {path_file}")
        raise Exception(f"'Sensor' column not found in file: {path_file}")
    return df_sensors.loc[df_sensors["type"] == 'AGUA', "Sensor"].tolist(), \
        df_sensors.loc[df_sensors["type"] == 'ESGOTO', "Sensor"].tolist()


def _fetchall_to_csv(rows, header, csv_file):
    with open(csv_file, 'w', newline="") as f:
        writer = csv.writer(f, delimiter=";")
        writer.writerow(header)
        writer.writerows(rows)


def load_data_from_db(conn,
                      table_name,
                      ids_sensors: list,
                      start_date: datetime.datetime,
                      end_date: datetime.datetime,
                      bucket_interval: str = '2 min') -> tuple:
    if conn and table_name and ids_sensors and isinstance(ids_sensors, list):
        _sensors = ','.join(str(x).replace("SEN_", "") for x in ids_sensors)

        if isinstance(_sensors, list):
            _sensors = tuple(_sensors)
        elif isinstance(_sensors, int):
            _sensors = f'({_sensors})'
        elif len(_sensors) == 1:
            _sensors = f'({_sensors[0]})'

        if isinstance(start_date, datetime.datetime):
            start_date = start_date.strftime("%Y-%m-%d %H:%M:%S")

        if isinstance(start_date, datetime.datetime):
            end_date = end_date.strftime("%Y-%m-%d %H:%M:%S")

        try:
            with conn.cursor() as cursor:
                query = f"""WITH measure_avg AS (
                            SELECT
                                pc.id as _point_id,
                                ei.name as equipment_name,
                                pc.equipmentinstallation_id as equipmentinstallation_id,
                                time_bucket('{bucket_interval}'::interval, m.measure_datetime::TIMESTAMP) as bucket,
                                stats_agg(CASE 
                                    WHEN m.calculated_measure < 0 THEN 0 
                                    ELSE m.calculated_measure 
                                END) as stats
                            FROM public.measure m
                            INNER JOIN pointconfig pc ON pc.id = m.pointconfig_id	
                            INNER JOIN equipmentinstallation ei ON ei.id = pc.equipmentinstallation_id

                            WHERE 
                            m.measure_datetime BETWEEN '{start_date}' AND '{end_date}'
                            AND pc.id IN ({_sensors})

                            GROUP BY 1,2,3,4)

                        SELECT
                            'SEN_' || _point_id Name,
                            bucket Timestamp,
                            average(rollup(stats)) AS "Value"
                        FROM measure_avg 
                        GROUP BY 1,2
                        ORDER BY 2 
                        """

                cursor.execute(query)
                rows = cursor.fetchall()
                header = [desc[0] for desc in cursor.description]
                if len(rows) > 0:
                    return rows, header
                raise Exception("No data found in the last 24 hours")
        except (Exception, psycopg2.Error) as error:
            logger.critical("Error while fetching data from PostgreSQL", error)
            return False, False


def save_list_to_csv_and_zip(data_list: list, 
                             header: list, 
                             _type: str, 
                             data_source_name="TELELOG",
                             destination_folder=".\\out",
                             zip_file=True,
                             to_ftp=True):
    """
    Save a list of data to a .csv file, and optionally compress it in a .zip file. 
    The .csv file will have a dynamic name with the following format: "DataSourceName_YYYYMMDDHH24MMSS.csv".

    Parameters:
        data_list (list): A list containing data in the form of tuples (e.g., returned from cursor.fetchall()).
        header (list): A list containing the column headers for the data in the same order as the data_list tuples.
        _type (str): System Type - Water or Sewage (AGUA, ESGOTO).
        data_source_name (str): Name of the data source that will be used as a prefix in the .csv file name.
                                 Default is "TELELOG".
        destination_folder (str): The folder path where the .zip file will be saved. Default is ".\\out".
        zip_file (bool): If True, compress the .csv file into a .zip file. Default is True.
        to_ftp (bool): If True, upload the .zip file to an FTP server using the upload_to_ftp function. Default is True.

    Raises:
        - Exception: If data_list is not a list of tuples.
        - Exception: If the .csv file is not found for zipping (only if zip_file is True).

    Note:
        The upload_to_ftp function, if used (to_ftp=True), requires appropriate environment variables to be set
        for successful FTP connection and file upload.

    Example:
        Assuming data_list contains the data and header contains the column names, you can call the function like this:
        >>> save_list_to_csv_and_zip(data_list, header, _type="AGUA", data_source_name="SOME_DATASOURCE")
    """
    if not all(isinstance(i, tuple) for i in data_list):
         raise Exception("data_list is not an instance of 'list' containing tuples.")

    if not os.path.exists(destination_folder):
        os.makedirs(destination_folder)

    now = datetime.datetime.now()
    csv_file = f"{data_source_name}_{now.strftime('%Y%m%d%H%M%S')}"
    path_to_save_csv = os.path.join(destination_folder, csv_file + ".csv")
    _fetchall_to_csv(rows=data_list, header=header, csv_file=path_to_save_csv)

    if zip_file:
        if not os.path.isfile(path_to_save_csv):
            logger.error("save_list_to_csv_and_zip: File .csv not found.")
            raise Exception("save_list_to_csv_and_zip: File .csv not found.")

        zip_file_name = f"{_type}-{csv_file}.zip"
        path_to_save_zip = os.path.join(destination_folder, zip_file_name)
        with zipfile.ZipFile(path_to_save_zip, 'w', zipfile.ZIP_DEFLATED) as myzip:
            myzip.write(path_to_save_csv, arcname=csv_file + ".csv")

        os.remove(path_to_save_csv)

    if to_ftp:
        ...
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

    folder_path = 'scripts\\uploadWavin'
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


def download_and_save(conn, list_ids_agua, list_ids_esgoto, start_date, end_date):
    data_agua, header_agua = load_data_from_db(conn, "measure", ids_sensors=list_ids_agua,
                                               start_date=start_date, end_date=end_date)
    if data_agua:
        logger.info("Save file for new data [data_agua]")
        save_list_to_csv_and_zip(data_list=data_agua, header=header_agua, _type='AGUA', to_ftp=True)
    
    data_esgoto, header_esgoto = load_data_from_db(conn, "measure", ids_sensors=list_ids_esgoto,
                                                   start_date=start_date, end_date=end_date)

    if data_esgoto:
        logger.info("Save file for new data [data_esgoto]")
        save_list_to_csv_and_zip(data_list=data_esgoto, header=header_esgoto, _type='ESGOTO', to_ftp=True)


# @app.task(every(RUN_EVERY_TIME) | retry(3))
@app.task()
async def run_app(date_range_in_hours=Arg('date_range_in_hours')):
    logger.info("Iniciando carregamento dos dados...")

    start_time = datetime.datetime.now()

    list_ids_agua, list_ids_esgoto = load_csv_list_sensors(PATH_FILE_ID_SENSORS)
    conn = connect_to_postgres()

    _end_date = datetime.datetime.now()
    _start_date = _end_date - datetime.timedelta(hours=date_range_in_hours)
    # _start_date = datetime.datetime.strptime('25/10/24 00:00:00', '%d/%m/%y %H:%M:%S')
    # _end_date = datetime.datetime.strptime('9/11/24 00:00:00', '%d/%m/%y %H:%M:%S')

    diff_days = (_end_date - _start_date).days

    _max_days_per_file = 30
    if diff_days > _max_days_per_file:
        amount_intervals = diff_days // _max_days_per_file + 1
        for i in range(amount_intervals):
            start_date_interval = _start_date + datetime.timedelta(days=i * _max_days_per_file)
            end_date_interval = _start_date + datetime.timedelta(days=(i + 1) * _max_days_per_file - 1)
            print(f"Consulta do intervalo {i + 1}: {start_date_interval} até {end_date_interval}")
            download_and_save(conn, list_ids_agua, list_ids_esgoto, start_date_interval, end_date_interval)
    else:
        download_and_save(conn, list_ids_agua, list_ids_esgoto, _start_date, _end_date)
    end_time = datetime.datetime.now()
    exec_time = round((end_time - start_time).total_seconds(), 2)
    logger.info(f"Tempo para carregar os dados: {exec_time} segundos")


def main():
    print("Serviço TaKaDu Load Data iniciado...")

    # Criação do objeto ArgumentParser
    parser = argparse.ArgumentParser(description='TaKadu load data')

    # Adiciona os argumento de linha de comando
    parser.add_argument('-t', '--tempo', type=int, default=30, help='Tempo em minutos para executar')
    parser.add_argument('-dr', '--date_range', type=int, default=2, help='Tempo em horas do intervalo de dados do banco.' )
    parser.add_argument('-ls', '--list_sensors', default='data/sensors.csv',
                        help='Caminho do arquivo para lista dos sensores (IDs)')

    # Parse dos argumento fornecidos
    args = parser.parse_args()

    # Acessa os valores do arqgumentos
    tempo = args.tempo
    date_range = args.date_range
    path_sensors = args.list_sensors

    # Imprime os valroes dos argumentos
    print(f'Será executado a cada {tempo} minutos')
    print(f'Para cada envio teremos um histórico de {date_range} horas')
    print('Arquivo', path_sensors)

    run_time_loop = 'every ' + str(tempo) + ' minutes'

    # Getting a task instance
    task = app.session[run_app]

    # Setting batch run
    task.start_cond = run_time_loop


    app.params(date_range_in_hours=date_range)

    app.run()


if __name__ == '__main__':
    PATH_FOLDER_OUT = ""
    PATH_FILE_ID_SENSORS = "./data/sensors.csv"

    if initial_config('config.ini'):
        main()
