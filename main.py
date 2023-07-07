import datetime
import ftplib
import os

import zipfile
import csv
import configparser
from pathlib import Path
from typing import Any

import psycopg2
import pandas as pd

from redmail import EmailSender

from rocketry import Rocketry
from rocketry.conds import retry, every

from client_ftp import ClientFTP

app = Rocketry(execution='async')

############# LOGGING
import logging

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

from logging import Handler


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


def initial_config(config_file_path):
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
        os.environ['FTP_HOST'] = config['ftp']['host']
        os.environ['FTP_DIR_AGUA'] = config['ftp']['agua_dir']
        os.environ['FTP_DIR_ESGOTO'] = config['ftp']['esgoto_dir']

        if not os.environ.get('FTP_USER'):
            raise Exception("Variável de ambiente `FTP_USER` não foi definida.")
        if not os.environ.get('FTP_PASSWORD'):
            raise Exception("Variável de ambiente `FTP_PASSWORD` não foi definida.")

        PATH_FOLDER_OUT = config['default']['PATH_FOLDER_OUT']
        PATH_FILE_ID_SENSORS = config['default']['PATH_FILE_ID_SENSORS']

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

    except Exception as e:
        logger.error("ERROR IN THE LOADING OF THE ENVIROMENT:", e)


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
    df_sensors = pd.read_csv(path_file, delimiter=delimiter)
    if not "Sensor" in df_sensors.columns:
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
                      start_date: datetime.date,
                      end_date: datetime.date,
                      bucket_interval: str = '2 min') -> tuple:
    if conn and table_name and ids_sensors and isinstance(ids_sensors, list):
        _sensors = ','.join(str(x).replace("SEN_", "") for x in ids_sensors)

        if isinstance(_sensors, list):
            _sensors = tuple(_sensors)
        elif isinstance(_sensors, int):
            _sensors = f'({_sensors})'
        elif len(_sensors) == 1:
            _sensors = f'({_sensors[0]})'

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
                            m.measure_datetime BETWEEN '{start_date}' AND '{end_date} 23:59:59'
                            AND pc.id IN ({_sensors})

                            GROUP BY 1,2,3,4)

                        SELECT
                            'SEN_' || _point_id Name,
                            bucket Timestamp,
                            average(rollup(stats)) AS "Value"
                        FROM measure_avg
                        GROUP BY 1,2
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


def save_list_to_csv_and_zip(data_list: list, header: list, _type: str, data_source_name="TELELOG",
                             destination_folder=".\\out",
                             zip_file=True,
                             to_ftp=True):
    """
    Save a list to .csv file and compress it in a .zip file.
    The .csv file will have a dynamic name with the following format: "DataSourceName_YYYYMMDDHH24MMSS.csv".
    :param to_ftp:
    :param _type: System Type - Water or Sewage (AGUA, ESGOTO)
    :param data_list: list returned from cursor.fetchall() function "List with tuples"
    :param data_source_name: Name of the data source that will be used as a prefix in the .csv file name
    :param destination_folder: Folder where the .zip file will be saved
    :param zip_file: If it is necessary to "zip" the file
    """
    if not all(isinstance(i, tuple) for i in data_list):
        raise Exception("data_list not is a instance of 'list'")

    if not os.path.exists(destination_folder):
        os.makedirs(destination_folder)

    now = datetime.datetime.now()
    csv_file = f"{data_source_name}_{now.strftime('%Y%m%d%H%M%S')}"
    path_to_save_csv = os.path.join(destination_folder, csv_file + ".csv")
    _fetchall_to_csv(rows=data_list, header=header, csv_file=path_to_save_csv)

    if zip_file:
        if not os.path.isfile(path_to_save_csv):
            logger.error("save_list_to_csv_and_zip: File .csv not found.")
            raise Exception("File .csv not found.")

        zip_file_name = f"{_type}-{csv_file}.zip"
        path_to_save_zip = os.path.join(destination_folder, zip_file_name)
        with zipfile.ZipFile(path_to_save_zip, 'w', zipfile.ZIP_DEFLATED) as myzip:
            myzip.write(path_to_save_csv, arcname=csv_file + ".csv")

        os.remove(path_to_save_csv)

    if to_ftp:
        # Client FTP
        ftp_client = ClientFTP(
            user=os.environ.get('FTP_USER'),
            password=os.environ.get('FTP_PASSWORD'),
            host=os.environ.get('FTP_HOST'),
            port=22,
            tls_ssl=True
        )
        if ftp_client.connect():
            if _type == 'ESGOTO':
                dest_path = os.environ.get('FTP_DIR_ESGOTO', './upload/esgoto')
                ftp_client.upload(zip_file_name, path_to_save_zip, dest_path=dest_path)
            else:
                dest_path = os.environ.get('FTP_DIR_AGUA', './upload')
                ftp_client.upload(zip_file_name, path_to_save_zip, dest_path=dest_path)
            ftp_client.quit()

        delete_files_in_folder(destination_folder)


def delete_files_in_folder(folder_path: str):
    """
    Deleta todos os arquivos de uma pasta
    :param folder_path: Caminho da pasta.
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


# @app.task(every(RUN_EVERY_TIME) | retry(3))
@app.task()
async def run_app():
    logger.info("Iniciando carregamento dos dados...")
    start_time = datetime.datetime.now()

    list_ids_agua, list_ids_esgoto = load_csv_list_sensors(PATH_FILE_ID_SENSORS)
    conn = connect_to_postgres()

    _end_date = datetime.date.today()
    _start_date = _end_date - datetime.timedelta(days=1)

    diff_days = (_end_date - _start_date).days

    if diff_days > 30:
        amount_intervals = diff_days // 30 + 1
        for i in range(amount_intervals):
            start_date_interval = _start_date + datetime.timedelta(days=i * 30)
            end_date_interval = _start_date + datetime.timedelta(days=(i + 1) * 30 - 1)
            print(f"Consulta do intervalo {i + 1}: {start_date_interval} até {end_date_interval}")
            download_and_save(conn, list_ids_agua, list_ids_esgoto, start_date_interval, end_date_interval)
    else:
        download_and_save(conn, list_ids_agua, list_ids_esgoto, _start_date, _end_date)
    end_time = datetime.datetime.now()
    exec_time = round((end_time - start_time).total_seconds(), 2)
    logger.info(f"Tempo para carregar os dados: {exec_time} segundos")


def download_and_save(conn, list_ids_agua, list_ids_esgoto, start_date, end_date):
    data_agua, header_agua = load_data_from_db(conn, "measure", ids_sensors=list_ids_agua,
                                               start_date=start_date, end_date=end_date)
    data_esgoto, header_esgoto = load_data_from_db(conn, "measure", ids_sensors=list_ids_esgoto,
                                                   start_date=start_date, end_date=end_date)

    if data_agua:
        logger.info("Save file for new data [data_agua]")
        save_list_to_csv_and_zip(data_list=data_agua, header=header_agua, _type='AGUA', to_ftp=True)
    if data_esgoto:
        logger.info("Save file for new data [data_esgoto]")
        save_list_to_csv_and_zip(data_list=data_esgoto, header=header_esgoto, _type='ESGOTO', to_ftp=True)


import argparse


def main():
    print("Serviço TaKaDu Load Data iniciado...")

    # Criação do objeto ArgumentParser
    parser = argparse.ArgumentParser(description='TaKadu load data')

    # Adiciona os argumento de linha de comando
    parser.add_argument('-t', '--tempo', type=int, default=10, help='Tempo em minutos para executar')
    parser.add_argument('-ls', '--list_sensors', default='data/sensors.csv',
                        help='Caminho do arquivo para lista dos sensores (IDs)')

    # Parse dos argumento fornecidos
    args = parser.parse_args()

    # Acessa os valores do arqgumentos
    tempo = args.tempo
    path_sensors = args.list_sensors

    # Imprime os valroes dos argumentos
    print(f'Será executado a cada {tempo} minutos')
    print('Arquivo', path_sensors)

    run_time_loop = 'every ' + str(tempo) + ' minutes'

    # Getting a task instance
    task = app.session[run_app]

    # Setting batch run
    task.start_cond = run_time_loop

    app.run()


if __name__ == '__main__':
    PATH_FOLDER_OUT = ""
    PATH_FILE_ID_SENSORS = "./data/sensors.csv"

    initial_config('config.ini')
    main()
