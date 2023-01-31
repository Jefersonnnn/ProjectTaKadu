import datetime
import os
import time
import zipfile
import csv
import configparser
from pathlib import Path

import psycopg2
import pandas as pd

from redmail import EmailHandler, EmailSender

from rocketry import Rocketry
from rocketry.conds import retry,  every

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

from logging import Handler


class EmaillNotifyHandler(Handler):

    def __init__(self, level:int=logging.NOTSET, email=None):
        Handler.__init__(self, level)

        if not email:
            raise Exception("attr 'email' não informado!")
        self._email = email

    def emit(self,record:logging.LogRecord):
        self._email.send(
            subject=f'Takadu Service - Log Record: {record.levelname}',
            text=f"Logging level: { record.levelname }\nMessage: { record.message }",
            html=f"<ul><li>Logging level: { record.levelname }</li><li>Message: { record.message }</li></ul>",
            attachments={
                'app.log': Path("app.log")
            }
        )


def initial_config(config_file_path):
    try:
        config = configparser.ConfigParser()
        config.read(config_file_path)

        os.environ['PG_DATABASE'] = os.environ.get('PG_DATABASE', config['postgresql']['database'])
        os.environ['PG_USER'] = os.environ.get('PG_USER', config['postgresql']['user'])
        os.environ['PG_PASSWORD'] = os.environ.get('PG_PASSWORD', config['postgresql']['password'])
        os.environ['PG_HOST'] = os.environ.get('PG_HOST', config['postgresql']['host'])
        os.environ['PG_PORT'] = os.environ.get('PG_PORT', config['postgresql']['port'])

        error_notify_email_username =  os.environ.get('EMAIL_USERNAME', config['notify_outlook']['username'])
        error_notify_email_password =  os.environ.get('EMAIL_PASSWORD', config['notify_outlook']['password'])


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


def load_csv_list_sensors(path_file:str="./data/caj_sensors.csv", delimiter:str=";") -> list:
    df_sensors = pd.read_csv(path_file, delimiter=delimiter)
    if not "Sensor" in df_sensors.columns:
        raise Exception(f"'Sensor' column not found in file: {path_file}")
    return df_sensors["Sensor"].tolist()


def _fetchall_to_csv(rows, header, csv_file):
    with open(csv_file, 'w', newline="") as f:
        writer = csv.writer(f, delimiter=";")
        writer.writerow(header)
        writer.writerows(rows)


def load_data_from_db(conn, table_name, ids_sensors:list) -> tuple:

    if conn and table_name and ids_sensors and isinstance(ids_sensors, list):
        logger.info("Searching for new data (24hours) in the database")
        ids_sensors = ','.join(str(x) for x in ids_sensors)

        try:
            with conn.cursor() as cursor:
                query = f"""
                        SELECT
                            m.pointconfig_id Id
                            ,m.measure_datetime Timestamp
                            ,m.calculated_measure Value
                        
                        FROM
                        public.measure m
                        
                        WHERE
                            m.measure_datetime between NOW() - interval '24 hours' and NOW()
                            AND m.pointconfig_id IN ({ids_sensors})
                            
                            ORDER BY measure_datetime DESC
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


def save_list_to_csv_and_zip(data_list:list, header:list, data_source_name="TELELOG", destination_folder=".\\toftp", zip_file=True):
    """
    Save a list to .csv file and compress it in a .zip file.
    The .csv file will have a dynamic name with the following format: "DataSourceName_YYYYMMDDHH24MMSS.csv".
    :param data_list: lista retornada da função cursor.fetchall() "Lista com tuplas"
    :param data_source_name: Name of the data source that will be used as a prefix in the .csv file name
    :param destination_folder: Folder where the .zip file will be saved
    :param zip_file:
    """
    if not all(isinstance(i, tuple) for i in data_list):
        raise Exception("data_list not is a instance of 'list'")

    if not os.path.exists(destination_folder):
        os.makedirs(destination_folder)

    now = datetime.datetime.now()
    csv_file = f"{data_source_name}_{now.strftime('%Y%m%d%H%M%S')}.csv"
    path_to_save_csv = os.path.join(destination_folder, csv_file)
    _fetchall_to_csv(rows=data_list, header=header, csv_file=path_to_save_csv)
    time.sleep(5)

    if zip_file:
        if not os.path.isfile(path_to_save_csv):
            logger.error("File .csv not found.")
            raise Exception("File .csv not found.")

        zip_file_name = f"{csv_file}.zip"
        path_to_save_zip = os.path.join(destination_folder, zip_file_name)
        with zipfile.ZipFile(path_to_save_zip, 'w', zipfile.ZIP_DEFLATED) as myzip:
            myzip.write(path_to_save_csv, arcname=csv_file)

        os.remove(path_to_save_csv)

@app.task(every('5 minute') | retry(3))
async def run_app():
    logger.info("Iniciando carregamento dos dados...")
    start_time = datetime.datetime.now()

    list_ids_sensors = load_csv_list_sensors()
    conn = connect_to_postgres()
    data, header = load_data_from_db(conn, "measure", ids_sensors=list_ids_sensors)
    save_list_to_csv_and_zip(data_list=data, header=header)

    end_time = datetime.datetime.now()

    exec_time = round((end_time - start_time).total_seconds(),2)
    logger.info(f"Tempo para carregar os dados: {exec_time} segundos")

def main():
    print("Serviço TaKaDu Load Data iniciado...")
    app.run()


if __name__ == '__main__':
    initial_config('config.ini')
    main()
