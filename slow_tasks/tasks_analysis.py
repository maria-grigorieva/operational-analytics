import os, sys
ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
BASE_DIR = os.path.join(ROOT_DIR, '..' )
sys.path.append(os.path.abspath(BASE_DIR))
import oracledb
import cric
import pandas as pd
from sqlalchemy import create_engine, text
import configparser
from database_helpers.helpers import insert_to_db, check_for_data_existance, set_time_period
from datetime import datetime, timedelta

import logging

logging.basicConfig()
logging.getLogger('sqlalchemy.engine').setLevel(logging.ERROR)

SQL_DIR = BASE_DIR+'/sql'

config = configparser.ConfigParser()
config.read(BASE_DIR+'/config.ini')

oracle_mode = config['PanDA DB'].get('oracle_mode', 'thin').strip().lower()
client_path = config['PanDA DB'].get('client_path', '').strip()
if oracle_mode == 'thick':
    try:
        oracledb.init_oracle_client(lib_dir=client_path or None)
    except Exception as exc:
        logging.warning(f"Failed to initialize Oracle Thick mode client: {exc}")

PanDA_engine = create_engine(config['PanDA DB']['sqlalchemy_engine_str'], echo=True)
PostgreSQL_engine = create_engine(config['PostgreSQL']['sqlalchemy_engine_str'], echo=True)


def long_tasks_to_db(predefined_date = False):

    from_date = datetime.strftime(datetime.now(), "%Y-%m-%d %H:%M:%S") if not predefined_date else str(predefined_date)

    if not check_for_data_existance('long_tasks', from_date, delete=True):
        panda_connection = PanDA_engine.connect()
        query = text(open(SQL_DIR+'/PanDA/long_tasks.sql').read())
        df = pd.read_sql_query(query, panda_connection, parse_dates={'datetime': '%Y-%m-%d'},
                               params={'from_date': from_date})
        panda_connection.close()
        df.fillna(0, inplace=True)
        insert_to_db(df, 'long_tasks')
    else:
        pass
