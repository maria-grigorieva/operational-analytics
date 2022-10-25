import os, sys
ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
BASE_DIR = os.path.join(ROOT_DIR, '..' )
sys.path.append(os.path.abspath(BASE_DIR))
import cx_Oracle
import cric
import pandas as pd
from sqlalchemy import create_engine, text
import configparser
from cric.cric_json_api import enhance_queues
from database_helpers.helpers import insert_to_db, check_for_data_existance, set_time_period, localized_now
from datetime import datetime, timedelta

import logging

logging.basicConfig()
logging.getLogger('sqlalchemy.engine').setLevel(logging.ERROR)

SQL_DIR = BASE_DIR+'/sql'

config = configparser.ConfigParser()
config.read(BASE_DIR+'/config.ini')

# cx_Oracle.init_oracle_client(lib_dir=config['PanDA DB']['client_path'])

PanDA_engine = create_engine(config['PanDA DB']['sqlalchemy_engine_str'], echo=False, max_identifier_length=128)
PostgreSQL_engine = create_engine(config['PostgreSQL']['sqlalchemy_engine_str'], echo=False)


def group_popularity_to_db(predefined_date = False):

    from_date = datetime.strftime(localized_now(), "%Y-%m-%d %H:%M:%S") \
        if not predefined_date else str(predefined_date)

    if not check_for_data_existance('group_popularity', from_date, delete=True):
        panda_connection = PanDA_engine.connect()
        query = text(open(SQL_DIR+'/PanDA/group_popularity.sql').read())
        df = pd.read_sql_query(query,
                               panda_connection,
                               parse_dates={'datetime': '%Y-%m-%d'},
                               params={'from_date': from_date})
        panda_connection.close()
        insert_to_db(df, 'group_popularity')
    else:
        pass