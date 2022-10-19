import os, sys
ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
BASE_DIR = os.path.join(ROOT_DIR, '..' )
sys.path.append(os.path.abspath(BASE_DIR))
import cx_Oracle
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

# cx_Oracle.init_oracle_client(lib_dir=r"/usr/lib/oracle/19.13/client64/lib")

config = configparser.ConfigParser()
config.read(BASE_DIR+'/config.ini')

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

#
# start_date = datetime(2022, 1, 1, 1, 0, 0)
# end_date = datetime(2022, 4, 20, 1, 00, 0)
# delta_day = timedelta(days=1)
#
# while start_date <= end_date:
#     print(start_date)
#     long_tasks_to_db(start_date)
#     start_date += delta_day
# #
# long_tasks_to_db('2022-04-22 01:00:00')