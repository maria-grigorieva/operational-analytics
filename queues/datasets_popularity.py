import os, sys
ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
BASE_DIR = os.path.join(ROOT_DIR, '..' )
sys.path.append(os.path.abspath(BASE_DIR))
import pandas as pd
from sqlalchemy import create_engine, text
import configparser
from database_helpers.helpers import insert_to_db, if_data_exists, set_start_end_dates
import logging

logging.basicConfig()
logging.getLogger('sqlalchemy.engine').setLevel(logging.ERROR)

SQL_DIR = BASE_DIR+'/sql'

config = configparser.ConfigParser()
config.read(BASE_DIR+'/config.ini')

PanDA_engine = create_engine(config['PanDA DB']['sqlalchemy_engine_str'], echo=True, future=True)

def save_popularity_to_db(predefined_date = False):
    from_date, to_date = set_start_end_dates(predefined_date)
    if not if_data_exists('datasets_info', from_date):
        panda_connection = PanDA_engine.connect()
        query = text(open(SQL_DIR+'/PanDA/daily_datasets.sql').read())
        df = pd.read_sql_query(query, panda_connection, parse_dates={'datetime': '%Y-%m-%d'},
                               params={'from_date': from_date,
                                       'to_date': to_date})
        curr_date = df['datetime'].unique()[0]
        insert_to_db(df, 'datasets_info', curr_date)
    else:
        pass

#save_popularity_to_db('2021-12-13')