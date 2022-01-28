import os, sys
ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
BASE_DIR = os.path.join(ROOT_DIR, '..' )
sys.path.append(os.path.abspath(BASE_DIR))
import pandas as pd
from sqlalchemy import create_engine, text
import configparser
from database_helpers.helpers import insert_to_db, if_data_exists, set_start_end_dates, set_time_period, hour_rounder, day_rounder
import logging
from datetime import datetime, timedelta

logging.basicConfig()
logging.getLogger('sqlalchemy.engine').setLevel(logging.ERROR)

SQL_DIR = BASE_DIR+'/sql'

config = configparser.ConfigParser()
config.read(BASE_DIR+'/config.ini')

PanDA_engine = create_engine(config['PanDA DB']['sqlalchemy_engine_str'], echo=True, future=True)

def save_popularity_to_db(predefined_date = False):
    #from_date, to_date = set_start_end_dates(predefined_date)
    from_date, to_date = set_time_period(predefined_date, n_hours=24)

    if not if_data_exists('datasets_info', from_date):
        panda_connection = PanDA_engine.connect()
        query = text(open(SQL_DIR+'/PanDA/daily_datasets.sql').read())
        from_date = day_rounder(datetime.strptime(from_date, "%Y-%m-%d %H:%M:%S"))
        df = pd.read_sql_query(query, panda_connection, parse_dates={'datetime': '%Y-%m-%d'},
                               params={'from_date': from_date})
        # curr_date = df['datetime'].unique()[0]
        insert_to_db(df, 'datasets_info', from_date)
    else:
        pass
#
# save_popularity_to_db('2022-01-17 00:30:00')

def save_dataset_task_user_to_db(predefined_date = False):

    from_date, to_date = set_time_period(predefined_date, n_hours=24)

    if not if_data_exists('datasets_tasks_users', from_date):
        panda_connection = PanDA_engine.connect()
        query = text(open(SQL_DIR+'/PanDA/datasets_tasks_users.sql').read())
        from_date = day_rounder(datetime.strptime(from_date, "%Y-%m-%d %H:%M:%S"))
        df = pd.read_sql_query(query, panda_connection, parse_dates={'datetime': '%Y-%m-%d'},
                               params={'from_date': from_date})
        # curr_date = df['datetime'].unique()[0]
        insert_to_db(df, 'datasets_tasks_users', from_date)
    else:
        pass


# save_dataset_task_user_to_db('2022-01-24 00:00:00')