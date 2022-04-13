import os, sys
ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
BASE_DIR = os.path.join(ROOT_DIR, '..' )
sys.path.append(os.path.abspath(BASE_DIR))
import pandas as pd
from sqlalchemy import create_engine, text
import configparser
from database_helpers.helpers import insert_to_db, check_for_data_existance, set_time_period, day_rounder
import logging
from datetime import datetime, timedelta
import re

logging.basicConfig()
logging.getLogger('sqlalchemy.engine').setLevel(logging.ERROR)

SQL_DIR = BASE_DIR+'/sql'

config = configparser.ConfigParser()
config.read(BASE_DIR+'/config.ini')

PanDA_engine = create_engine(config['PanDA DB']['sqlalchemy_engine_str'], echo=True, future=True)


def data_format_extractor(row):
    format = row.split('.')[4]
    return re.sub('\d', '-', format).strip('-')

def prod_step_extractor(row):
    return re.sub('\d', '-', row.split('.')[3]).strip('-')

def extend_dataset_parameters(df):
    df['project'] = df['datasetname'].str.split(':').str.get(0)
    df['prod_step'] = df['datasetname'].apply(prod_step_extractor)
    df['data_format_full'] = df['datasetname'].apply(data_format_extractor)
    df['data_format'] = df['data_format_full'].str.split('_').str.get(0)
    df['data_format_desc'] = df['data_format_full'].str.split('_').str.get(1)
    df.drop('data_format_full', axis=1, inplace=True)
    return df


def save_historical_popularity_to_db(predefined_date = False):

    from_date, to_date = set_time_period(predefined_date, n_hours=24)

    if not check_for_data_existance('dataset_historical_popularity', from_date, delete=True):
        panda_connection = PanDA_engine.connect()
        query = text(open(SQL_DIR+'/PanDA/data_popularity.sql').read())
        from_date = day_rounder(datetime.strptime(from_date, "%Y-%m-%d %H:%M:%S"))
        df = pd.read_sql_query(query, panda_connection, parse_dates={'datetime': '%Y-%m-%d'},
                               params={'from_date': from_date,
                                       'hours': 24})
        df['project'] = df['datasetname'].str.split(':').str.get(0)
        df['prod_step'] = df['datasetname'].apply(prod_step_extractor)
        df['data_format_full'] = df['datasetname'].apply(data_format_extractor)
        df['data_format'] = df['data_format_full'].str.split('_').str.get(0)
        df['data_format_desc'] = df['data_format_full'].str.split('_').str.get(1)
        df.drop('data_format_full',axis=1,inplace=True)
        insert_to_db(df, 'dataset_historical_popularity')
    else:
        pass


def production_save_historical_popularity_to_db(predefined_date = False):

    from_date, to_date = set_time_period(predefined_date, n_hours=24)

    if not check_for_data_existance('dataset_historical_popularity_prod', from_date, delete=True):
        panda_connection = PanDA_engine.connect()
        query = text(open(SQL_DIR+'/PanDA/data_popularity_prod.sql').read())
        from_date = day_rounder(datetime.strptime(from_date, "%Y-%m-%d %H:%M:%S"))
        df = pd.read_sql_query(query, panda_connection, parse_dates={'datetime': '%Y-%m-%d'},
                               params={'from_date': from_date,
                                       'hours': 24})
        df['project'] = df['datasetname'].str.split(':').str.get(0)
        df['prod_step'] = df['datasetname'].apply(prod_step_extractor)
        df['data_format_full'] = df['datasetname'].apply(data_format_extractor)
        df['data_format'] = df['data_format_full'].str.split('_').str.get(0)
        df['data_format_desc'] = df['data_format_full'].str.split('_').str.get(1)
        df.drop('data_format_full',axis=1,inplace=True)
        insert_to_db(df, 'dataset_historical_popularity_prod')
    else:
        pass


def save_processed_datasets_to_db(predefined_date = False):

    from_date, to_date = set_time_period(predefined_date, n_hours=24)

    if not check_for_data_existance('datasets_processed_history_agg', from_date, delete=True):
        panda_connection = PanDA_engine.connect()
        query = text(open(SQL_DIR+'/PanDA/datasets_processed_history_agg.sql').read())
        from_date = day_rounder(datetime.strptime(from_date, "%Y-%m-%d %H:%M:%S"))
        df = pd.read_sql_query(query, panda_connection, parse_dates={'datetime': '%Y-%m-%d'},
                                                       params={'from_date': from_date,
                                                               'hours': 24})
        insert_to_db(df, 'datasets_processed_history_agg')
    else:
        pass


def save_dataset_task_user_to_db(predefined_date = False):

    from_date, to_date = set_time_period(predefined_date, n_hours=24)

    if not check_for_data_existance('datasets_tasks_users', from_date, delete=True):
        panda_connection = PanDA_engine.connect()
        query = text(open(SQL_DIR+'/PanDA/datasets_tasks_users.sql').read())
        df = pd.read_sql_query(query, panda_connection, parse_dates={'datetime': '%Y-%m-%d'},
                               params={'from_date': from_date})
        insert_to_db(df, 'datasets_tasks_users')
    else:
        pass


def collect_data_for_period():
    start_date = datetime(2022, 2, 10, 1, 0, 0)
    end_date = datetime(2022, 3, 10, 1, 0, 0)
    delta_day = timedelta(days=1)

    while start_date <= end_date:
        print(start_date)
        try:
            production_save_historical_popularity_to_db(datetime.strftime(start_date,"%Y-%m-%d %H:%M:%S"))
            print(f'{start_date} has been written to the database')
            start_date += delta_day
            print('Data has been written!')
        except:
            print(f'The process has been failed at date {start_date}. Please restart the app.')


# save_dataset_task_user_to_db('2022-04-06 00:40:00')