import os, sys
ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
BASE_DIR = os.path.join(ROOT_DIR, '..' )
sys.path.append(os.path.abspath(BASE_DIR))
import cx_Oracle
import cric
import pandas as pd
from sqlalchemy import create_engine, text, inspect
from sqlalchemy.dialects.postgresql import insert
import configparser
from rucio_api import dataset_info
from database_helpers.helpers import insert_to_db, set_start_end_dates, if_data_exists, set_time_period,day_rounder
from datetime import datetime, timedelta

SQL_DIR = BASE_DIR+'/sql'

config = configparser.ConfigParser()
config.read(BASE_DIR+'/config.ini')

PostgreSQL_engine = create_engine(config['PostgreSQL']['sqlalchemy_engine_str'], echo=True, pool_size=10, max_overflow=20)


def dataset_replicas_to_db(predefined_date = False):
    #from_date, to_date = set_start_end_dates(predefined_date)
    from_date, to_date = set_time_period(predefined_date, n_hours=24)

    if not if_data_exists('dataset_replicas', from_date):

        postgres_connection = PostgreSQL_engine.connect()
        from_date = day_rounder(datetime.strptime(from_date, "%Y-%m-%d %H:%M:%S"))
        query = text(f'select distinct datasetname, datetime from datasets_info where datetime = DATE_TRUNC(\'day\', TIMESTAMP \'{from_date}\')')

        df = pd.read_sql_query(query, postgres_connection, parse_dates={'datetime': '%Y-%m-%d'},
                               params={'from_date': from_date})
        print(f'Number of rows: {df.shape[0]}')
        datasets = df['datasetname'].unique()
        print(f'Number of datasets = {len(datasets)}')
        #curr_date = df['datetime'].unique()[0]
        datasets_replicas = [dataset_info.get_dataset_info(d) for d in datasets]
        print('Concatenation...')
        datasets_replicas = pd.concat(datasets_replicas)
        #datasets_replicas['datasetname'] = datasets_replicas['scope'].astype(str).str.cat(datasets_replicas['name'], sep=':')
        datasets_replicas['datetime'] = from_date
        print('Saving to the database...')
        insert_to_db(datasets_replicas, 'dataset_replicas', from_date)
    else:
        pass


def collect_dataset(name):
    panda = create_engine(config['PanDA DB']['sqlalchemy_engine_str_alt'], echo=True, future=True)
    panda_connection = panda.connect()
    postgres_connection = PostgreSQL_engine.connect()
    query = text(open(SQL_DIR+'/PanDA/dataset_popularity_single.sql').read())
    #query.bindparams(ds_name=name)
    df = pd.read_sql_query(query, panda_connection, parse_dates={'datetime':'%Y-%m-%d'}, params={'ds_name':name})
    curr_date = df['datetime'].unique()[0]
    datasets_info = dataset_info.get_dataset_info(name)
    datasets_info['datasetname'] = datasets_info['scope'].astype(str).str.cat(datasets_info['name'], sep=':')
    result = pd.merge(datasets_info, df, left_on='datasetname', right_on='datasetname')

    # The recommended way to check for existence
    if inspect(PostgreSQL_engine).has_table('datasets'):

        with postgres_connection.begin():
            if postgres_connection.execute(text(f'SELECT distinct datetime from datasets '
                                                f'where datetime = \'{curr_date}\' '
                                                f'and datasetname = \'{name}\'')).first() is not None:
                # delete those rows that we are going to "upsert"
                postgres_connection.execute(text(f'delete from datasets '
                                                 f'where datetime = \'{curr_date}\''
                                                 f'and datasetname = \'{name}\''))

    # insert changed rows
    result.to_sql('datasets', postgres_connection,
                  if_exists='append',
                  method='multi',
                  index=False)

#collect_dataset('data16_13TeV:data16_13TeV.00299584.physics_Main.deriv.DAOD_TOPQ1.r9264_p3083_p4513_tid25513236_00')
#collect()
dataset_replicas_to_db('2022-01-17 01:00:00')