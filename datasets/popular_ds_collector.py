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


SQL_DIR = BASE_DIR+'/sql'

cx_Oracle.init_oracle_client(lib_dir=r"/usr/lib/oracle/19.13/client64/lib")

config = configparser.ConfigParser()
config.read(BASE_DIR+'/config.ini')

PanDA_engine = create_engine(config['PanDA DB']['sqlalchemy_engine_str'], echo=True, future=True)
PostgreSQL_engine = create_engine(config['PostgreSQL']['sqlalchemy_engine_str'], echo=True)

def collect():
    panda_connection = PanDA_engine.connect()
    postgres_connection = PostgreSQL_engine.connect()
    query = text(open(SQL_DIR+'/PanDA/dataset_popularity.sql').read())
    df = pd.read_sql_query(query, panda_connection, parse_dates={'datetime':'%Y-%m-%d'})
    curr_date = df['datetime'].unique()[0]
    datasets = df['datasetname'].values
    datasets_info = []
    for d in datasets:
        datasets_info.append(dataset_info.get_dataset_info(d))
    datasets_info = pd.concat(datasets_info)
    datasets_info['datasetname'] = datasets_info['scope'].astype(str).str.cat(datasets_info['name'], sep=':')
    result = pd.merge(datasets_info, df, left_on='datasetname', right_on='datasetname')

    # The recommended way to check for existence
    if inspect(PostgreSQL_engine).has_table('datasets'):

        with postgres_connection.begin():
            if postgres_connection.execute(text(f'SELECT distinct datetime from datasets '
                                                f'where datetime = \'{curr_date}\'')).first() is not None:
                # delete those rows that we are going to "upsert"
                postgres_connection.execute(text(f'delete from datasets '
                                                 f'where datetime = \'{curr_date}\''))

    # insert changed rows
    result.to_sql('datasets', postgres_connection,
                  if_exists='append',
                  method='multi',
                  index=False)

def collect_dataset(name):
    panda_connection = PanDA_engine.connect()
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

collect_dataset('data16_13TeV:data16_13TeV.00299584.physics_Main.deriv.DAOD_TOPQ1.r9264_p3083_p4513_tid25513236_00')
