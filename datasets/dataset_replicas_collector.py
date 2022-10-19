import os, sys
ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
BASE_DIR = os.path.join(ROOT_DIR, '..' )
# sys.path.append(os.path.abspath(BASE_DIR))
import cx_Oracle
import cric
import pandas as pd
from sqlalchemy import create_engine, text, inspect
from sqlalchemy.dialects.postgresql import insert
import configparser
from rucio_api import dataset_info
from database_helpers.helpers import insert_to_db, check_for_data_existance, set_time_period,day_rounder
from datetime import datetime, timedelta

SQL_DIR = BASE_DIR+'/sql'

config = configparser.ConfigParser()
config.read(BASE_DIR+'/config.ini')

PostgreSQL_engine = create_engine(config['PostgreSQL']['sqlalchemy_engine_str'], echo=False, pool_size=10, max_overflow=20)


def dataset_replicas_to_db(predefined_date = False):

    from_date, to_date = set_time_period(predefined_date, n_hours=24)

    if not check_for_data_existance('datasets_replicas', from_date, delete=True):
        postgres_connection = PostgreSQL_engine.connect()
        query = text(f'select distinct datasetname, datetime from datasets_tasks_users where '
                     f'datetime >= DATE_TRUNC(\'day\', TIMESTAMP \'{from_date}\')'
                     f'and datetime < DATE_TRUNC(\'day\', TIMESTAMP \'{from_date}\' + INTERVAL \'1day\')')

        df = pd.read_sql_query(query, postgres_connection, parse_dates={'datetime': '%Y-%m-%d'},
                               params={'from_date': from_date})
        datasets = df['datasetname'].unique()
        print(datasets)
        print(f'Number of rows: {df.shape[0]}, Number of datasets = {len(datasets)}')
        datasets_replicas = [dataset_info.get_dataset_info(d) for d in datasets]
        print('Concatenation...')
        datasets_replicas = pd.concat(datasets_replicas)
        datasets_replicas['datetime'] = day_rounder(datetime.strptime(from_date, "%Y-%m-%d %H:%M:%S"))
        datasets_replicas.drop(['rse_id','id','child_rule_id','eol_at','error','grouping',
                                'ignore_account_limit','ignore_availability','locked',
                                'locks_ok_cnt','locks_replicating_cnt','locks_stuck_cnt',
                                'meta','notification','priority','purge_replicas',
                                'source_replica_expression','split_container','stuck_at',
                                'subscription_id','weight','is_new','obsolete','suppressed',
                                'activity','expires_at','rse_expression','rule_id','comments',
                                'account', 'copies', 'provenance', 'phys_group', 'lumiblocknr',
                                'deleted_at'], axis=1, inplace=True)
        datasets_replicas.drop_duplicates(keep=False, inplace=True)
        print('Saving to the database...')
        insert_to_db(datasets_replicas, 'datasets_replicas')
    else:
        pass


def collect_dataset(name):
    panda = create_engine(config['PanDA DB']['sqlalchemy_engine_str'], echo=False, future=True)
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

#collect_dataset('mc20_13TeV:mc20_13TeV.700019.Sh_228_mmgamma_pty7_ptV90.deriv.DAOD_EGAM4.e7947_s3681_r13167_r13146_p4940_tid27820266_00')
# dataset_replicas_to_db('2022-04-11 05:00:00')