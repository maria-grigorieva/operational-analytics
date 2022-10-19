import os, sys
ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
BASE_DIR = os.path.join(ROOT_DIR, '..' )
sys.path.append(os.path.abspath(BASE_DIR))
import configparser
SQL_DIR = BASE_DIR+'/sql'
from sqlalchemy import create_engine, text
from database_helpers.helpers import insert_to_db, check_for_data_existance, set_time_period,day_rounder,nulls_ints_to_zeroes
import pandas as pd
from datetime import datetime, timedelta


config = configparser.ConfigParser()
config.read(BASE_DIR+'/config.ini')

PostgreSQL_engine = create_engine(config['PostgreSQL']['sqlalchemy_engine_str'], echo=False)

def queues_rse_cric(predefined_date = False):

    from_date, to_date = set_time_period(predefined_date, n_hours=24)

    if not check_for_data_existance('resource_snapshot', from_date, 'day', True):
        query = SQL_DIR + '/postgreSQL/merge_queue_rse_cric.sql'
        postgres_connection = PostgreSQL_engine.connect()
        result = pd.read_sql_query(text(open(query).read()),postgres_connection,parse_dates={'datetime': '%Y-%m-%d'},
                                   params={'from_date': from_date})
        postgres_connection.close()
        insert_to_db(result, 'resource_snapshot')
    else:
        pass


def dataset_cric_replicas(predefined_date = False):

    from_date, to_date = set_time_period(predefined_date, n_hours=24)

    if not check_for_data_existance('datasets_daily_snapshots', from_date, 'day', True):

        postgres_connection = PostgreSQL_engine.connect()
        merged = SQL_DIR + '/postgreSQL/merge_datasets_cric_replicas_v1.sql'
        merged_df = pd.read_sql_query(text(open(merged).read()), postgres_connection,
                                      parse_dates={'datetime': '%Y-%m-%d'},
                                      params={'from_date': from_date})
        postgres_connection.close()
        merged_df[['corecount']].fillna(0, inplace=True)

        insert_to_db(merged_df, 'datasets_daily_snapshots')

    else:
        pass
