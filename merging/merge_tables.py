import os, sys
ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
BASE_DIR = os.path.join(ROOT_DIR, '..' )
sys.path.append(os.path.abspath(BASE_DIR))
import configparser
SQL_DIR = BASE_DIR+'/sql'
from sqlalchemy import create_engine, text, inspect
from database_helpers.helpers import insert_to_db, if_data_exists, set_start_end_dates, set_time_period,day_rounder
import pandas as pd
import numpy as np
from datetime import datetime, timedelta


config = configparser.ConfigParser()
config.read(BASE_DIR+'/config.ini')

PostgreSQL_engine = create_engine(config['PostgreSQL']['sqlalchemy_engine_str'], echo=True)

def queues_rse_cric(predefined_date = False):

    # from_date, to_date = set_start_end_dates(predefined_date)
    from_date, to_date = set_time_period(predefined_date, n_hours=24)

    if not if_data_exists('resource_snapshot', from_date):
        query = SQL_DIR + '/postgreSQL/merge_queue_rse_cric.sql'
        postgres_connection = PostgreSQL_engine.connect()
        from_date = day_rounder(datetime.strptime(from_date, "%Y-%m-%d %H:%M:%S"))
        result = pd.read_sql_query(text(open(query).read()),
                                   postgres_connection,
                                   parse_dates={'datetime': '%Y-%m-%d'},
                                   params={'from_date': from_date})
        insert_to_db(result, 'resource_snapshot', from_date)
    else:
        pass


def dataset_cric_replicas(predefined_date = False):

    # from_date, to_date = set_start_end_dates(predefined_date)
    from_date, to_date = set_time_period(predefined_date, n_hours=24)

    if not if_data_exists('datasets_snapshot', from_date):

        postgres_connection = PostgreSQL_engine.connect()
        merged = SQL_DIR + '/postgreSQL/merge_datasets_cric_replicas.sql'
        from_date = day_rounder(datetime.strptime(from_date, "%Y-%m-%d %H:%M:%S"))
        merged_df = pd.read_sql_query(text(open(merged).read()), postgres_connection,
                                      parse_dates={'datetime': '%Y-%m-%d'},
                                      params={'from_date': from_date})
        insert_to_db(merged_df, 'datasets_snapshot', from_date)

    else:
        pass

dataset_cric_replicas('2022-01-17 03:00:00')