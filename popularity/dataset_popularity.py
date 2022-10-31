import os, sys
ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
BASE_DIR = os.path.join(ROOT_DIR, '..' )
sys.path.append(os.path.abspath(BASE_DIR))
import cx_Oracle
import pandas as pd
from sqlalchemy import create_engine, text
import configparser
from database_helpers.helpers import write_to_postgreSQL, check_for_data_existance, set_time_period, localized_now
from datetime import datetime, timedelta

import logging

logging.basicConfig()

SQL_DIR = BASE_DIR+'/sql'

config = configparser.ConfigParser()
config.read(BASE_DIR+'/config.ini')

PanDA_engine = create_engine(config['PanDA DB']['sqlalchemy_engine_str'], echo=False, max_identifier_length=128)
PostgreSQL_engine = create_engine(config['PostgreSQL']['sqlalchemy_engine_str'], echo=False)


def datasets_popularity_to_db(predefined_date = False):

    from_date = datetime.strftime(localized_now(), "%Y-%m-%d %H:%M:%S") if not predefined_date else str(predefined_date)

    if not check_for_data_existance('datasets_popularity', from_date, delete=True):
        panda_connection = PanDA_engine.connect()
        query = text(open(SQL_DIR+'/PanDA/datasets_popularity.sql').read())
        df = pd.read_sql_query(query,
                               panda_connection,
                               parse_dates={'datetime': '%Y-%m-%d'},
                               params={'from_date': from_date})
        panda_connection.close()
        write_to_postgreSQL(df, 'datasets_popularity')
    else:
        pass


def aggregation_week(predefined_date = False):

    from_date = datetime.strftime(localized_now(), "%Y-%m-%d %H:%M:%S") if not predefined_date else str(predefined_date)

    conn = PostgreSQL_engine.connect()
    query = text(open(SQL_DIR+'/postgreSQL/agg_week_datasets_popularity.sql').read())
    df = pd.read_sql_query(query, conn,
                           parse_dates={'datetime': '%Y-%m-%d'},
                           params={'from_date': from_date}
                           )

    for row in df.to_dict('records'):
        q = text(open(SQL_DIR + '/postgreSQL/agg_week_datasets_popularity_upsert.sql').read())
        conn.execute(q, row, params=row)

    conn.close()


def aggregation_all():
    PostgreSQL_connection = PostgreSQL_engine.connect()
    query = text('SELECT '
                 'datasetname,'
                 'input_format_short,'
                 'input_format_desc,'
                 'input_project,'
                 'prod_step,'
                 'process_desc,'
                 'n_dataset,'
                 'tid,'
                 'process_tags,'
                 'sum(n_tasks) as n_tasks,'
                 'min(datetime) as start_usage,'
                 'max(datetime) as end_usage,'
                 'DATE_PART(\'day\', max(datetime) - min(datetime)) as usage_period '
                 'FROM datasets_popularity '
                 'GROUP BY datasetname,'
                 'input_format_short,'
                 'input_format_desc,'
                 'input_project,'
                 'prod_step,'
                 'process_desc,'
                 'n_dataset,'
                 'tid,'
                 'process_tags')
    df = pd.read_sql_query(query, PostgreSQL_connection)
    PostgreSQL_connection.close()
    write_to_postgreSQL(df, 'aggregated_datasets_popularity')




