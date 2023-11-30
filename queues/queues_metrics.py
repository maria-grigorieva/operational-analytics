import os, sys
ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
BASE_DIR = os.path.join(ROOT_DIR, '..' )
sys.path.append(os.path.abspath(BASE_DIR))
import cx_Oracle
import cric
import pandas as pd
from sqlalchemy import create_engine, text
import configparser
from cric.cric_json_api import enhance_queues, extract_architecture_specs
from database_helpers.helpers import insert_to_db, check_for_data_existance, set_time_period, localized_now, check_postgreSQL, write_to_postgreSQL, delete_from_pgsql
from datetime import datetime, timedelta

import logging

logging.basicConfig()
logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO)

SQL_DIR = BASE_DIR+'/sql'

config = configparser.ConfigParser()
config.read(BASE_DIR+'/config.ini')

cx_Oracle.init_oracle_client(lib_dir=config['PanDA DB']['client_path'])

PanDA_engine = create_engine(config['PanDA DB']['sqlalchemy_engine_str'], echo=True, max_identifier_length=128)
PostgreSQL_engine = create_engine(config['PostgreSQL']['sqlalchemy_engine_str'], echo=False)


def queues_workload(predefined_date=False, queues='actual'):

    from_date = datetime.strftime(localized_now(), "%Y-%m-%d %H:%M:%S") \
        if not predefined_date else str(predefined_date)

    if check_postgreSQL('queues_workload', from_date, accuracy='hour', datetime_col_name='datetime') == True:
        delete_from_pgsql('queues_workload', from_date, accuracy='hour', datetime_col_name='datetime')

    engine = PanDA_engine.connect()
    query = text(open(SQL_DIR + f'/PanDA/queues_workload.sql').read())
    df = pd.read_sql_query(query, con=engine, parse_dates={'datetime': '%Y-%m-%d %H:%M:%S'},
                           params={'from_date': from_date})
    engine.close()
    if queues == 'actual':
        try:
            from_cric = cric.cric_json_api.enhance_queues()
        except Exception as e:
            PostgreSQL_connection = PostgreSQL_engine.connect()
            query = text('SELECT queue,cloud,site,resource_type,'
                         'tier_level, status, state, nodes,'
                         'corepower, corecount, region, transferring_limit '
                         'FROM cric_resources WHERE '
                         'datetime = (SELECT max(datetime) FROM cric_resources)'
                         'GROUP by queue,cloud,site,resource_type,'
                         'tier_level, status, state, nodes,'
                         'corepower, corecount, region, transferring_limit')
            from_cric = pd.read_sql_query(query, PostgreSQL_connection)
    elif queues == 'db':
        PostgreSQL_connection = PostgreSQL_engine.connect()
        query = text('SELECT queue,cloud,site,resource_type,'
                     'tier_level, status, state, nodes,'
                     'corepower, corecount, region, transferring_limit '
                     'FROM cric_resources WHERE '
                     'datetime = (SELECT max(datetime) FROM cric_resources)'
                     'GROUP by queue,cloud,site,resource_type,'
                     'tier_level, status, state, nodes,'
                     'corepower, corecount, region, transferring_limit')
        from_cric = pd.read_sql_query(query, PostgreSQL_connection)

    from_cric.rename(columns={'resource_type':'cric_resource_type'},inplace=True)
    result = pd.merge(df, from_cric, left_on='queue', right_on='queue')
    write_to_postgreSQL(result, 'queues_workload')


def jobs_statuslog_extended(predefined_date=False, queues='actual'):

    from_date = datetime.strftime(localized_now(), "%Y-%m-%d %H:%M:%S") \
        if not predefined_date else str(predefined_date)

    if check_postgreSQL('jobs_statuslog_extended', from_date, accuracy='hour', datetime_col_name='end_time') == True:
        delete_from_pgsql('jobs_statuslog_extended', from_date, accuracy='hour', datetime_col_name='end_time')

    engine = PanDA_engine.connect()
    query = text(open(SQL_DIR + f'/PanDA/jobs_statuslog_extended.sql').read())
    df = pd.read_sql_query(query, con=engine, parse_dates={'end_time': '%Y-%m-%d %H:%M:%S', 'start_time': '%Y-%m-%d %H:%M:%S'},
                           params={'from_date': from_date})
    engine.close()
    if queues == 'actual':
        try:
            from_cric = cric.cric_json_api.enhance_queues(all=True)
        except Exception as e:
            PostgreSQL_connection = PostgreSQL_engine.connect()
            query = text('SELECT queue,cloud,site,resource_type,'
                         'tier_level, nodes,'
                         'corepower, corecount, region, transferring_limit '
                         'FROM cric_resources WHERE '
                         'datetime = (SELECT max(datetime) FROM cric_resources)'
                         'GROUP by queue,cloud,site,resource_type,'
                         'tier_level, nodes,'
                         'corepower, corecount, region, transferring_limit')
            from_cric = pd.read_sql_query(query, PostgreSQL_connection)
    elif queues == 'db':
        PostgreSQL_connection = PostgreSQL_engine.connect()
        query = text('SELECT queue,cloud,site,resource_type,'
                     'tier_level, nodes,'
                     'corepower, corecount, region, transferring_limit '
                     'FROM cric_resources WHERE '
                     'datetime = (SELECT max(datetime) FROM cric_resources)'
                     'GROUP by queue,cloud,site,resource_type,'
                     'tier_level, nodes,'
                     'corepower, corecount, region, transferring_limit')
        from_cric = pd.read_sql_query(query, PostgreSQL_connection)

    from_cric.rename(columns={'resource_type':'cric_resource_type'},inplace=True)
    from_cric.drop(['nodes','corepower','corecount','transferring_limit'],axis=1,inplace=True)
    result = pd.merge(df, from_cric, left_on='queue', right_on='queue')

    specs = extract_architecture_specs()
    res = pd.merge(result, specs, left_on='queue', right_on='queue')
    write_to_postgreSQL(res, 'jobs_statuslog_extended')