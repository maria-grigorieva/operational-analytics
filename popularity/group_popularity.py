import os, sys
ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
BASE_DIR = os.path.join(ROOT_DIR, '..' )
sys.path.append(os.path.abspath(BASE_DIR))
import pandas as pd
from sqlalchemy import create_engine, text
import configparser
from database_helpers.helpers import write_to_postgreSQL, check_postgreSQL, delete_from_pgsql, localized_now
from datetime import datetime

import logging

logging.basicConfig()

SQL_DIR = BASE_DIR+'/sql'

config = configparser.ConfigParser()
config.read(BASE_DIR+'/config.ini')

PanDA_engine = create_engine(config['PanDA DB']['sqlalchemy_engine_str'], echo=False, max_identifier_length=128)
PostgreSQL_engine = create_engine(config['PostgreSQL']['sqlalchemy_engine_str'], echo=False)

def group_popularity_to_db(predefined_date = False):

    from_date = datetime.strftime(localized_now(), "%Y-%m-%d %H:%M:%S") if not predefined_date else str(predefined_date)

    if check_postgreSQL('group_popularity', from_date, accuracy='day', datetime_col_name='datetime') == True:
        delete_from_pgsql('group_popularity', from_date, accuracy='day', datetime_col_name='datetime')

    panda_connection = PanDA_engine.connect()
    query = text(open(SQL_DIR+'/PanDA/group_popularity.sql').read())
    df = pd.read_sql_query(query,
                           panda_connection,
                           parse_dates={'datetime': '%Y-%m-%d'},
                           params={'from_date': from_date})
    panda_connection.close()
    write_to_postgreSQL(df, 'group_popularity')


def group_popularity_daily_to_db(predefined_date = False):

    from_date = datetime.strftime(localized_now(), "%Y-%m-%d %H:%M:%S") if not predefined_date else str(predefined_date)

    if check_postgreSQL('group_popularity_daily', from_date, accuracy='day', datetime_col_name='datetime') == True:
        delete_from_pgsql('group_popularity_daily', from_date, accuracy='day', datetime_col_name='datetime')

    panda_connection = PanDA_engine.connect()
    query = text(open(SQL_DIR+'/PanDA/group_popularity_daily.sql').read())
    df = pd.read_sql_query(query,
                           panda_connection,
                           parse_dates={'datetime': '%Y-%m-%d'},
                           params={'from_date': from_date})
    panda_connection.close()
    write_to_postgreSQL(df, 'group_popularity_daily')