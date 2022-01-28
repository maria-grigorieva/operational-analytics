import os, sys
ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
BASE_DIR = os.path.join(ROOT_DIR, '..' )
sys.path.append(os.path.abspath(BASE_DIR))
import configparser
from sqlalchemy import create_engine, text, inspect
from datetime import datetime, timedelta
from google.cloud import bigquery
import pandas_gbq
import os
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="/opt/data_placement/conf/atlas-336515-9bbd95e3dadf.json"


SQL_DIR = BASE_DIR+'/sql'

config = configparser.ConfigParser()
config.read(BASE_DIR+'/config.ini')


def insert_to_db(df, table_name, curr_date, check_existance=False, date_format='%Y-%m-%d'):
    PostgreSQL_engine = create_engine(config['PostgreSQL']['sqlalchemy_engine_str'], echo=True)
    conn = PostgreSQL_engine.connect()

    curr_date = datetime.strptime(curr_date, date_format)

    if check_existance:
        # The recommended way to check for existence
        if inspect(PostgreSQL_engine).has_table(table_name):

            with conn.begin():
                if conn.execute(text(f'SELECT distinct datetime from {table_name} '
                                                    f'where datetime = \'{curr_date}\'')).first() is not None:
                    # delete those rows that we are going to "upsert"
                    conn.execute(text(f'delete from {table_name} '
                                                     f'where datetime = \'{curr_date}\''))
        df.to_sql(table_name, conn,
                  if_exists='append',
                  method='multi',
                  index=False,
                  chunksize=20000)
        pandas_gbq.to_gbq(df, 'analytix.'+table_name, project_id='atlas-336515', if_exists='append')
    else:
        # insert changed rows
        df.to_sql(table_name, conn,
                  if_exists='append',
                  method='multi',
                  index=False,
                  chunksize=20000)
        pandas_gbq.to_gbq(df, 'analytix.' + table_name, project_id='atlas-336515', if_exists='append')


def if_data_exists(table_name, date):
    PostgreSQL_engine = create_engine(config['PostgreSQL']['sqlalchemy_engine_str'], echo=True)
    conn = PostgreSQL_engine.connect()
    # The recommended way to check for existence
    if inspect(PostgreSQL_engine).has_table(table_name):
        with conn.begin():
            if conn.execute(text(f'SELECT distinct datetime from {table_name} '
                                 f'where datetime = DATE_TRUNC(\'day\', TIMESTAMP \'{date}\')')).first() is not None:
                return True
            else:
                return False


def set_start_end_dates(predefined_date):

    if not predefined_date:
        return datetime.strftime(datetime.now(),'%Y-%m-%d'), \
               datetime.strftime(datetime.now() + timedelta(days=1),'%Y-%m-%d')

    else:
        return datetime.strftime(datetime.strptime(predefined_date, "%Y-%m-%d"), "%Y-%m-%d"), \
                             datetime.strftime(datetime.strptime(predefined_date, "%Y-%m-%d") + timedelta(days=1),"%Y-%m-%d")


def set_time_period(predefined_date, n_hours = 1):

    if not predefined_date:
        return datetime.strftime(datetime.now()-timedelta(hours=n_hours),'%Y-%m-%d %H:%M:%S'), \
               datetime.strftime(datetime.now(),'%Y-%m-%d %H:%M:%S')

    else:
        return datetime.strftime(datetime.strptime(predefined_date, "%Y-%m-%d %H:%M:%S") - timedelta(hours=n_hours),"%Y-%m-%d %H:%M:%S"), \
               datetime.strftime(datetime.strptime(predefined_date, "%Y-%m-%d %H:%M:%S"), "%Y-%m-%d %H:%M:%S")



def hour_rounder(t):
    # Rounds to nearest hour by adding a timedelta hour if minute >= 30
    return datetime.strftime(t.replace(second=0, microsecond=0, minute=0, hour=t.hour)
               +timedelta(hours=t.minute//30),'%Y-%m-%d %H:%M:%S')

def day_rounder(t):
    return datetime.strftime(t, '%Y-%m-%d')