import os, sys
ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
BASE_DIR = os.path.join(ROOT_DIR, '..' )
# sys.path.append(os.path.abspath(BASE_DIR))
import configparser
from sqlalchemy import create_engine, text, inspect
from datetime import datetime, timedelta
from google.cloud import bigquery
# from google.oauth2 import service_account
import pandas_gbq
import json
import os
import pandas as pd
import re
import pytz
import logging

logging.basicConfig()
logging.getLogger('sqlalchemy').setLevel(logging.ERROR)
# sqla_logger = logging.getLogger('sqlalchemy')
# sqla_logger.propagate = False
# sqla_logger.addHandler(logging.FileHandler('sqla.log'))


SQL_DIR = BASE_DIR+'/sql'
BG_SCHEMAS = BASE_DIR+'/google_schemas/'
BG_DATETYPES = BASE_DIR+'/BigQuery/data_types.json'

config = configparser.ConfigParser()
config.read(BASE_DIR+'/config.ini')

bigquery_project_id = 'atlas-336515'
bigquery_dataset = 'analytix'

PostgreSQL_engine = create_engine(config['PostgreSQL']['sqlalchemy_engine_str'], echo=False)

os.environ["GOOGLE_APPLICATION_CREDENTIALS"]=config['GOOGLE']['app']

# def insert_to_db(df, table_name, curr_date, delete=True):
#
#         if check_for_data_existance(table_name, curr_date, accuracy, delete) == False:
#             write_to_db(df, table_name)
#         else:
#             pass
#     else:
#         write_to_db(df, table_name)


def write_to_postgreSQL(df, table_name):

    conn = PostgreSQL_engine.connect()
    # Write to PostgreSQL
    df.to_sql(table_name, conn, if_exists='append', method='multi', index=False, chunksize=20000)
    conn.close()


def get_google_schema(table_name):

    schema = None
    arr = os.listdir(BG_SCHEMAS)
    for i in arr:
        if i.split('.')[0] == table_name:
            file = open(BG_SCHEMAS + i, mode='r')
            all_of_it = file.read()
            schema = json.loads(all_of_it)
            file.close()
    return schema

def write_to_bigquery(df, table_name):

    df = fix_datetypes(df)
    pandas_gbq.to_gbq(df, 'analytix.' + table_name, project_id='atlas-336515', if_exists='append',
                      table_schema=get_google_schema(table_name))


def insert_to_db(df, table_name):

    write_to_postgreSQL(df, table_name)
    write_to_bigquery(df, table_name)


def check_for_data_existance(table_name, now, accuracy='day', delete=True, dt='datetime'):
    """
    Check if a table have rows with the defined datetime value
    :param table_name:
    :param date:
    :param accuracy: day | hour
    :return:
    """
    pgsql_exists = check_postgreSQL(table_name, now, accuracy=accuracy, datetime_col_name=dt)
    bigquery_exists = check_bigquery(table_name, now, accuracy=accuracy, datetime_col_name=dt)

    if pgsql_exists and bigquery_exists:
        if delete:
            delete_from_pgsql(table_name, now, datetime_col_name=dt)
            delete_from_bigquery(table_name, now, datetime_col_name=dt)
            return False
        else:
            return True
    elif pgsql_exists and not bigquery_exists:
        if delete:
            delete_from_pgsql(table_name, now, datetime_col_name=dt)
            return False
        else:
            return True
    elif bigquery_exists and not pgsql_exists:
        if delete:
            delete_from_bigquery(table_name, now, datetime_col_name=dt)
            return False
        else:
            return True
    elif not bigquery_exists and not pgsql_exists:
        return False


def check_postgreSQL(table_name, now, accuracy, datetime_col_name='datetime'):

    try:
        conn = PostgreSQL_engine.connect()
        query = f'SELECT * FROM {table_name} WHERE {datetime_col_name} >= date_trunc(\'{accuracy}\', TIMESTAMP \'{now}\') ' \
                f'AND {datetime_col_name} < date_trunc(\'{accuracy}\', TIMESTAMP \'{now}\' + INTERVAL \'1 {accuracy}\')'
        result = conn.execute(text(query)).first()
        conn.close()
        return True if result is not None else False
    except Exception as e:
        return False

def check_bigquery(table_name, now, accuracy, datetime_col_name='datetime'):

    #from_date = day_rounder(datetime.strptime(now, "%Y-%m-%d %H:%M:%S"))
    freq = 'DAY' if accuracy=='day' else 'HOUR'
    query = f'SELECT * FROM `{bigquery_project_id}.{bigquery_dataset}.{table_name}` ' \
            f'WHERE TIMESTAMP({datetime_col_name}) >= TIMESTAMP(date_trunc(\'{now}\', {freq}))' \
            f'AND TIMESTAMP({datetime_col_name}) < TIMESTAMP(date_trunc(DATE_ADD(DATE \'{now}\', INTERVAL 1 {freq}), {freq}))'

    try:
        df = pandas_gbq.read_gbq(query)
        return not df.empty
    except Exception as e:
        return False


def delete_from_pgsql(table_name, now, accuracy='day', datetime_col_name='datetime'):

    conn = PostgreSQL_engine.connect()
    # from_date = day_rounder(datetime.strptime(now, "%Y-%m-%d %H:%M:%S"))
    remove_statement = f'DELETE FROM {table_name} WHERE {datetime_col_name} >= date_trunc(\'{accuracy}\', TIMESTAMP \'{now}\') ' \
            f'AND {datetime_col_name} < date_trunc(\'{accuracy}\', TIMESTAMP \'{now}\' + INTERVAL \'1day\')'
    conn.execute(text(remove_statement))
    conn.close()


def delete_from_bigquery(table_name, now, accuracy='day', datetime_col_name='datetime'):

    client = bigquery.Client()

    #from_date = day_rounder(datetime.strptime(now, "%Y-%m-%d %H:%M:%S"))
    freq = 'DAY' if accuracy == 'day' else 'HOUR'
    remove_statement = f'DELETE FROM `{bigquery_project_id}.{bigquery_dataset}.{table_name}` ' \
            f'WHERE TIMESTAMP({datetime_col_name}) >= TIMESTAMP(date_trunc(\'{now}\', {freq}))' \
            f'AND TIMESTAMP({datetime_col_name}) < TIMESTAMP(date_trunc(DATE_ADD(DATE \'{now}\', INTERVAL 1 {freq}), {freq}))'
    query_job = client.query(remove_statement)
    query_job.result()


def set_start_end_dates(predefined_date):

    d = localized_now() if not predefined_date else datetime.strptime(predefined_date, "%Y-%m-%d")
    return datetime.strftime(d, '%Y-%m-%d'), \
           datetime.strftime(d + timedelta(days=1), '%Y-%m-%d')


def localized_now():
    oracle_tz = pytz.timezone("Europe/Zurich")
    d = datetime.now()
    local_datetime = oracle_tz.localize(d, is_dst=None)
    utc_datetime = local_datetime.astimezone(pytz.utc)
    return utc_datetime


def set_time_period(predefined_date, n_hours = 1):

    d = localized_now() if not predefined_date else datetime.strptime(predefined_date, "%Y-%m-%d %H:%M:%S")
    return datetime.strftime(d - timedelta(hours=n_hours), '%Y-%m-%d %H:%M:%S'), \
           datetime.strftime(d, '%Y-%m-%d %H:%M:%S')


def hour_rounder(t):
    # Rounds to nearest hour by adding a timedelta hour if minute >= 30
    return datetime.strftime(t.replace(second=0, microsecond=0, minute=0, hour=t.hour)
               +timedelta(hours=t.minute//30),'%Y-%m-%d %H:%M:%S')

def day_rounder(t):
    return datetime.strftime(t, '%Y-%m-%d')


def nulls_ints_to_zeroes(df):
    # select numeric columns
    int_columns = df.select_dtypes(include=['int','float']).columns
    date_columns = df.select_dtypes(include=['datetime']).columns

    # fill -1 to all NaN
    df[int_columns] = df[int_columns].fillna(0)
    df[date_columns] = df[date_columns].fillna('0000-00-00 00:00:00')


def fix_datetypes(df):
    data_types = open(BG_DATETYPES, 'r')
    content = data_types.read()
    types = json.loads(content)
    dict_of_fields = {}
    columns = df.columns
    for i in columns:
        if i in types:
            dict_of_fields[i] = types[i]
    df.reset_index(inplace=True, drop=True)
    df = df.astype(dict_of_fields)
    return df

# def data_format_extractor(row):
#     format = row.split('.')[4]
#     return re.sub('\d', '-', format).strip('-')
#     # return format[:-1] + last_char
# def prod_step_extractor(row):
#     return re.sub('\d', '-', row.split('.')[3]).strip('-')
#
#
# conn = PostgreSQL_engine.connect()
# query = 'SELECT * FROM datasets_tasks_users limit 10'
# df = pd.read_sql_query(text(query), conn)
# # df['project'] = df['datasetname'].str.split(':').str.get(0)
# # df['data_type'] = df['datasetname'].apply(data_format_extractor)
# # df['data_format'] = df['data_type'].str.split('_').str.get(0)
# # df['data_type_desc'] = df['data_type'].str.split('_').str.get(1)
# df['project'] = df['datasetname'].str.split(':').str.get(0)
# df['prod_step'] = df['datasetname'].apply(prod_step_extractor)
# df['data_format_full'] = df['datasetname'].apply(data_format_extractor)
# df['data_format'] = df['data_format_full'].str.split('_').str.get(0)
# df['data_format_desc'] = df['data_format_full'].str.split('_').str.get(1)
# df.drop('data_format_full',axis=1,inplace=True)
# print(df['data_format_desc'])