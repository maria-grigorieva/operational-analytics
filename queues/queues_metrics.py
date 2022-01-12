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
from cric.cric_json_api import enhance_queues
from rse_info.storage_info import get_agg_storage_data
from rucio_api.dataset_info import update_from_rucio
import numpy as np
from database_helpers.helpers import insert_to_db, if_data_exists, set_start_end_dates, set_time_period
import time
from datetime import datetime, timedelta

import logging

logging.basicConfig()
logging.getLogger('sqlalchemy.engine').setLevel(logging.ERROR)

SQL_DIR = BASE_DIR+'/sql'

cx_Oracle.init_oracle_client(lib_dir=r"/usr/lib/oracle/19.13/client64/lib")

config = configparser.ConfigParser()
config.read(BASE_DIR+'/config.ini')

metrics = {
    'efficiency':
        {
            'sql': SQL_DIR+'/PanDA/efficiency.sql',
            'table_name': 'queues_efficiency'
         },
    'occupancy':
        {
            'sql': SQL_DIR+'/PanDA/occupancy.sql',
            'table_name': 'queues_occupancy'
         },
    'running_time':
        {
            'sql': SQL_DIR+'/PanDA/running_time.sql',
            'table_name': 'running_time_pq'
         },
    'queue_time':
        {
            'sql': SQL_DIR+'/PanDA/queue_time.sql',
            'table_name': 'queue_time_pq'
        },
    'merged_queues_metrics':
        {
            'sql': SQL_DIR+'/PanDA/queues_metrics.sql',
            'table_name': 'queues_metrics'
        },
    'queues_statuslog':
        {
            'sql': {'statuslog': SQL_DIR+'/PanDA/queues_statuslog.sql',
                    'queue_time': SQL_DIR+'/PanDA/queue_time.sql',
                    'running_time': SQL_DIR+'/PanDA/running_time.sql'},
            'table_name': 'queues_statuslog'
        },
    'queues_statuslog_hourly':
        {
            'sql': SQL_DIR + '/PanDA/queues_statuslog_hourly.sql',
            'table_name': 'queues_snapshot_'
        },
    'queues_statuslog_actual':
        {
            'sql': SQL_DIR+'/PanDA/queues_statuslog_actual.sql',
            'table_name': 'queues_snapshots'
        }

}

PanDA_engine = create_engine(config['PanDA DB']['sqlalchemy_engine_str'], echo=True, future=True)
PostgreSQL_engine = create_engine(config['PostgreSQL']['sqlalchemy_engine_str'], echo=True)


def queues_to_db(metric, predefined_date = False):

    from_date, to_date = set_start_end_dates(predefined_date)

    if not if_data_exists(metrics.get(metric)["table_name"], from_date):
        panda_connection = PanDA_engine.connect()
        query = text(open(metrics.get(metric)['sql']).read())
        df = pd.read_sql_query(query, panda_connection, parse_dates={'datetime': '%Y-%m-%d'},
                               params={'from_date': from_date,
                                       'to_date': to_date})
        insert_to_db(df, metrics.get(metric)["table_name"], from_date)
    else:
        pass

def queues_to_db_tmp(metric, predefined_date = False):

    from_date, to_date = set_time_period(predefined_date, n_hours=24)

    if not if_data_exists(metrics.get(metric)["table_name"], from_date):
        panda_connection = PanDA_engine.connect()
        query = text(open(metrics.get(metric)['sql']).read())
        df = pd.read_sql_query(query, panda_connection, parse_dates={'datetime': '%Y-%m-%d %H:%M:%S'},
                               params={'from_date': from_date,
                                       'to_date': to_date})
        print(df)
        #insert_to_db(df, metrics.get(metric)["table_name"], from_date)
    else:
        pass


def queues_hourly_to_db(metric, predefined_date = False, n_hours=3):

    from_time, to_time = set_time_period(predefined_date, n_hours=n_hours)

    if not if_data_exists(metrics.get(metric)["table_name"], from_time):
        panda_connection = PanDA_engine.connect()
        query = text(open(metrics.get(metric)['sql']).read())
        df = pd.read_sql_query(query, panda_connection, parse_dates={'datetime': '%Y-%m-%d'},
                               params={'from_time': from_time,
                                       'to_time': to_time})
        insert_to_db(df, f'{metrics.get(metric)["table_name"]}{n_hours}h', from_time)
    else:
        pass


def calculate_metric(metric):
    panda_connection = PanDA_engine.connect()
    # postgres_connection = PostgreSQL_engine.connect()
    query = text(open(metrics.get(metric)['sql']).read())
    df = pd.read_sql_query(query, panda_connection, parse_dates={'datetime':'%Y-%m-%d'})
    curr_date = df['datetime'].unique()[0]
    from_cric = cric.cric_json_api.enhance_queues()
    result = pd.merge(df, from_cric, left_on='queue', right_on='queue')
    result['transferring_diff'] = result['transferring_limit'] - result['transferring']

    storage_info = get_agg_storage_data()
    result = pd.merge(result, storage_info, left_on='rse', right_on='rse')

    insert_to_db(df, metrics.get(metric)["table_name"], curr_date)


def exclude_outliers():
    postgres_connection = PostgreSQL_engine.connect()
    query = text('select * from queues_metrics')
    df = pd.read_sql_query(query, postgres_connection, parse_dates={'datetime': '%Y-%m-%d'})
    curr_date = df['datetime'].unique()[0]
    cols = ['queue_time_avg','queue_utilization','queue_filling','queue_efficiency']  # one or more

    Q1 = df[cols].quantile(0.1)
    Q3 = df[cols].quantile(0.9)
    IQR = Q3 - Q1

    lower_bound = Q1 - 1.5 * IQR
    upper_bound = Q3 + 1.5 * IQR

    reduced_df = df[~((df[cols] < lower_bound) | (df[cols] > upper_bound)).any(axis=1)]
    reduced_df = reduced_df[reduced_df['status']!='test']
    reduced_df = reduced_df[(reduced_df['Difference']>0) | (pd.isnull(reduced_df['Difference']))]
    reduced_df = reduced_df[reduced_df['queue_efficiency'] > 0.7]

    excluded_df = pd.concat([df, reduced_df]).drop_duplicates(keep=False)

    insert_to_db(df, postgres_connection, 'filtered_metrics', curr_date)
    insert_to_db(df, postgres_connection, 'excluded_metrics', curr_date)


def popularity_by_tasks(from_date, hours=4):
    """
    Calculates popularity (by number of tasks) of each input dataset used during the specified period,
    resulted dataset list is sorted by popularity value (from max to min)
    """
    panda_connection = PanDA_engine.connect()
    postgres_connection = PostgreSQL_engine.connect()

    # get MAX modificationtime from datasets_popularity
    try:
        from_date = pd.read_sql_query(text('select max(task_modificationtime) from datasets_popularity'),
                                   postgres_connection)['max'].values[0]
        from_date = np.datetime_as_string(from_date, unit='s').replace('T',' ')
    except Exception as e:
        None

    query = text(open(SQL_DIR+'/PanDA/data_popularity.sql').read())
    df = pd.read_sql_query(query, panda_connection, parse_dates={'datetime': '%Y-%m-%d %H:%M:%S'}, params={'from_date':from_date,'hours':hours})
    from_cric = cric.cric_json_api.enhance_queues(all=True)
    result = pd.merge(df, from_cric, left_on='queue', right_on='queue')
    # result.to_sql('datasets_popularity', postgres_connection,
    #                   if_exists='append',
    #                   method='multi',
    #                   index=False)

    ds_names = result['datasetname'].unique()
    print(f'Total {len(ds_names)} datasets')
    try:
        datasets = pd.DataFrame([update_from_rucio(d) for d in ds_names])
        datasets = datasets.add_prefix('ds_')
        datasets_df = pd.merge(result, datasets, left_on='datasetname', right_on='ds_datasetname')
        datasets_df.to_sql('datasets_popularity', postgres_connection,
                      if_exists='append',
                      method='multi',
                      index=False)
    except Exception as e:
        print('Rucio API returned NO info about datasets')
        return None


def queues_statuslog(from_date):
    panda_connection = PanDA_engine.connect()
    postgres_connection = PostgreSQL_engine.connect()
    statuslog_query = open(metrics.get('queues_statuslog')['sql']['statuslog']).read()
    queue_time_query = open(metrics.get('queues_statuslog')['sql']['queue_time']).read()
    running_time_query = open(metrics.get('queues_statuslog')['sql']['running_time']).read()
    statuslog_df = pd.read_sql_query(text(statuslog_query), panda_connection,
                                     parse_dates={'datetime': '%Y-%m-%d'},
                                     params={'datetime':from_date})
    queue_time_df = pd.read_sql_query(text(queue_time_query), panda_connection,
                                      parse_dates={'datetime': '%Y-%m-%d'},
                                      params={'datetime':from_date})
    running_time_df = pd.read_sql_query(text(running_time_query), panda_connection,
                                        parse_dates={'datetime': '%Y-%m-%d'},
                                        params={'datetime':from_date})

    tmp = pd.merge(statuslog_df,queue_time_df,left_on=['queue','datetime'],right_on=['queue','datetime'])
    result_df = pd.merge(tmp,running_time_df,left_on=['queue','datetime'],right_on=['queue','datetime'])

    from_cric = cric.cric_json_api.enhance_queues()
    result = pd.merge(result_df, from_cric, left_on='queue', right_on='queue')
    #
    # storage_info = get_agg_storage_data()
    # result = pd.merge(result, storage_info, left_on='rse', right_on='rse')

    # insert changed rows
    result.to_sql(metrics.get('queues_statuslog')['table_name'], postgres_connection,
                  if_exists='append',
                  method='multi',
                  index=False)


def queues_statuslog_hourly(from_date):
    panda_connection = PanDA_engine.connect()
    postgres_connection = PostgreSQL_engine.connect()
    statuslog_query = open(metrics.get('queues_statuslog_hourly')['sql']).read()
    statuslog_df = pd.read_sql_query(text(statuslog_query), panda_connection,
                                     parse_dates={'datetime': '%Y-%m-%d'},
                                     params={'datetime': from_date})
    from_cric = cric.cric_json_api.enhance_queues()
    result = pd.merge(statuslog_df, from_cric, left_on='queue', right_on='queue')

    # insert changed rows
    result.to_sql(metrics.get('queues_statuslog_hourly')['table_name'], postgres_connection,
                  if_exists='append',
                  method='multi',
                  index=False)


# popularity_by_tasks('2021-12-01 00:00:00',1)
#queues_to_db_tmp('queues_statuslog_actual', '2022-01-10 09:00:00')
#queues_hourly_to_db('queues_statuslog_hourly', predefined_date = False, n_hours=12)