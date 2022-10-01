import os, sys
ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
BASE_DIR = os.path.join(ROOT_DIR, '..' )
sys.path.append(os.path.abspath(BASE_DIR))
import cx_Oracle
import cric
import pandas as pd
from sqlalchemy import create_engine, text
import configparser
from cric.cric_json_api import enhance_queues
from database_helpers.helpers import insert_to_db, check_for_data_existance, set_time_period, localized_now
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
            'table_name': 'queues_snapshot_intervals'
        },
    'queues_hourly':
        {
            'sql': SQL_DIR + '/PanDA/queues_statuslog_hourly.sql',
            'table_name': 'queues_intervals'
        },
    'queues_statuslog_actual':
        {
            'sql': SQL_DIR+'/PanDA/queues_statuslog_actual.sql',
            'table_name': 'queues_snapshots'
        },
    'queues_statuslog_detailed':
        {
            'sql': SQL_DIR + '/PanDA/queues_utilization.sql',
            'table_name': 'queues_utilization'
        }
}

PanDA_engine = create_engine(config['PanDA DB']['sqlalchemy_engine_str'], echo=True, future=True)
PostgreSQL_engine = create_engine(config['PostgreSQL']['sqlalchemy_engine_str'], echo=True)


def queues_to_db(metric, predefined_date = False):

    from_date, to_date = set_time_period(predefined_date, n_hours=24)

    if not check_for_data_existance(metrics.get(metric)["table_name"], from_date, delete=True):
        panda_connection = PanDA_engine.connect()
        query = text(open(metrics.get(metric)['sql']).read())
        df = pd.read_sql_query(query, panda_connection, parse_dates={'datetime': '%Y-%m-%d'},
                               params={'from_date': from_date})
        print(df)
        panda_connection.close()
        df.fillna(0, inplace=True)
        insert_to_db(df, metrics.get(metric)["table_name"])
    else:
        pass


# detailed queues statuslog with computingelements
def queues_hourly_statuslog_to_db(metric, predefined_date = False):

    now = datetime.strftime(localized_now(),"%Y-%m-%d %H:%M:%S") if not predefined_date else str(predefined_date)

    panda_connection = PanDA_engine.connect()
    postgresql_connection = PostgreSQL_engine.connect()
    query = text(open(metrics.get(metric)['sql']).read())
    df = pd.read_sql_query(query, panda_connection,parse_dates={'datetime': '%Y-%m-%d %H:%M:%S'},
                           params={'from_date': now, 'hours': 1})
    panda_connection.close()
    # print(f'{df.shape[0]} rows has been returned from PanDA DB')
    from_cric = cric.cric_json_api.enhance_queues()
    result = pd.merge(df, from_cric, left_on='queue', right_on='queue')

    with postgresql_connection.begin():
        try:
            if postgresql_connection.execute(text(f'SELECT * FROM {metrics.get(metric)["table_name"]} '
                                              f'WHERE datetime = DATE_TRUNC(\'hours\', TIMESTAMP \'{now}\')')).rowcount == 0:
                insert_to_db(result,metrics.get(metric)["table_name"])
                postgresql_connection.close()
                print(f'The data has {len(result)} rows')
        except Exception as e:
            print('Insert data failed!')


def queues_hourly_to_db(metric, predefined_date = False, n_hours=1):

    now = datetime.strftime(localized_now(),"%Y-%m-%d %H:%M:%S") if not predefined_date else str(predefined_date)

    panda_connection = PanDA_engine.connect()
    postgresql_connection = PostgreSQL_engine.connect()
    query = text(open(metrics.get(metric)['sql']).read())
    df = pd.read_sql_query(query, panda_connection,parse_dates={'datetime': '%Y-%m-%d %H:%M:%S'},
                           params={'now': now,'n_hours': n_hours})
    panda_connection.close()
    print(f'{df.shape[0]} rows has been returned from PanDA DB')
    from_cric = cric.cric_json_api.enhance_queues()
    result = pd.merge(df, from_cric, left_on='queue', right_on='queue')
    result['transferring_diff'] = result['transferring_limit'] - result['transferring']
    result['interval_hours'] = n_hours
    result['corecount'].fillna(0,inplace=True)
    # Custom check for existance using datetime and interval_hours parameters
    with postgresql_connection.begin():
        try:
            if postgresql_connection.execute(text(f'SELECT * FROM {metrics.get(metric)["table_name"]} '
                                              f'WHERE datetime = DATE_TRUNC(\'hour\', TIMESTAMP \'{now}\') '
                                              f'AND interval_hours = {n_hours}')).rowcount == 0:
                insert_to_db(result,metrics.get(metric)["table_name"])
                postgresql_connection.close()
                print(f'The data has {len(result)} rows')
        except Exception as e:
            print('Insert data failed!')


def enhanced_queues_utilization(predefined_date = False, mode='daily'):

    now = datetime.strftime(localized_now(), "%Y-%m-%d %H:%M:%S") \
        if not predefined_date else str(predefined_date)

#    from_date, to_date = set_time_period(predefined_date, n_hours=24 if mode=='daily' else 1)

    if not check_for_data_existance(f'{mode}_enhanced_queues_utilization', now,  accuracy='hour', delete=True):
        postgreSQL_connection = PostgreSQL_engine.connect()
        query = text(open(SQL_DIR + f'/postgreSQL/queues_utilization_enhanced_{mode}.sql').read())
        df = pd.read_sql_query(query, postgreSQL_connection, parse_dates={'datetime': '%Y-%m-%d'},
                               params={'from_date': now})
        postgreSQL_connection.close()
        #df.fillna(0, inplace=True)
        insert_to_db(df, f'{mode}_enhanced_queues_utilization')
    else:
        pass



def collect_queues_for_period():

    start_date = datetime(2022, 9, 25, 1, 0, 0)
    end_date = datetime(2022, 9, 25, 2, 0, 0)
    # delta_day = timedelta(days=1)
    delta_hours = timedelta(hours=1)

    while start_date <= end_date:
        print(start_date)
        #queues_hourly_statuslog_to_db('queues_statuslog_detailed',
        #                             predefined_date=datetime.strftime(start_date, "%Y-%m-%d %H:%M:%S"))
        enhanced_queues_utilization(predefined_date=datetime.strftime(start_date, "%Y-%m-%d %H:%M:%S"),
                                     mode='hourly')
 #       queues_to_db('queues_statuslog_actual',
 #                    predefined_date=datetime.strftime(start_date, "%Y-%m-%d %H:%M:%S"))
        #start_date += delta_day
        start_date += delta_hours



def collect_hourly_data_for_period(metric):
    start_date = datetime(2022, 5, 16, 0, 0, 0)
    end_date = datetime(2022, 5, 17, 0, 0, 0)
    delta_day = timedelta(days=1)
    delta_1hour = timedelta(hours=1)
    delta_3hours = timedelta(hours=3)
    delta_6hours = timedelta(hours=6)
    delta_12hours = timedelta(hours=12)

    while start_date <= end_date:
        print(start_date)

        curr_date = start_date
        while curr_date <= start_date + delta_day:
            print(curr_date)
            queues_hourly_to_db(metric, predefined_date = curr_date, n_hours=1)
            curr_date += delta_1hour

        curr_date = start_date
        while curr_date <= start_date + delta_day:
            print(curr_date)
            queues_hourly_to_db(metric, predefined_date = curr_date, n_hours=3)
            curr_date += delta_3hours

        curr_date = start_date
        while curr_date <= start_date + delta_day:
            print(curr_date)
            queues_hourly_to_db(metric, predefined_date = curr_date, n_hours=6)
            curr_date += delta_6hours

        curr_date = start_date
        while curr_date <= start_date + delta_day:
            print(curr_date)
            queues_hourly_to_db(metric, predefined_date = curr_date, n_hours=12)
            curr_date += delta_12hours

        queues_hourly_to_db(metric, predefined_date = start_date, n_hours=24)

        start_date += delta_day

collect_queues_for_period()
# collect_hourly_data_for_period('queues_hourly')
# queues_to_db('queues_statuslog_actual', '2022-09-02 01:00:00')
#queues_hourly_to_db('queues_statuslog_hourly','2022-05-16 10:00:00',1)
# queues_hourly_to_db('queues_hourly','2022-05-17 15:00:00',1)
# queues_hourly_to_db('queues_hourly',False,1)

# queues_hourly_statuslog_to_db('queues_statuslog_detailed','2022-06-13 19:30:00')