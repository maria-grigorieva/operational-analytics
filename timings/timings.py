import os, sys
ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
BASE_DIR = os.path.join(ROOT_DIR, '..' )
sys.path.append(os.path.abspath(BASE_DIR))
import cx_Oracle
import pandas as pd
from sqlalchemy import create_engine, text
import configparser
from database_helpers.helpers import insert_to_db, check_for_data_existance, set_time_period, localized_now
from datetime import datetime, timedelta
from sklearn.preprocessing import MinMaxScaler

import logging

logging.basicConfig()
logging.getLogger('sqlalchemy').setLevel(logging.ERROR)

SQL_DIR = BASE_DIR+'/sql'

cx_Oracle.init_oracle_client(lib_dir=r"/usr/lib/oracle/19.13/client64/lib")

config = configparser.ConfigParser()
config.read(BASE_DIR+'/config.ini')

PanDA_engine = create_engine(config['PanDA DB']['sqlalchemy_engine_str'], echo=True, future=True)
PostgreSQL_engine = create_engine(config['PostgreSQL']['sqlalchemy_engine_str'], echo=True)

def task_timings_to_db(predefined_date = False, hours=24):

    from_date, to_date = set_time_period(predefined_date, n_hours=24)

    if not check_for_data_existance('tasks_timings', from_date, delete=True):
        panda_connection = PanDA_engine.connect()
        query = text(open(SQL_DIR+'/PanDA/task_timings.sql').read())
        df = pd.read_sql_query(query,
                               panda_connection,
                               parse_dates={'datetime': '%Y-%m-%d'},
                               params={'from_date': from_date,
                                       'hours': hours})
        datetime_cols = ['rerefined_tstamp',
                         'defined_tstamp',
                         'ready_tstamp',
                         'running_tstamp',
                         'scouting_tstamp'
                         ]
        for i in datetime_cols:
            df[i] = df[i].astype(str)
        panda_connection.close()
        # df.fillna(0, inplace=True)
        insert_to_db(df, 'tasks_timings')
    else:
        pass


def job_timings_to_db(predefined_date = False, hours=24):

    from_date, to_date = set_time_period(predefined_date, n_hours=24)

    if not check_for_data_existance('jobs_timings', from_date, delete=True):
        panda_connection = PanDA_engine.connect()
        query = text(open(SQL_DIR+'/PanDA/jobs_timings.sql').read())
        df = pd.read_sql_query(query,
                               panda_connection,
                               # parse_dates={'start_tstamp': '%Y-%m-%d'},
                               params={'from_date': from_date,
                                       'hours': hours})
        datetime_cols = ['transferring_tstamp',
                         'merging_tstamp']
        for i in datetime_cols:
            df[i] = df[i].astype(str)
        panda_connection.close()
        # df.fillna(0, inplace=True)
        insert_to_db(df, 'jobs_timings')
    else:
        pass


def jobs_agg(predefined_date = False):

    from_date, to_date = set_time_period(predefined_date, n_hours=24)

    if not check_for_data_existance('jobs_agg', from_date, delete=True):
        postgresql_connection = PostgreSQL_engine.connect()
        query = text(open(SQL_DIR + '/postgreSQL/jobs_agg.sql').read())
        df = pd.read_sql_query(query,
                               postgresql_connection,
                               params={'from_date': from_date})
        # datetime_cols = ['transferring_tstamp',
        #                  'merging_tstamp']
        # for i in datetime_cols:
        #     df[i] = df[i].astype(str)
        postgresql_connection.close()

        scaler = MinMaxScaler()
        columns_to_scale = ['sum_inputfilebytes',
                            'sum_outputfilebytes',
                            'sum_ninputdatafiles',
                            'sum_noutputdatafiles',
                            'sum_nevents',
                            'avg_cpuconsumptiontime']
        idx = df.index
        df_to_scale = df[columns_to_scale]
        scaled = scaler.fit_transform(df_to_scale)
        scaled = pd.DataFrame(scaled, columns=columns_to_scale,index=idx)
        df['complexity']= scaled.sum(axis=1,skipna=True)
        # df.fillna(0, inplace=True)
        insert_to_db(df, 'jobs_agg')
    else:
        pass

def collection_for_time_period():

    start_date = datetime(2022, 6, 10, 0, 0, 0)
    end_date = datetime(2022, 6, 11, 0, 0, 0)
    delta_day = timedelta(days=1)

    while start_date <= end_date:
        print(start_date)
        # job_timings_to_db(predefined_date = datetime.strftime(start_date, "%Y-%m-%d %H:%M:%S"), hours=24) # 08.05.2022
        # task_timings_to_db(predefined_date=datetime.strftime(start_date, "%Y-%m-%d %H:%M:%S"), hours=24)
        jobs_agg(predefined_date=datetime.strftime(start_date, "%Y-%m-%d %H:%M:%S"), hours=24)
        start_date += delta_day


# collection_for_time_period()

# job_timings_to_db('2022-08-02 00:00:00', hours=24)
# task_timings_to_db('2022-06-30 00:00:00', hours=24)
jobs_agg('2022-06-30 00:00:00')