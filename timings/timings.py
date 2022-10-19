import os, sys
ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
BASE_DIR = os.path.join(ROOT_DIR, '..' )
sys.path.append(os.path.abspath(BASE_DIR))
import cx_Oracle
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
import configparser
from cric.cric_json_api import enhance_queues
from database_helpers.helpers import insert_to_db, check_for_data_existance, set_time_period, localized_now
from datetime import datetime, timedelta
import json

import logging

logging.basicConfig()
logging.getLogger('sqlalchemy').setLevel(logging.ERROR)
logging.disable(logging.INFO)

SQL_DIR = BASE_DIR+'/sql'

config = configparser.ConfigParser()
config.read(BASE_DIR+'/config.ini')

# cx_Oracle.init_oracle_client(lib_dir=config['PanDA DB']['client_path'])

PanDA_engine = create_engine(config['PanDA DB']['sqlalchemy_engine_str'], echo=False, max_identifier_length=128)
PostgreSQL_engine = create_engine(config['PostgreSQL']['sqlalchemy_engine_str'], echo=False)

def task_timings_to_db(predefined_date = False):

    from_date, to_date = set_time_period(predefined_date, n_hours=24)

    if not check_for_data_existance('tasks_timings', from_date, delete=True, dt="end_time"):
        panda_connection = PanDA_engine.connect()
        query = text(open(SQL_DIR+'/PanDA/task_timings.sql').read())
        df = pd.read_sql_query(query,
                               panda_connection,
                               parse_dates={'datetime': '%Y-%m-%d'},
                               params={'from_date': from_date})
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
    #from_date = datetime.strftime(localized_now(),"%Y-%m-%d %H:%M:%S") if not predefined_date else str(predefined_date)

    if not check_for_data_existance('jobs_timings', from_date, delete=True, dt='completed_tstamp'):
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

        from_cric = enhance_queues()
        from_cric.drop(['status','state','corecount','corepower','transferring_limit',
                        'nodes'],axis=1,inplace=True)
        result = pd.merge(df, from_cric, left_on='queue', right_on='queue')
        # df.fillna(0, inplace=True)
        panda_connection.close()
        insert_to_db(result, 'jobs_timings')
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
        df = df.astype({'jeditaskid': np.int64,
                 'task_attemptnr': np.int32,
                 'lib_size': np.int64,
                 'assigned_priority': np.int32,
                 'current_priority': np.int32,
                 'avg_hs06sec': np.float64,
                 'sum_nevents': np.int64,
                 'sum_ninputdatafiles': np.float64,
                 'sum_noutputdatafiles': np.float64,
                 'sum_inputfilebytes': np.float64,
                 'sum_outputfilebytes': np.float64,
                 'execution_start_tstamp': np.datetime64,
                 'execution_end_tstamp': np.datetime64,
                 'completed_tstamp': np.datetime64,
                 'creation_tstamp': np.datetime64,
                 'avg_waiting_time': np.float64,
                 'avg_execution_time': np.float64,
                 'avg_cpuconsumptiontime': np.float64,
                 'cpu_walltime_ratio': np.float64,
                 'avg_total_time': np.float64,
                 'avg_transferring_time': np.float64,
                 'avg_merging_time': np.float64,
                 'weighted_waiting_time': np.float64,
                 'weighted_cpuconsumptiontime': np.float64,
                 'weighted_execution_time': np.float64,
                 'weighted_total_time': np.float64,
                 'weighted_transferring_time': np.float64,
                 'weighted_merging_time': np.float64})
        # datetime_cols = ['transferring_tstamp',
        #                  'merging_tstamp']
        # for i in datetime_cols:
        #     df[i] = df[i].astype(str)
        postgresql_connection.close()

        # scaler = MinMaxScaler()
        # columns_to_scale = ['sum_inputfilebytes',
        #                     'sum_outputfilebytes',
        #                     'sum_ninputdatafiles',
        #                     'sum_noutputdatafiles',
        #                     'sum_nevents',
        #                     'weighted_cpuconsumptiontime',
        #                     'weighted_execution_time',
        #                     'lib_size']
        # idx = df.index
        # df_to_scale = df[columns_to_scale]
        # scaled = scaler.fit_transform(df_to_scale)
        # scaled = pd.DataFrame(scaled, columns=columns_to_scale,index=idx)
        # df['complexity']= scaled.sum(axis=1,skipna=True)
        # df.fillna(0, inplace=True)
        insert_to_db(df, 'jobs_agg')
    else:
        pass

