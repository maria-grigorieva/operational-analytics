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
    'job_shares':
        {
            'sql': SQL_DIR+'/PanDA/jobs_shares.sql',
            'table_name': 'jobs_shares'
         },
    'queueing_time':
        {
            'sql': SQL_DIR+'/PanDA/queueing_time.sql',
            'table_name': 'queueing_time'
        },
    'merged_queues_metrics':
        {
            'sql': SQL_DIR+'/PanDA/queues_metrics.sql',
            'table_name': 'queues_metrics'
        }
}

PanDA_engine = create_engine(config['PanDA DB']['sqlalchemy_engine_str'], echo=True, future=True)
PostgreSQL_engine = create_engine(config['PostgreSQL']['sqlalchemy_engine_str'], echo=True)

def calculate_metric(metric):
    panda_connection = PanDA_engine.connect()
    postgres_connection = PostgreSQL_engine.connect()
    query = text(open(metrics.get(metric)['sql']).read())
    df = pd.read_sql_query(query, panda_connection, parse_dates={'datetime':'%Y-%m-%d'})
    curr_date = df['datetime'].unique()[0]
    from_cric = cric.cric_json_api.enhance_queues()
    result = pd.merge(df, from_cric, left_on='queue', right_on='queue')

    storage_info = get_agg_storage_data()
    result = pd.merge(result, storage_info, left_on='rse', right_on='rse')

    # The recommended way to check for existence
    if inspect(PostgreSQL_engine).has_table(metrics.get(metric)['table_name']):

        with postgres_connection.begin():
            if postgres_connection.execute(text(f'SELECT distinct datetime from {metrics.get(metric)["table_name"]} '
                                                f'where datetime = \'{curr_date}\'')).first() is not None:
                # delete those rows that we are going to "upsert"
                postgres_connection.execute(text(f'delete from {metrics.get(metric)["table_name"]} '
                                                 f'where datetime = \'{curr_date}\''))

    # insert changed rows
    result.to_sql(metrics.get(metric)['table_name'], postgres_connection,
                  if_exists='append',
                  method='multi',
                  index=False)
