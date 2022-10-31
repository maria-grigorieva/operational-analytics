import os, sys
ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
BASE_DIR = os.path.join(ROOT_DIR, '..' )
sys.path.append(os.path.abspath(BASE_DIR))
import pandas as pd
from sqlalchemy import create_engine, text
import configparser
from database_helpers.helpers import write_to_postgreSQL
import logging
from datetime import datetime, timedelta
from group_popularity import group_popularity_to_db
from dataset_popularity import datasets_popularity_to_db, aggregation_week


logging.basicConfig()
logging.getLogger('sqlalchemy.engine').setLevel(logging.ERROR)

SQL_DIR = BASE_DIR+'/sql'

config = configparser.ConfigParser()
config.read(BASE_DIR+'/config.ini')

PostgreSQL_engine = create_engine(config['PostgreSQL']['sqlalchemy_engine_str'], echo=False)

# aggregation_week('2022-10-24')

# PostgreSQL_connection = PostgreSQL_engine.connect()
# query = text('SELECT * FROM datasets_popularity')
# df = pd.read_sql_query(query, PostgreSQL_connection)
#
# subset_columns = ['datasetname',
#                   'input_format_short',
#                   'input_format_desc',
#                   'input_project',
#                   'prod_step',
#                   'process_desc',
#                   'n_dataset',
#                   'tid',
#                   'process_tags']
#
# all_datasets = df[subset_columns].drop_duplicates(subset=subset_columns)
#
# weeks = df.groupby('datetime')
#
# for k,v in weeks:
#     print(k)
#     not_used = all_datasets[~all_datasets['datasetname'].isin(v['datasetname'])]
#     not_used['used'] = 0
#     not_used['n_tasks'] = 0
#     not_used['n_users'] = 0
#     not_used['datetime'] = k
#     write_to_postgreSQL(not_used)


def collect_dates():
    start_date = datetime(2021, 5, 2)
    end_date = datetime(2022, 10, 17)
    list_of_dates = pd.date_range(start=datetime.strftime(start_date, "%Y-%m-%d"),
                  end=datetime.strftime(end_date, "%Y-%m-%d"), freq='W')
    print(list_of_dates)
    for i in list_of_dates:
        print(i)
        datasets_popularity_to_db(predefined_date = datetime.strftime(i, "%Y-%m-%d"))
