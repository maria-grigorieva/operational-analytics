from google.cloud import bigquery
import os, sys
ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
BASE_DIR = os.path.join(ROOT_DIR, '..' )
import pandas_gbq
import pandas as pd
import os
import io
from sqlalchemy import create_engine, text, inspect
import configparser
import json

config = configparser.ConfigParser()
config.read(BASE_DIR+'/config.ini')

os.environ["GOOGLE_APPLICATION_CREDENTIALS"]=config['GOOGLE']['app']

SQL_DIR = BASE_DIR+'/sql'
BG_SCHEMAS = BASE_DIR+'/google_schemas/'
BG_DATETYPES = BASE_DIR+'/BigQuery/data_types.json'

project_id = 'atlas-336515'


PostgreSQL_engine = create_engine(config['PostgreSQL']['sqlalchemy_engine_str'], echo=True)

connection = PostgreSQL_engine.connect()

query = """SELECT * FROM dataset_historical_popularity"""

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


def postgres2google(table_name, query):
    query = text(query)
    df = pd.read_sql_query(query, connection, parse_dates={'datetime': '%Y-%m-%d'})
    df= fix_datetypes(df)
    pandas_gbq.to_gbq(df, f'analytix.{table_name}', project_id=project_id, if_exists='append',
                      table_schema=get_google_schema(table_name))
    print(f'Table {table_name} has been import to Google BigQuery')

# postgres2google('dataset_historical_popularity', query)

# table_names = ['dataset_replicas']
# for i in table_names:
#     query = text(f'select * from {i}')
#     print(query)
#     df = pd.read_sql_query(query, connection, parse_dates={'datetime': '%Y-%m-%d'})
#     pandas_gbq.to_gbq(df, f'analytix.{i}', project_id='atlas-336515', if_exists='append',
#                       table_schema=)
#     print(f'Table {i} has been import to Google BigQuery')

client = bigquery.Client()


# query_job = client.query(
#     """
#     SELECT * FROM `atlas-336515.analytix.datasets_snapshot` WHERE DATE(_PARTITIONTIME) = "2022-01-21" LIMIT 1000"""
# )
#
# results = query_job.result()
#
# for row in results:
#         print(row)

def get_table_schema(project, dataset_id, table_id):
    dataset_ref = client.dataset(dataset_id, project=project)
    table_ref = dataset_ref.table(table_id)
    table = client.get_table(table_ref)

    f = io.StringIO("")
    client.schema_to_json(table.schema, f)
    print(f.getvalue())




# sql = """
# SELECT * FROM `atlas-336515.analytix.datasets_snapshot` WHERE DATE(_PARTITIONTIME) = "2022-01-21" LIMIT 1000
# """
# df = pandas_gbq.read_gbq(sql)
# print(df)
#
# pandas_gbq.to_gbq(df, 'digital_cases.test', project_id='atlas-336515', if_exists='append')


get_table_schema('atlas-336515', 'analytix', 'group_popularity')