import os, sys
import pandas as pd
from sqlalchemy import create_engine, text
import configparser
import numpy as np

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
BASE_DIR = os.path.join(ROOT_DIR, '..' )
sys.path.append(os.path.abspath(BASE_DIR))

SQL_DIR = BASE_DIR+'/sql'
print(SQL_DIR)

config = configparser.ConfigParser()
config.read(BASE_DIR+'/config.ini')

PostgreSQL_engine = create_engine(config['PostgreSQL']['sqlalchemy_engine_str'], echo=True)

def calculate_weights(datasetname):
    postgres_connection = PostgreSQL_engine.connect()
    query = text(open(SQL_DIR + '/postgreSQL/merging.sql').read())
    df = pd.read_sql_query(query, postgres_connection, parse_dates={'datetime': '%Y-%m-%d'}, params={'ds_name':datasetname})
    df.drop('Storage Timestamp', axis=1, inplace=True)
    df.set_index(['queue', 'rse', 'site', 'cloud', 'tier_level', 'datetime', 'src','dest',
                  'queue_type','state','status','resource_type','region','datasetname', 'timestamp'],inplace=True)
    norm_df = df.apply(lambda x: round((x - np.mean(x)) / (np.max(x) - np.min(x)), 3))
    norm_df[np.isnan(norm_df)] = 0
    norm_df.reset_index(inplace=True)

    norm_df['rse_weight'] = norm_df['queue_efficiency'] + \
                            norm_df['queue_occupancy'] + \
                            norm_df['Difference'] + \
                            norm_df['Unlocked'] + \
                            norm_df['closeness'] + \
                            norm_df['queue_service_quality']

    df.reset_index(inplace=True)

    df['rse_weight'] = round(norm_df['rse_weight'], 3)

    df.to_sql('resource_weights', postgres_connection,
                  if_exists='append',
                  method='multi',
                  index=False)


calculate_weights('data16_13TeV:data16_13TeV.00297730.physics_Main.deriv.DAOD_TOPQ1.r9264_p3083_p4513_tid25513194_00')





