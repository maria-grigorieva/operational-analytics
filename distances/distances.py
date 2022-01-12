import requests
import pandas as pd
import datetime as dt
import os,sys
ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
BASE_DIR = os.path.join(ROOT_DIR, '..' )
sys.path.append(os.path.abspath(BASE_DIR))
from sqlalchemy import create_engine, text, inspect
import configparser
from cric import cric_json_api

config = configparser.ConfigParser()
config.read(BASE_DIR+'/config.ini')

PostgreSQL_engine = create_engine(config['PostgreSQL']['sqlalchemy_engine_str'], echo=True)

def get_distances():
    distances_url = config['Rucio API']['sites_distances']
    distances = requests.get(distances_url).json()
    dist = []
    for k,v in distances.items():
        src, dst = k.split(':')
        if 'closeness' in v:
            closeness = v['closeness']['latest']
            dist.append({'src': src,
                         'dest': dst,
                         'closeness': closeness})
    df = pd.DataFrame(dist)
    df['datetime'] = dt.datetime.today().strftime("%d-%m-%Y")

    sites_info = cric_json_api.enhance_sites()

    src = pd.merge(df, sites_info, left_on='src', right_on='site')
    src.rename(columns={'latitude':'src_latitude',
                        'longitude': 'src_longitude',
                        'cloud': 'src_cloud',
                        'tier_level': 'src_tier_level',
                        'corepower': 'src_corepower'}, inplace=True)
    src.drop('site',axis=1,inplace=True)
    dst = pd.merge(src, sites_info, left_on='dest', right_on='site')
    dst.rename(columns={'latitude':'dest_latitude',
                        'longitude': 'dest_longitude',
                        'cloud': 'dest_cloud',
                        'tier_level': 'dest_tier_level',
                        'corepower': 'dest_corepower'}, inplace=True)
    dst.drop('site', axis=1, inplace=True)

    postgres_connection = PostgreSQL_engine.connect()
    if inspect(PostgreSQL_engine).has_table('distances'):

        with postgres_connection.begin():
            if postgres_connection.execute(text(f'SELECT distinct datetime from distances '
                                                f'where datetime = \'{dt.datetime.today().strftime("%m-%d-%Y")}\'')).first() is not None:
                # delete those rows that we are going to "upsert"
                postgres_connection.execute(text(f'delete from distances '
                                                 f'where datetime = \'{dt.datetime.today().strftime("%m-%d-%Y")}\''))

    # insert changed rows
    dst.to_sql('distances', postgres_connection,
                  if_exists='append',
                  method='multi',
                  index=False)

get_distances()