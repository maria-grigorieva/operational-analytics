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
from database_helpers.helpers import insert_to_db
from datetime import datetime, timedelta


config = configparser.ConfigParser()
config.read(BASE_DIR+'/config.ini')

PostgreSQL_engine = create_engine(config['PostgreSQL']['sqlalchemy_engine_str'], echo=False)

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
    from_date = datetime.strftime(datetime.now(),"%Y-%m-%d")
    df['datetime'] = from_date

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

    insert_to_db(dst, 'distances')

# get_distances()