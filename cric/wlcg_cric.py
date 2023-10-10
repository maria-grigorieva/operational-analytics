import os, sys
ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
BASE_DIR = os.path.join(ROOT_DIR, '..' )
sys.path.append(os.path.abspath(BASE_DIR))
import urllib.parse
import requests
import pandas as pd
import configparser
import os
from sqlalchemy import create_engine, text, inspect
from database_helpers.helpers import insert_to_db, check_for_data_existance, day_rounder
from datetime import datetime
import ssl
import urllib3
import json

SQL_DIR = BASE_DIR+'/sql'

config = configparser.ConfigParser()
config.read(BASE_DIR+'/config.ini')

wlcg_cric_url = config['CRIC']['wlcg_cric_url']

PostgreSQL_engine = create_engine(config['PostgreSQL']['sqlalchemy_engine_str'], echo=False)

http = urllib3.PoolManager(
    cert_file=os.path.join(BASE_DIR, config['CRIC']['ssl_cert']),
    cert_reqs="CERT_REQUIRED",
    key_file=os.path.join(BASE_DIR, config['CRIC']['ssl_key']),
    key_password=config['CRIC']['cert_pwd'],
    ca_certs=os.path.join(BASE_DIR, config['CRIC']['tls_ca_certificate'])
)

def get_sites_info():

    response = http.request('GET',wlcg_cric_url)
    data = response.data
    sites = json.loads(data)
    sites_list = []

    for site, attrs in sites.items():
        sites_list.append({
            'site': site,
            'cores': attrs['cores'],
            'cpu_capacity': attrs['cpu_capacity'],
            'slots': attrs['slots'],
            'coreenergy': attrs['coreenergy'],
            'corepower': attrs['corepower']
        })

    sites_df = pd.DataFrame(sites_list)
    sites_df.to_csv('sites_attrs_v1.csv')
    return sites_df


