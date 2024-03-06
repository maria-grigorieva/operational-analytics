import urllib.parse
import requests
import pandas as pd
import configparser
import os
from sqlalchemy import create_engine, text, inspect
from database_helpers.helpers import write_to_postgreSQL, check_for_data_existance, day_rounder
from datetime import datetime
import ssl
import urllib3
import json

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
BASE_DIR = os.path.join(ROOT_DIR, '..' )


SQL_DIR = BASE_DIR+'/sql'

config = configparser.ConfigParser()
config.read(BASE_DIR+'/config.ini')

import json
import pandas as pd
import pprint
import requests

import logging

logging.basicConfig()
logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO)

#conf_notes_url = 'https://inspirehep.net/api/literature?size=1000&fields=publication_info.conference_record,publication_info.cnum&doc_type=conference%20paper&collaboration=ATLAS&subject=Experiment-HEP&page=1&q=&earliest_date=2022--2024'
conf_notes_url = 'https://inspirehep.net/api/literature?size=1000&fields=publication_info.conference_record,publication_info.cnum&doc_type=conference%20paper&collaboration=ATLAS&page=1&q=&earliest_date=2021--2024'

PostgreSQL_engine = create_engine(config['PostgreSQL']['sqlalchemy_engine_str'], echo=False)


def get_atlas_conferences():
    conf_notes = requests.get(conf_notes_url).json()
    root = conf_notes['hits']['hits']
    conferences = []
    for i in root:
        if 'metadata' in i:
            if 'publication_info' in i['metadata']:
                pub_info = i['metadata']['publication_info'][0]
                if 'conference_record' in pub_info:
                    url = pub_info['conference_record']['$ref']
                    if not any(c['url'] == url for c in conferences):
                        new_conf_record = {}
                        new_conf_record['url'] = url
                        new_conf_record['n_papers'] = 1
                        conferences.append(new_conf_record)
                    else:
                        d = next(c for c in conferences if c['url'] == url)
                        d['n_papers'] += 1
    df = pd.DataFrame(conferences)
    df.to_csv('atlas_conferences.csv')


def conferences_to_db():

    conf_notes = requests.get(conf_notes_url).json()

    root = conf_notes['hits']['hits']

    conferences = []
    for i in root:
        if 'metadata' in i:
            if 'publication_info' in i['metadata']:
                pub_info = i['metadata']['publication_info'][0]
                if 'conference_record' in pub_info:
                    url = pub_info['conference_record']['$ref']
                    if not any(c['url'] == url for c in conferences):
                        new_conf_record = {}
                        new_conf_record['url'] = url
                        new_conf_record['n_papers'] = 1
                        conferences.append(new_conf_record)
                    else:
                        d = next(c for c in conferences if c['url'] == url)
                        d['n_papers'] += 1

    for i in conferences:
        print(i['url'])
        data = requests.get(i['url']).json()
        i['series'] = data['metadata']['series'][0]['name'] if 'series' in data['metadata'] else ''
        i['number'] = data['metadata']['series'][0]['number'] if 'number' in data['metadata'] else ''
        i['title'] = data['metadata']['titles'][0]['title']
        i['opening_date'] = data['metadata']['opening_date']
        i['closing_date'] = data['metadata']['closing_date']
        i['acronym'] = data['metadata']['acronyms'][0] if 'acronyms' in data['metadata'] else ''

    df = pd.DataFrame(conferences)
    datetime_cols = ['opening_date','closing_date']
    for d in datetime_cols:
        df[d] = pd.to_datetime(df[d], infer_datetime_format=True, errors='ignore')
    #df.to_csv('ihep_conf_2022_2023.csv')
    write_to_postgreSQL(df, 'ihep_conferences')


# conferences_to_db()
#get_atlas_conferences()