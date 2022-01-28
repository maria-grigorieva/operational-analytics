import urllib.parse
import requests
import pandas as pd
import configparser
import os
from sqlalchemy import create_engine, text, inspect
from database_helpers.helpers import insert_to_db, if_data_exists, day_rounder
from datetime import datetime

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
BASE_DIR = os.path.join(ROOT_DIR, '..' )


SQL_DIR = BASE_DIR+'/sql'

config = configparser.ConfigParser()
config.read(BASE_DIR+'/config.ini')

cric_base_url = config['CRIC']['cric_base_url']
url_queue = urllib.parse.urljoin(cric_base_url, config['CRIC']['url_queue'])
url_site = urllib.parse.urljoin(cric_base_url, config['CRIC']['url_site'])
url_queue_all = urllib.parse.urljoin(cric_base_url, config['CRIC']['url_queue_all'])
url_site_all = urllib.parse.urljoin(cric_base_url, config['CRIC']['url_site_all'])

PostgreSQL_engine = create_engine(config['PostgreSQL']['sqlalchemy_engine_str'], echo=True)


def enhance_queues(all=False, with_rse=False):

    cric_queues = requests.get(url_queue_all if all else url_queue,
                               cert=(os.path.join(BASE_DIR, config['CRIC']['ssl_cert']),
                                    os.path.join(BASE_DIR, config['CRIC']['ssl_key'])),
                               verify=os.path.join(BASE_DIR, config['CRIC']['tls_ca_certificate'])).json()
    enhanced_queues = []

    for queue, attrs in cric_queues.items():
        #datadisks = [[d for d in v if 'DATADISK' in d or 'VP_DISK' in d] for k, v in attrs['astorages'].items() if 'write_lan' in k]
        queues_dict = {
            'queue': queue,
            'site': attrs['rc_site'],
            'cloud': attrs['cloud'],
            'tier_level': attrs['tier_level'],
            'transferring_limit': attrs['transferringlimit'] or 2000,
            'status': attrs['status'],
            'state': attrs['state'],
            'resource_type': attrs['resource_type'],
            'nodes': attrs['nodes'],
            'corepower': attrs['corepower'],
            'corecount': attrs['corecount'],
            'region': attrs['region']
        }

        if with_rse:
            datadisks = [[d for d in v if 'DATADISK' in d or 'VP_DISK' in d] for k, v in attrs['astorages'].items()]
            flat_datadisks = list(set([item for sublist in datadisks for item in sublist]))
            queues_dict['rse'] = flat_datadisks or 'no rse'

        enhanced_queues.append(queues_dict)

    enhanced_queues = pd.DataFrame(enhanced_queues)
    if with_rse:
        return enhanced_queues.explode('rse')
    else:
        return enhanced_queues


def cric_resources_to_db(predefined_date = False):

    now = day_rounder(datetime.now()) if not predefined_date else predefined_date
    if not if_data_exists('cric_resources', now):
        result = enhance_queues()
        result['datetime'] = now
        insert_to_db(result, 'cric_resources', now, True)
    else:
        pass
    # try:
    #     current_state = "select * from cric_resources where datetime = (select max(datetime) from cric_resources)"
    #     current_state_df = pd.read_sql_query(current_state, postgres_connection, parse_dates={'datetime': '%Y-%m-%d'})
    #     current_state_df.drop('datetime',axis=1,inplace=True)
    #     if result.equals(current_state_df):
    #         return None
    #     else:
    #         diff = pd.concat([result, current_state_df]).drop_duplicates(keep=False)
    #         diff['datetime'] = pd.to_datetime('today')
    #
    #         diff.to_sql('cric_resources', postgres_connection,
    #                     if_exists='append',
    #                     method='multi',
    #                     index=False)
    # except Exception as e:
    #     result['datetime'] = pd.to_datetime('today')


def enhance_sites(all=False):
    # cric_base_url = config['CRIC']['cric_base_url']
    # url_queue = urllib.parse.urljoin(cric_base_url, config['CRIC']['url_site'])
    cric_sites = requests.get(url_site_all if all else url_site,
                              cert=(os.path.join(BASE_DIR, config['CRIC']['ssl_cert']),
                              os.path.join(BASE_DIR, config['CRIC']['ssl_key'])),
                            verify=os.path.join(BASE_DIR, config['CRIC']['tls_ca_certificate'])).json()
    enhanced_sites = []

    for site, attrs in cric_sites.items():
        enhanced_sites.append({
            'site': site,
            'latitude': attrs['latitude'],
            'longitude': attrs['longitude'],
            'tier_level': attrs['tier_level'],
            'cloud': attrs['cloud'],
            'corepower': attrs['corepower']
        })

    return pd.DataFrame(enhanced_sites)


def get_replicas_sites(list_of_ddm_endpoints):
    """
    :param cric_sites:
    :param list_of_ddm_endpoints:
    :return:
    """
    list_of_sites = []
    list_of_clouds = []
    nested = []
    for endpoint in list_of_ddm_endpoints:
        for key, value in requests.get(url_site_all,
                                       cert=(os.path.join(BASE_DIR, config['CRIC']['ssl_cert']),
                                             os.path.join(BASE_DIR, config['CRIC']['ssl_key'])),
                                       verify=os.path.join(BASE_DIR, config['CRIC']['tls_ca_certificate'])).json().items():
            ddm_endpoints = value['ddmendpoints']
            if endpoint in ddm_endpoints:
                site_info = {
                    "ddm_endpoint": endpoint,
                    "site_name": key,
                    "site_cloud": value.get("cloud", None),
                    "site_grid_flavour": value.get("grid_flavour", None),
                    "site_state": value.get("state", None),
                    "site_location": f"{value.get('latitude', '0')},{value.get('longitude', '0')}" or "0,0",
                    "site_corepower": value.get("corepower", None),
                    "tier_level": value.get('rc_tier_level') or value['tier_level']
                }
                nested.append(site_info)
                list_of_sites.append(key)
                list_of_clouds.append(value.get('cloud', None))
                break
    return list(set(list_of_sites)), list(set(list_of_clouds)), nested


#cric_resources_to_db('2022-01-17')