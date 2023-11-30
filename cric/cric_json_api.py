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
from database_helpers.helpers import check_postgreSQL, write_to_postgreSQL, day_rounder, delete_from_pgsql
from datetime import datetime
import ssl
import urllib3
import json
# from wlcg_cric import get_sites_info

SQL_DIR = BASE_DIR+'/sql'

config = configparser.ConfigParser()
config.read(BASE_DIR+'/config.ini')

cric_base_url = config['CRIC']['cric_base_url']
url_queue = urllib.parse.urljoin(cric_base_url, config['CRIC']['url_queue'])
url_site = urllib.parse.urljoin(cric_base_url, config['CRIC']['url_site'])
url_queue_all = urllib.parse.urljoin(cric_base_url, config['CRIC']['url_queue_all'])
url_site_all = urllib.parse.urljoin(cric_base_url, config['CRIC']['url_site_all'])

PostgreSQL_engine = create_engine(config['PostgreSQL']['sqlalchemy_engine_str'], echo=False)

http = urllib3.PoolManager(
    cert_file=os.path.join(BASE_DIR, config['CRIC']['ssl_cert']),
    cert_reqs="CERT_REQUIRED",
    key_file=os.path.join(BASE_DIR, config['CRIC']['ssl_key']),
    key_password=config['CRIC']['cert_pwd'],
    ca_certs=os.path.join(BASE_DIR, config['CRIC']['tls_ca_certificate'])
)

# def atlas_wlcg_sites_info():
#
#     response = http.request('GET', url_queue_all if all else url_queue)
#     data = response.data
#     cric_queues = json.loads(data)
#     queues = []
#
#     for queue, attrs in cric_queues.items():
#         queues.append({
#             'queue': queue,
#             'site': attrs['rc_site'],
#             'cloud': attrs['cloud'],
#             'tier_level': attrs['tier_level'],
#             'resource_type': attrs['resource_type']
#         })
#     queues_df = pd.DataFrame(queues)
#
#     wlcg_sites_df = get_sites_info()
#
#     result_df = queues_df.merge(wlcg_sites_df, how='left', on='site')
#     result_df.to_csv('sites_attrs.csv')


def extract_architecture_specs(url='https://atlas-cric.cern.ch/api/atlas/pandaqueue/query/?json&preset=tags'):
    response = http.request('GET',url)
    data = response.data
    cric_queues = json.loads(data)

    enhanced_queues = []
    for key, value in cric_queues.items():
        if value.get('architectures'):
            architectures = value['architectures'][0]
            arch = architectures['arch'][0] if architectures.get('arch') else 'unknown'
            instr = architectures['instr'][0] if architectures.get('instr') else 'unknown'
            type_value = architectures['type'] if architectures.get('type') else 'unknown'
            queues_dict = {'queue': key,
                           'architecture': arch,
                           'instruction': instr,
                           'processor_type': type_value}
        else:
            queues_dict = {'queue': key,
                           'architecture': 'unknown',
                           'instruction': 'unknown',
                           'processor_type': 'unknown'}
        enhanced_queues.append(queues_dict)

    return pd.DataFrame(enhanced_queues)


def queues_sites():
    cric_queues = json.loads(http.request('GET',url_queue_all if all else url_queue).data)
    return [{'queue': queue, 'site': attrs['atlas_site']} for queue, attrs in cric_queues.items()]


def enhance_queues(all=False, with_rse=False):

    # cric_queues = requests.get(url_queue_all if all else url_queue,
    #                            cert=(os.path.join(BASE_DIR, config['CRIC']['ssl_cert']),
    #                                 os.path.join(BASE_DIR, config['CRIC']['ssl_key'])),
    #                            verify=False).json()
    response = http.request('GET',url_queue_all if all else url_queue)
    data = response.data
    cric_queues = json.loads(data)
                               # os.path.join(BASE_DIR, config['CRIC']['tls_ca_certificate'])).json()
    enhanced_queues = []

    for queue, attrs in cric_queues.items():
        transferringlimit = attrs['transferringlimit'] if attrs.get('transferringlimit') else 2000
        region = attrs['region'] if attrs.get('region') else 'unknown'
        #datadisks = [[d for d in v if 'DATADISK' in d or 'VP_DISK' in d] for k, v in attrs['astorages'].items() if 'write_lan' in k]
        queues_dict = {
            'queue': queue,
            'site': attrs['atlas_site'],
            'cloud': attrs['cloud'],
            'tier_level': attrs['tier_level'],
            'transferring_limit': transferringlimit,
            'status': attrs['status'],
            'state': attrs['state'],
            'resource_type': attrs['resource_type'],
            'nodes': attrs['nodes'],
            'corepower': attrs['corepower'],
            'corecount': attrs['corecount'],
            'region': region
        }

        if with_rse:
            datadisks = [[d for d in v if 'DATADISK' in d or 'VP_DISK' in d] for k, v in attrs['astorages'].items()]
            flat_datadisks = list(set([item for sublist in datadisks for item in sublist]))
            queues_dict['rse'] = flat_datadisks or 'no rse'

        enhanced_queues.append(queues_dict)

    enhanced_queues = pd.DataFrame(enhanced_queues)

    return enhanced_queues.explode('rse') if with_rse else enhanced_queues


def cric_resources_to_db(predefined_date = False):

    from_date = datetime.strftime(datetime.now(), "%Y-%m-%d %H:%M:%S") if not predefined_date else str(predefined_date)

    if not check_postgreSQL('cric_resources', from_date, accuracy='day', datetime_col_name='datetime'):
        result = enhance_queues(with_rse=True)
        result['datetime'] = day_rounder(datetime.strptime(from_date, "%Y-%m-%d %H:%M:%S"))
        int_columns = result.select_dtypes(include=['int', 'float']).columns
        result[int_columns] = result[int_columns].fillna(0)
        result['datetime'] = pd.to_datetime(result['datetime'])
        result = result.astype({'nodes': 'int64',
                                  'transferring_limit': 'int64',
                                  'tier_level': 'int64',
                                  'corepower': 'float64',
                                  'corecount': 'float64',
                                  'datetime': 'datetime64'
                                  })
        write_to_postgreSQL(result, 'cric_resources')
    else:
        delete_from_pgsql('cric_resources', from_date, accuracy='day', datetime_col_name='datetime')

def enhance_sites(all=False):
    # cric_base_url = config['CRIC']['cric_base_url']
    # url_queue = urllib.parse.urljoin(cric_base_url, config['CRIC']['url_site'])
    response = http.request('GET',url_site_all if all else url_site)
    data = response.data
    cric_sites = json.loads(data)
    # cric_sites = requests.get(url_site_all if all else url_site,
    #                           cert=(os.path.join(BASE_DIR, config['CRIC']['ssl_cert']),
    #                           os.path.join(BASE_DIR, config['CRIC']['ssl_key'])),
    #                         verify=os.path.join(BASE_DIR, config['CRIC']['tls_ca_certificate'])).json()
    enhanced_sites = []

    for site, attrs in cric_sites.items():
        enhanced_sites.append({
            'site': site,
            'latitude': attrs['latitude'],
            'longitude': attrs['longitude'],
            'tier_level': attrs['tier_level'],
            'cloud': attrs['cloud'],
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
        response = http.request('GET', url_site_all)
        data = response.data
        for key, value in json.loads(data).items():
        # for key, value in requests.get(url_site_all,
        #                                cert=(os.path.join(BASE_DIR, config['CRIC']['ssl_cert']),
        #                                      os.path.join(BASE_DIR, config['CRIC']['ssl_key'])),
        #                                verify=os.path.join(BASE_DIR, config['CRIC']['tls_ca_certificate'])).json().items():
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
