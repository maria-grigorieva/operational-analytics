import urllib.parse
import requests
import pandas as pd
import configparser
import os

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
BASE_DIR = os.path.join(ROOT_DIR, '..' )


SQL_DIR = BASE_DIR+'/sql'

config = configparser.ConfigParser()
config.read(BASE_DIR+'/config.ini')


def enhance_queues():
    cric_base_url = config['CRIC']['cric_base_url']
    url_queue = urllib.parse.urljoin(cric_base_url, config['CRIC']['url_queue'])
    cric_queues = requests.get(url_queue, cert=(os.path.join(BASE_DIR, config['CRIC']['ssl_cert']),
                                                os.path.join(BASE_DIR, config['CRIC']['ssl_key'])),
                               verify=os.path.join(BASE_DIR, config['CRIC']['tls_ca_certificate'])).json()
    enhanced_queues = []

    for queue, attrs in cric_queues.items():
        #datadisks = [[d for d in v if 'DATADISK' in d or 'VP_DISK' in d] for k, v in attrs['astorages'].items() if 'write_lan' in k]
        datadisks = [[d for d in v if 'DATADISK' in d or 'VP_DISK' in d] for k, v in attrs['astorages'].items()]
        flat_datadisks = list(set([item for sublist in datadisks for item in sublist]))
        enhanced_queues.append({
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
            'region': attrs['region'],
            'rse': flat_datadisks or 'no rse'
        })

    enhanced_queues = pd.DataFrame(enhanced_queues)
    return enhanced_queues.explode('rse')


def enhance_sites():
    cric_base_url = config['CRIC']['cric_base_url']
    url_queue = urllib.parse.urljoin(cric_base_url, config['CRIC']['url_site'])
    cric_sites = requests.get(url_queue, cert=(os.path.join(BASE_DIR, config['CRIC']['ssl_cert']),
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