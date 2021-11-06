"""
Get replicas for a dataset:
Input Parameters:
-----------------
x509_user_proxy: i.e. ./proxy,
account: <account>,
auth_type: i.e. x509_proxy,
dataset: <dataset name>
"""
import pandas as pd
import datetime as dt
import configparser
import os, sys
from rucio.client import Client
ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
BASE_DIR = os.path.join(ROOT_DIR, '..' )
sys.path.append(os.path.abspath(BASE_DIR))
config = configparser.ConfigParser()
config.read(BASE_DIR+'/config.ini')


def get_dataset_info(dataset: str):

    os.environ['X509_USER_PROXY'] = config['RUCIO']['x509_user_proxy']
    os.environ['RUCIO_ACCOUNT'] = config['RUCIO']['account']
    os.environ['RUCIO_AUTH_TYPE'] = config['RUCIO']['auth_type']

    if ':' in dataset:
        tmp = dataset.split(':')
        scope, name = tmp[0], tmp[1]
    else:
        tmp = dataset.split['.']
        scope, name = tmp[0], dataset

    CLIENT = Client()
    replicas = CLIENT.list_dataset_replicas(scope=scope, name=name, deep=True)
    replicas = pd.DataFrame(replicas)
    rules = pd.DataFrame(list(CLIENT.list_replication_rule_full_history(scope=scope, name=name)))
    rule_details = []
    for id in rules['rule_id'].values:
        try:
            rule_details.append(CLIENT.get_replication_rule(id))
        except Exception as e:
            print(e)
    rule_details = pd.DataFrame(rule_details)
    cols_to_use = rule_details.columns.difference(rules.columns)
    rules_df = pd.merge(rules, rule_details[cols_to_use], left_on='rule_id', right_on='id')
    rules_df['official'] = rules_df['expires_at'].isnull()
    rules_df['has_rule'] = True
    cols_to_use = rules_df.columns.difference(replicas.columns)
    all_replicas = pd.merge(replicas, rules_df[cols_to_use], left_on='rse', right_on='rse_expression', how='outer')
    all_replicas['timestamp'] =  dt.datetime.today().strftime("%m-%d-%Y")
    all_replicas['available_TB'] = round(all_replicas['available_bytes']/1073741824/1024, 4)
    all_replicas['TB'] = round(all_replicas['bytes']/1073741824/1024, 4)

    rse_info = []
    for rse in set(all_replicas['rse'].values):
        attrs = CLIENT.list_rse_attributes(rse)
        attrs['rse'] = attrs.pop(rse)
        attrs['rse'] = rse
        rse_info.append(attrs)
    rse_info = pd.DataFrame(rse_info)

    rse_info = rse_info[['rse', 'cloud', 'site', 'tier', 'freespace']]
    result = pd.merge(all_replicas, rse_info, left_on='rse', right_on='rse')
    result['official'].fillna(False, inplace=True)
    result['has_rule'].fillna(False, inplace=True)
    result.drop(['rse_id','available_bytes','bytes','child_rule_id','comments',
                 'eol_at','error','grouping','id','ignore_account_limit',
                 'ignore_availability','locked','locks_ok_cnt',
                 'locks_replicating_cnt','locks_stuck_cnt','meta',
                 'notification','priority','purge_replicas','rse_expression',
                 'rule_id','source_replica_expression','stuck_at','subscription_id',
                 'weight'], axis=1, inplace=True)
    return result

res = get_dataset_info('data16_13TeV:data16_13TeV.00311481.physics_Main.deriv.DAOD_TOPQ1.r9264_p3083_p4513_tid25513587_00')
print(res)