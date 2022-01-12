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
from cric.cric_json_api import get_replicas_sites
ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
BASE_DIR = os.path.join(ROOT_DIR, '..' )
sys.path.append(os.path.abspath(BASE_DIR))
config = configparser.ConfigParser()
config.read(BASE_DIR+'/config.ini')

os.environ['X509_USER_PROXY'] = config['RUCIO']['x509_user_proxy']
os.environ['RUCIO_ACCOUNT'] = config['RUCIO']['account']
os.environ['RUCIO_AUTH_TYPE'] = config['RUCIO']['auth_type']


def extract_scope(dataset: str):
    if ':' in dataset:
        tmp = dataset.split(':')
        scope, name = tmp[0], tmp[1]
    else:
        tmp = dataset.split('.')
        scope, name = tmp[0], dataset
    return scope, name

def get_dataset_info(dataset: str):

    scope, name = extract_scope(dataset)

    try:
        CLIENT = Client()

        try:
            metadata = CLIENT.get_metadata(scope, name)
            data_type, data_type_desc = split_data_type(metadata['datatype'])
            metadata['datasetname'] = dataset
            metadata['data_type'] = data_type
            metadata['data_type_desc'] = data_type_desc
            metadata_df = pd.DataFrame([metadata])
            metadata_df.drop(['updated_at','created_at','accessed_at','closed_at','scope','length','bytes','account','did_type','is_open',
                              'monotonic','hidden','complete','availability','md5','adler32','expired_at','purge_replicas',
                              'guid','eol_at','task_id','panda_id','is_archive','constituent','transient'],axis=1,inplace=True)

            try:
                replicas = CLIENT.list_dataset_replicas(scope=scope, name=name, deep=True)
                replicas = pd.DataFrame(replicas)

                try:
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
                    rules_df['replica_type'] = rules_df['expires_at'].apply(
                        lambda x: 'primary' if x is None else 'secondary')
                    cols_to_use = rules_df.columns.difference(replicas.columns)
                    all_replicas = pd.merge(replicas, rules_df[cols_to_use], left_on='rse', right_on='rse_expression',
                                            how='outer')
                    all_replicas['available_terabytes'] = round(all_replicas['available_bytes'] / 1073741824 / 1024, 4)
                    all_replicas['terabytes'] = round(all_replicas['bytes'] / 1073741824 / 1024, 4)

                    result = pd.merge(all_replicas, metadata_df, left_on='name', right_on='name')
                    result['replica_type'].fillna('tmp', inplace=True)
                    # result.drop(['child_rule_id', 'comments',
                    #              'eol_at', 'error', 'grouping', 'id', 'ignore_account_limit',
                    #              'ignore_availability', 'locked', 'locks_ok_cnt',
                    #              'locks_replicating_cnt', 'locks_stuck_cnt', 'meta',
                    #              'notification', 'priority', 'purge_replicas', 'rse_expression',
                    #              'rule_id', 'source_replica_expression', 'stuck_at', 'subscription_id',
                    #              'weight'], axis=1, inplace=True)

                    return result
                except Exception as e:
                    print(e)
            except Exception as e:
                print(e)
        except Exception as e:
            print(e)
    except Exception as e:
        print(e)



def update_from_rucio(dataset: str):
    """
    Add the metainformation from Rucio about datasets
    - dataset metadata
    - dataset replicas (DDM endpoints)
    """

    scope, name = extract_scope(dataset)

    CLIENT = Client()
    try:
        metadata = CLIENT.get_metadata(scope, name)
        data_type, data_type_desc = split_data_type(metadata['datatype'])
        metadata['datasetname'] = dataset
        metadata['data_type'] = data_type
        metadata['data_type_desc'] = data_type_desc
        ds_replicas = list(CLIENT.list_dataset_replicas(scope=scope, name=name))
        metadata['replicas_number'] = len(ds_replicas)
        metadata['replicas_ddm'] = ','.join([i['rse'] for i in ds_replicas]).strip(",")
        replicas, clouds, nested = get_replicas_sites([i['rse'] for i in ds_replicas])
        metadata['replicas_sites'] = ','.join(replicas).strip(",")
        metadata['replicas_clouds'] = ','.join(clouds).strip(",")
        # metadata['replicas_info'] = nested
        print(f'{dataset} has been found in Rucio')
        return metadata
    except Exception as e:
        print(f'Can\'t find {dataset} in Rucio')
        pass


def split_data_type(data_type):
    data_type, data_type_desc = data_type.split("_") if len(data_type.split("_")) > 1 else (data_type, "")
    return data_type, data_type_desc



#res = get_dataset_info('data16_13TeV:data16_13TeV.00299584.physics_Main.deriv.DAOD_TOPQ1.r9264_p3083_p4513_tid25513236_00')

#print(res.to_dict('records'))
#print(update_from_rucio('mc16_13TeV:mc16_13TeV.830027.H7EG_jetjet_dipole_JZ7.recon.AOD.e7954_e7400_s3126_r10244_tid26730472_00'))
