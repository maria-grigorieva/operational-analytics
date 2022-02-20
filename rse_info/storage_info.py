import os, sys
import requests
import pandas as pd
# import datetime as dt
from datetime import datetime, timedelta
from database_helpers.helpers import insert_to_db, check_for_data_existance, day_rounder, set_time_period

# All numbers in TB. Click on table headers to sort.
# Used (other) is other RSEs sharing the same space token.
# Quota (other) is the sum of quotas defined on other RSEs sharing the same space token.
# Used (dark) is the difference between Storage used and Rucio used for all the RSEs on the space token.
# Unlocked is data eligible for deletion.
# Min space is a limit dynamically set by a collector based on storage capacity of the endpoint (Total(storage) - Used(other)) and according to the following policy:
#
# DATADISK: Min(10% or 300TB free)
# SCRATCHDISK: Min(25% or 50TB free)
# TAPE: 10TB (arbitrary limit for these tables - actual cleaning depends on the garbage collection algorithm on each site)
# Primary diff - amount of primaries (UsedRucio-Unlocked) over a threshold: (Total(storage) - GroupQuota)*0.600000)
# Avg tombstone - days between now and timestamp of average tombstone, giving an indication of the turnover of secondary data


def get_agg_storage_data():
    agg_rucio_url = 'http://adc-ddm-mon.cern.ch/ddmusr01/all_storage_data.json'
    disk_sizes = requests.get(agg_rucio_url).json()
    df = pd.DataFrame(disk_sizes)
    df = df.T
    df.reset_index(inplace=True)
    df.rename(columns={'index':'rse'}, inplace=True)
    cols = df.columns.drop(['rse','Storage Timestamp'])
    df[cols] = df[cols].apply(pd.to_numeric, errors='coerce')
    return df


def save_storage_attrs_to_db(predefined_date = False):

    now = datetime.strftime(datetime.now(), "%Y-%m-%d %H:%M:%S") if not predefined_date else str(predefined_date)
    if not check_for_data_existance('storage_info', now, delete=True):
        result = get_agg_storage_data()
        result['datetime'] = day_rounder(datetime.strptime(now, "%Y-%m-%d %H:%M:%S"))
        result.rename(columns={'Avg tombstone':'avg_tombstone', 'Min space':'min_space', 'Free(storage)':'free_storage',
                               'Primary diff': 'primary_diff', 'Quota(other)':'quota_other', 'Storage Timestamp': 'storage_timestamp',
                               'Total(storage)': 'total_storage', 'Used(dark)': 'used_dark', 'Used(other)': 'used_other',
                               'Used(rucio)': 'used_rucio', 'Difference': 'difference', 'Persistent': 'persistent',
                               'Temporary': 'temporary', 'Unlocked': 'unlocked'}, inplace=True)
        insert_to_db(result, 'storage_info')
    else:
        pass

#
# save_storage_attrs_to_db('2022-02-01 01:00:00')