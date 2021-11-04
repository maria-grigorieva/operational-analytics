import requests
import pandas as pd
import datetime as dt

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
    #df = df[df['rse'].str.contains('DATADISK')]
    cols = df.columns.drop(['rse','Storage Timestamp'])
    df[cols] = df[cols].apply(pd.to_numeric, errors='coerce')
    return df
