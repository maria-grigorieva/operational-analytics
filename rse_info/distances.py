import requests
import pandas as pd
import datetime as dt

def get_distances():
    distances_url = 'http://atlas-adc-netmetrics-lb.cern.ch/metrics/latest.json'
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
    df['datetime'] = dt.datetime.today().strftime("%m-%d-%Y")
    return df
