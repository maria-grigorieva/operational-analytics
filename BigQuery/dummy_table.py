import os, sys
ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
BASE_DIR = os.path.join(ROOT_DIR, '..' )
import pandas as pd
from google.cloud import bigquery
import configparser

config = configparser.ConfigParser()
config.read(BASE_DIR+'/config.ini')

os.environ["GOOGLE_APPLICATION_CREDENTIALS"]=config['GOOGLE']['app']

records =[
    {
        "Name": "Alex",
        "Age": 25,
        "City":"New York"
    },
    {
        "Name": "Bryan",
        "Age": 27,
        "City":"San Francisco"

    }
]

dataframe = pd.DataFrame(
    records,columns=["Name","Age","City"])

print(dataframe)

#to_gbq
dataframe.to_gbq('test.pandas_bq_test',project_id='atlas-336515',if_exists='append')
