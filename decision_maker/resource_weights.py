import os, sys
import pandas as pd
from sqlalchemy import create_engine, text
import configparser
import numpy as np
from database_helpers.helpers import insert_to_db, check_for_data_existance, set_time_period, day_rounder
from sklearn.preprocessing import StandardScaler

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
BASE_DIR = os.path.join(ROOT_DIR, '..' )
sys.path.append(os.path.abspath(BASE_DIR))

from datetime import datetime, timedelta

SQL_DIR = BASE_DIR+'/sql'
print(SQL_DIR)

config = configparser.ConfigParser()
config.read(BASE_DIR+'/config.ini')

PostgreSQL_engine = create_engine(config['PostgreSQL']['sqlalchemy_engine_str'], echo=True)

def calculate_weights(predefined_date = False):

    from_date, to_date = set_time_period(predefined_date, n_hours=24)

    if not check_for_data_existance('resource_weight', from_date, delete=True):

        postgres_connection = PostgreSQL_engine.connect()
        query = text(open(SQL_DIR + '/postgreSQL/resource_weight.sql').read())
        df = pd.read_sql_query(query, postgres_connection, parse_dates={'datetime': '%Y-%m-%d'}, params={'now':from_date})
        postgres_connection.close()
        df.set_index(['dest_queue', 'dest_rse', 'dest_site', 'dest_cloud', 'src_site', 'src_cloud', 'dest_tier_level', 'datetime'],inplace=True)
        df['closeness_'] = 11 - df['closeness']
        norm_df = df.apply(lambda x: round((x - np.mean(x)) / (np.max(x) - np.min(x)), 3))
        norm_df[np.isnan(norm_df)] = 0
        norm_df.reset_index(inplace=True)

        norm_df['rse_weight'] = norm_df['queue_efficiency'] + \
                                norm_df['difference'] + \
                                norm_df['closeness_'] + \
                                norm_df['utilization_diff'] + \
                                norm_df['fullness_diff'] + \
                                norm_df['queue_time_diff']

        df.reset_index(inplace=True)

        df['rse_weight'] = round(norm_df['rse_weight'], 3)

        df.drop(['closeness_'], axis=1, inplace=True)

        insert_to_db(df, 'resource_weight')
    else:
        pass


def calculate_weights_overall(predefined_date = False):

    from_date, to_date = set_time_period(predefined_date, n_hours=24)

    if not check_for_data_existance('resource_weight_overall', from_date, delete=True):

        postgres_connection = PostgreSQL_engine.connect()
        query = text(open(SQL_DIR + '/postgreSQL/resource_weight_overall.sql').read())
        df = pd.read_sql_query(query, postgres_connection, parse_dates={'datetime': '%Y-%m-%d'}, params={'now':from_date})
        postgres_connection.close()
        df.set_index(['queue', 'rse', 'site', 'cloud', 'tier_level', 'datetime'], inplace=True)
        idx = df.index
        cols = df.columns
        # scaler = StandardScaler()
        # scaled = scaler.fit_transform(df)
        scaled_df = df.apply(lambda x: round((x - np.mean(x)) / (np.max(x) - np.min(x)), 3))
        scaled_df[np.isnan(scaled_df)] = 0
        # scaled_df = pd.DataFrame(scaled, index=idx, columns=cols)
        scaled_df.reset_index(inplace=True)

        scaled_df['resource_weight'] = scaled_df['queue_efficiency'] + \
                                scaled_df['difference'] + \
                                scaled_df['utilization_diff'] + \
                                scaled_df['fullness_diff'] + \
                                scaled_df['queue_time_diff'] + \
                                scaled_df['daily_jobs_diff'] + \
                                scaled_df['corepower']


        df.reset_index(inplace=True)

        df['resource_weight'] = round(scaled_df['resource_weight'], 3)

        insert_to_db(df, 'resource_weight_overall')
    else:
        pass



def calculate_queue_weights(predefined_date = False):

    from_date, to_date = set_time_period(predefined_date, n_hours=24)

    if not check_for_data_existance('queue_weights', from_date, delete=True):

        postgres_connection = PostgreSQL_engine.connect()
        query = text(open(SQL_DIR + '/postgreSQL/queue_weight.sql').read())
        df = pd.read_sql_query(query, postgres_connection, parse_dates={'datetime': '%Y-%m-%d'}, params={'now':from_date})
        postgres_connection.close()
        df.set_index(['queue', 'site', 'cloud', 'tier_level', 'datetime'],inplace=True)
        norm_df = df.apply(lambda x: round((x - np.mean(x)) / (np.max(x) - np.min(x)), 3))
        norm_df[np.isnan(norm_df)] = 0
        norm_df.reset_index(inplace=True)


        norm_df['queue_weight'] = norm_df['queue_efficiency'] + \
                                norm_df['utilization_diff'] + \
                                norm_df['fullness_diff'] + \
                                norm_df['queue_time_diff'] + \
                                norm_df['daily_jobs_number']

        df.reset_index(inplace=True)

        df['queue_weight'] = round(norm_df['queue_weight'], 3)

        insert_to_db(df, 'queue_weights')
    else:
        pass
# def calculate_weights(datasetname):
#     postgres_connection = PostgreSQL_engine.connect()
#     query = text(open(SQL_DIR + '/postgreSQL/merging.sql').read())
#     df = pd.read_sql_query(query, postgres_connection, parse_dates={'datetime': '%Y-%m-%d'}, params={'ds_name':datasetname})
#     df.drop('Storage Timestamp', axis=1, inplace=True)
#     df.set_index(['queue', 'rse', 'site', 'cloud', 'tier_level', 'datetime', 'src','dest',
#                   'queue_type','state','status','resource_type','region','datasetname', 'timestamp'],inplace=True)
#     df['queue_utilization_'] = round(1/df['queue_utilization'],4)
#     df['closeness_'] = round(1 / df['closeness'],4)
#     df['queue_filling_'] = round(1 / df['queue_filling'],4)
#     norm_df = df.apply(lambda x: round((x - np.mean(x)) / (np.max(x) - np.min(x)), 3))
#     norm_df[np.isnan(norm_df)] = 0
#     norm_df.reset_index(inplace=True)
#
#     norm_df['rse_weight'] = norm_df['queue_efficiency'] + \
#                             norm_df['queue_utilization_'] + \
#                             norm_df['Difference'] + \
#                             norm_df['Unlocked'] + \
#                             norm_df['closeness_'] + \
#                             norm_df['queue_filling_']
#
#     df.reset_index(inplace=True)
#
#     df['rse_weight'] = round(norm_df['rse_weight'], 3)
#     df['datasetname'] = datasetname
#
#     df.drop(['queue_utilization_','closeness_','queue_filling_'],axis=1, inplace=True)
#
#
#     df.to_sql('resource_weights',
#               postgres_connection,
#               if_exists='append',
#               method='multi',
#               index=False)



# calculate_weights()
# calculate_weights('data16_13TeV:data16_13TeV.00299584.physics_Main.deriv.DAOD_TOPQ1.r9264_p3083_p4513_tid25513236_00')
start_date = datetime(2022, 7, 1, 1, 0, 0)
end_date = datetime(2022, 7, 25, 1, 00, 0)
delta_day = timedelta(days=1)

while start_date <= end_date:
    print(start_date)
    calculate_weights_overall(datetime.strftime(start_date,"%Y-%m-%d %H:%M:%S"))
    start_date += delta_day
    print('Data has been written!')

# calculate_queue_weights('2022-04-21 01:00:00')




