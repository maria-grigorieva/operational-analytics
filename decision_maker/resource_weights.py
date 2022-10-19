import os, sys
import pandas as pd
from sqlalchemy import create_engine, text
import configparser
import numpy as np
from database_helpers.helpers import localized_now, insert_to_db, check_for_data_existance, set_time_period, day_rounder
from sklearn.preprocessing import StandardScaler

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
BASE_DIR = os.path.join(ROOT_DIR, '..' )
sys.path.append(os.path.abspath(BASE_DIR))

from datetime import datetime, timedelta

SQL_DIR = BASE_DIR+'/sql'
print(SQL_DIR)

config = configparser.ConfigParser()
config.read(BASE_DIR+'/config.ini')

PostgreSQL_engine = create_engine(config['PostgreSQL']['sqlalchemy_engine_str'], echo=False)

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
        query = text(open(SQL_DIR + '/postgreSQL/queue_weights.sql').read())
        df = pd.read_sql_query(query, postgres_connection, parse_dates={'datetime': '%Y-%m-%d'}, params={'now':from_date})
        print(df)
        postgres_connection.close()
        df.set_index(['queue', 'site',
                      'cloud', 'tier_level',
                      'datetime'], inplace=True)
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


def calculate_queue_weights_hourly_enhanced(predefined_date = False):

    # from_date, to_date = set_time_period(predefined_date, n_hours=24)
    from_date = datetime.strftime(localized_now(), "%Y-%m-%d %H:%M:%S") \
        if not predefined_date else str(predefined_date)

    if not check_for_data_existance('queue_weights_hourly_enhanced', from_date, accuracy='hour', delete=True):

        postgres_connection = PostgreSQL_engine.connect()
        query = text(open(SQL_DIR + '/postgreSQL/queue_weights_hourly_enhanced.sql').read())
        df = pd.read_sql_query(query, postgres_connection, parse_dates={'datetime': '%Y-%m-%d'}, params={'now':from_date})
        postgres_connection.close()
        df.set_index(['queue', 'cpuconsumptionunit', 'site', 'cloud', 'tier_level', 'datetime', 'resource_type'],inplace=True)
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

        insert_to_db(df, 'queue_weights_hourly_enhanced')
    else:
        pass



def calculate_queue_weights_weighted(predefined_date = False):

    from_date = datetime.strftime(localized_now(), "%Y-%m-%d %H:%M:%S") \
        if not predefined_date else str(predefined_date)

    if not check_for_data_existance('queue_weighted_weighted', from_date, accuracy='hour', delete=True):

        postgres_connection = PostgreSQL_engine.connect()
        query = text(open(SQL_DIR + '/postgreSQL/queue_weights_weighted.sql').read())
        df = pd.read_sql_query(query, postgres_connection, parse_dates={'datetime': '%Y-%m-%d'}, params={'now':from_date})
        postgres_connection.close()
        df.set_index(['queue', 'tend'],inplace=True)
        df.fillna(0, inplace=True)
        df.apply(pd.to_numeric)
        norm_df = df.apply(lambda x: round((x - np.mean(x)) / (np.max(x) - np.min(x)), 3))
        norm_df.fillna(0, inplace=True)
        norm_df.reset_index(inplace=True)
        print(norm_df)

        norm_df['current_weight'] = norm_df['performance_weighted']-\
                                    norm_df['utilization_weighted']-\
                                    norm_df['fullness_weighted']+\
                                    norm_df['capacity_weighted']-\
                                    norm_df['avg_waiting_time']

        print(norm_df['current_weight'].values)

        # df.reset_index(inplace=True)
        #
        # df['current_weight'] = round(norm_df['current_weight'], 3)

        norm_df['historical_weight'] = norm_df['performance_hist_pq'] - \
                                    norm_df['utilization_hist_pq'] - \
                                    norm_df['fullness_hist_pq'] + \
                                    norm_df['capacity_hist_pq'] - \
                                    norm_df['queue_time_hist_pq']

        df.reset_index(inplace=True)

        df['current_weight'] = round(norm_df['current_weight'], 3)
        df['historical_weight'] = round(norm_df['historical_weight'], 3)

        insert_to_db(df, 'queue_weights_weighted')
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



# start_date = datetime(2022, 9, 15, 3, 0, 0)
# end_date = datetime(2022, 10, 1, 0, 0, 0)
# delta = timedelta(hours=3)
#
# while start_date <= end_date:
#     print(start_date)
#     calculate_queue_weights_weighted(datetime.strftime(start_date,"%Y-%m-%d %H:%M:%S"))
#     # calculate_queue_weights_hourly_enhanced(datetime.strftime(start_date,"%Y-%m-%d %H:%M:%S"))
#     # calculate_queue_weights(datetime.strftime(start_date,"%Y-%m-%d %H:%M:%S"))
#     # calculate_weights_overall(datetime.strftime(start_date,"%Y-%m-%d %H:%M:%S"))
#     start_date += delta
#     print('Data has been written!')


# calculate_queue_weights_hourly_enhanced('2022-09-17 00:00:00')





