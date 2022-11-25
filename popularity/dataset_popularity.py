import os, sys
ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
BASE_DIR = os.path.join(ROOT_DIR, '..' )
sys.path.append(os.path.abspath(BASE_DIR))
import cx_Oracle
import pandas as pd
from sqlalchemy import create_engine, text
import configparser
from database_helpers.helpers import write_to_postgreSQL, check_for_data_existance, set_time_period, localized_now
from datetime import datetime, timedelta
from sklearn.ensemble import RandomForestClassifier
import numpy as np

import logging

logging.basicConfig()

SQL_DIR = BASE_DIR+'/sql'

config = configparser.ConfigParser()
config.read(BASE_DIR+'/config.ini')

PanDA_engine = create_engine(config['PanDA DB']['sqlalchemy_engine_str'], echo=False, max_identifier_length=128)
PostgreSQL_engine = create_engine(config['PostgreSQL']['sqlalchemy_engine_str'], echo=False)


def datasets_popularity_to_db(predefined_date = False):

    from_date = datetime.strftime(localized_now(), "%Y-%m-%d %H:%M:%S") if not predefined_date else str(predefined_date)

    if not check_for_data_existance('datasets_popularity', from_date, delete=True):
        panda_connection = PanDA_engine.connect()
        query = text(open(SQL_DIR+'/PanDA/datasets_popularity.sql').read())
        df = pd.read_sql_query(query,
                               panda_connection,
                               parse_dates={'datetime': '%Y-%m-%d'},
                               params={'from_date': from_date})
        panda_connection.close()
        write_to_postgreSQL(df, 'datasets_popularity')
    else:
        pass


def aggregation_week(predefined_date = False):

    from_date = datetime.strftime(localized_now(), "%Y-%m-%d %H:%M:%S") if not predefined_date else str(predefined_date)

    conn = PostgreSQL_engine.connect()
    query = text(open(SQL_DIR+'/postgreSQL/agg_week_datasets_popularity.sql').read())
    df = pd.read_sql_query(query, conn,
                           parse_dates={'datetime': '%Y-%m-%d'},
                           params={'from_date': from_date}
                           )

    for row in df.to_dict('records'):
        q = text(open(SQL_DIR + '/postgreSQL/agg_week_datasets_popularity_upsert.sql').read())
        conn.execute(q, row, params=row)

    conn.close()


def aggregation_all():
    PostgreSQL_connection = PostgreSQL_engine.connect()
    query = text('SELECT '
                 'datasetname,'
                 'input_format_short,'
                 'input_format_desc,'
                 'input_project,'
                 'prod_step,'
                 'process_desc,'
                 'n_dataset,'
                 'tid,'
                 'process_tags,'
                 'sum(n_tasks) as n_tasks,'
                 'min(datetime) as start_usage,'
                 'max(datetime) as end_usage,'
                 'DATE_PART(\'day\', max(datetime) - min(datetime)) as usage_period '
                 'FROM datasets_popularity '
                 'GROUP BY datasetname,'
                 'input_format_short,'
                 'input_format_desc,'
                 'input_project,'
                 'prod_step,'
                 'process_desc,'
                 'n_dataset,'
                 'tid,'
                 'process_tags')
    df = pd.read_sql_query(query, PostgreSQL_connection)
    PostgreSQL_connection.close()
    write_to_postgreSQL(df, 'aggregated_datasets_popularity')


def datasets_forecast_init():

    PostgreSQL_connection = PostgreSQL_engine.connect()
    query = text('''
                SELECT t1.datasetname,
                       date_trunc('day', cal.date)::date as date,
                       COALESCE(t2.n_tasks,0) as n_tasks
            FROM (SELECT generate_series
                 (min(datetime),max(datetime), '1 week'::interval)::timestamp as date FROM datasets_popularity) cal
            CROSS JOIN (SELECT DISTINCT datasetname FROM datasets_popularity
                  WHERE input_format_short = 'DAOD'
                  AND input_project = 'mc16_13TeV'
                  AND input_format_desc LIKE 'HIGG%') t1
            LEFT JOIN datasets_popularity t2
                 ON t2.datetime = cal.date AND t2.datasetname = t1.datasetname
            ORDER BY
                t1.datasetname,
                cal.date
    ''')

    df_iterator = pd.read_sql_query(query, PostgreSQL_connection, chunksize=20000)

    for i, df_chunk in enumerate(df_iterator):
        print(i)
        write_to_postgreSQL(df_chunk, 'dataset_popularity_forecast')


def datasets_forecast_classification(predefined_date=False):

    from_date = datetime.strftime(localized_now(), "%Y-%m-%d %H:%M:%S") if not predefined_date else str(predefined_date)
    print(from_date)
    PostgreSQL_connection = PostgreSQL_engine.connect()

    if check_for_data_existance('dataset_popularity_forecast', from_date, delete=False, dt='date') == False:
        query = text('''
                        SELECT
                        t1.datasetname,
                        date_trunc('day', TIMESTAMP :now)::date as date,
                        COALESCE(t2.n_tasks,0) as n_tasks
                    FROM (SELECT DISTINCT datasetname FROM datasets_popularity
                           WHERE input_format_short = 'DAOD'
                        AND input_project = 'mc16_13TeV'
                        AND input_format_desc LIKE 'HIGG%') t1
                    LEFT JOIN datasets_popularity t2
                        ON t2.datetime = date_trunc('day', TIMESTAMP :now) AND t2.datasetname = t1.datasetname
                    ORDER BY
                        t1.datasetname,
                        date_trunc('day', TIMESTAMP :now)::date
            ''')
        df = pd.read_sql_query(query,
                               PostgreSQL_connection,
                               parse_dates={'date': '%Y-%m-%d'},
                               params={'now': from_date})
        write_to_postgreSQL(df, 'dataset_popularity_forecast')
        print('The data has been added to the DB')

    # get all data from the beginning to the predefined date
    query = text('''
                    SELECT datasetname,
                           array_agg(n_tasks::numeric ORDER BY date) as n_tasks
                    FROM dataset_popularity_forecast
                    WHERE date <= TIMESTAMP :now
                    GROUP BY datasetname
                ''')
    df = pd.read_sql_query(query,
                           PostgreSQL_connection,
                           parse_dates={'date': '%Y-%m-%d'},
                           params={'now': from_date})

    # data preparation for training
    df['n_tasks'] = df['n_tasks'].apply(lambda x: [int(item) for item in x])
    df['train_sequence'] = df['n_tasks'].apply(lambda x: [item for item in x][:-1])
    df['labels'] = df['n_tasks'].apply(lambda x: [1 if item > 0 else 0 for item in x][-1])

    # shuffle data frame
    # df = df.sample(frac=1).reset_index(drop=True)

    X = list(df['train_sequence'].values)
    y = list(df['labels'].values)

    # train model on all data
    clf = RandomForestClassifier(n_estimators=300)
    clf.fit(X, y)

    n_weeks = df['train_sequence'].str.len()[0]
    print(f'The model was trained on {n_weeks} weeks')

    # prediction
    sequences_to_predict = list(df['n_tasks'].apply(lambda x: [item for item in x][1:]).values)
    predicted = clf.predict(sequences_to_predict)
    df['usage_forecast'] = predicted

    # backpropagation
    next_date = datetime.strptime(from_date, '%Y-%m-%d %H:%M:%S') + timedelta(weeks=1)
    next_date = datetime.strftime(next_date, '%Y-%m-%d')
    df['date'] = next_date
    df['n_tasks'] = 0

    # upsert into database
    for row in df[['datasetname','date','n_tasks','usage_forecast']].to_dict('records'):
        query = text('''
            INSERT INTO dataset_popularity_forecast(datasetname,
                                                    date,
                                                    n_tasks,
                                                    usage_forecast)
            VALUES(:datasetname,
                   :date,
                   :n_tasks,
                   :usage_forecast)
            ON CONFLICT (datasetname, date)
            DO
               UPDATE SET usage_forecast = EXCLUDED.usage_forecast,
                          n_tasks = EXCLUDED.n_tasks + dataset_popularity_forecast.n_tasks
        ''')
        PostgreSQL_connection.execute(query, row, params=row)
        # print(f'inserted row {row}')











