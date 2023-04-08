import os, sys
ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
BASE_DIR = os.path.join(ROOT_DIR, '..' )
sys.path.append(os.path.abspath(BASE_DIR))
import cx_Oracle
import cric
import pandas as pd
from sqlalchemy import create_engine, text
import configparser
from cric.cric_json_api import enhance_queues
from database_helpers.helpers import insert_to_db, check_for_data_existance, set_time_period, localized_now, check_postgreSQL, write_to_postgreSQL, delete_from_pgsql
from datetime import datetime, timedelta

import logging

logging.basicConfig()
logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO)

SQL_DIR = BASE_DIR+'/sql'

config = configparser.ConfigParser()
config.read(BASE_DIR+'/config.ini')

cx_Oracle.init_oracle_client(lib_dir=config['PanDA DB']['client_path'])

metrics = {
    'efficiency':
        {
            'sql': SQL_DIR+'/PanDA/efficiency.sql',
            'table_name': 'queues_efficiency'
         },
    'occupancy':
        {
            'sql': SQL_DIR+'/PanDA/occupancy.sql',
            'table_name': 'queues_occupancy'
         },
    'running_time':
        {
            'sql': SQL_DIR+'/PanDA/running_time.sql',
            'table_name': 'running_time_pq'
         },
    'queue_time':
        {
            'sql': SQL_DIR+'/PanDA/queue_time.sql',
            'table_name': 'queue_time_pq'
        },
    'merged_queues_metrics':
        {
            'sql': SQL_DIR+'/PanDA/queues_metrics.sql',
            'table_name': 'queues_metrics'
        },
    'queues_statuslog':
        {
            'sql': {'statuslog': SQL_DIR+'/PanDA/queues_statuslog.sql',
                    'queue_time': SQL_DIR+'/PanDA/queue_time.sql',
                    'running_time': SQL_DIR+'/PanDA/running_time.sql'},
            'table_name': 'queues_statuslog'
        },
    'queues_statuslog_hourly':
        {
            'sql': SQL_DIR + '/PanDA/queues_statuslog_hourly.sql',
            'table_name': 'queues_snapshot_intervals'
        },
    'queues_hourly':
        {
            'sql': SQL_DIR + '/PanDA/queues_statuslog_hourly.sql',
            'table_name': 'queues_intervals'
        },
    'queues_statuslog_actual':
        {
            'sql': SQL_DIR+'/PanDA/queues_statuslog_actual.sql',
            'table_name': 'queues_snapshots'
        },
    'queues_statuslog_detailed':
        {
            'sql': SQL_DIR + '/PanDA/queues_utilization.sql',
            'table_name': 'queues_utilization'
        }
}

PanDA_engine = create_engine(config['PanDA DB']['sqlalchemy_engine_str'], echo=True, max_identifier_length=128)
PostgreSQL_engine = create_engine(config['PostgreSQL']['sqlalchemy_engine_str'], echo=False)


def queues_to_db(metric, predefined_date = False):

    from_date, to_date = set_time_period(predefined_date, n_hours=24)

    if not check_for_data_existance(metrics.get(metric)["table_name"], from_date, delete=True):
        panda_connection = PanDA_engine.connect()
        query = text(open(metrics.get(metric)['sql']).read())
        df = pd.read_sql_query(query, panda_connection, parse_dates={'datetime': '%Y-%m-%d'},
                               params={'from_date': from_date})
        print(df)
        panda_connection.close()
        df.fillna(0, inplace=True)
        insert_to_db(df, metrics.get(metric)["table_name"])
    else:
        pass


# detailed queues statuslog with computingelements
def queues_hourly_statuslog_to_db(metric, predefined_date = False):

    my_date = datetime.strftime(localized_now(),"%Y-%m-%d %H:%M:%S") if not predefined_date else str(predefined_date)

    panda_connection = PanDA_engine.connect()
    postgresql_connection = PostgreSQL_engine.connect()
    query = text(open(metrics.get(metric)['sql']).read())
    df = pd.read_sql_query(query, panda_connection,parse_dates={'datetime': '%Y-%m-%d %H:%M:%S'},
                           params={'from_date': my_date, 'hours': 1})
    panda_connection.close()
    # print(f'{df.shape[0]} rows has been returned from PanDA DB')
    from_cric = cric.cric_json_api.enhance_queues()
    result = pd.merge(df, from_cric, left_on='queue', right_on='queue')

    with postgresql_connection.begin():
        try:
            if postgresql_connection.execute(text(f'SELECT * FROM {metrics.get(metric)["table_name"]} '
                                              f'WHERE datetime = DATE_TRUNC(\'hours\', TIMESTAMP \'{my_date}\')')).rowcount == 0:
                insert_to_db(result,metrics.get(metric)["table_name"])
                postgresql_connection.close()
                print(f'The data has {len(result)} rows')
        except Exception as e:
            print('Insert data failed!')


def queues_hourly_to_db(metric, predefined_date = False, n_hours=1):

    from_date = datetime.strftime(localized_now(),"%Y-%m-%d %H:%M:%S") if not predefined_date else str(predefined_date)

    panda_connection = PanDA_engine.connect()
    postgresql_connection = PostgreSQL_engine.connect()
    query = text(open(metrics.get(metric)['sql']).read())
    df = pd.read_sql_query(query, panda_connection,parse_dates={'datetime': '%Y-%m-%d %H:%M:%S'},
                           params={'now': from_date,'n_hours': n_hours})
    panda_connection.close()
    print(f'{df.shape[0]} rows has been returned from PanDA DB')
    from_cric = cric.cric_json_api.enhance_queues()
    result = pd.merge(df, from_cric, left_on='queue', right_on='queue')
    result['transferring_diff'] = result['transferring_limit'] - result['transferring']
    result['interval_hours'] = n_hours
    result['corecount'].fillna(0,inplace=True)
    # Custom check for existance using datetime and interval_hours parameters
    with postgresql_connection.begin():
        try:
            if postgresql_connection.execute(text(f'SELECT * FROM {metrics.get(metric)["table_name"]} '
                                              f'WHERE datetime = DATE_TRUNC(\'hour\', TIMESTAMP \'{from_date}\') '
                                              f'AND interval_hours = {n_hours}')).rowcount == 0:
                insert_to_db(result,metrics.get(metric)["table_name"])
                postgresql_connection.close()
                print(f'The data has {len(result)} rows')
        except Exception as e:
            print('Insert data failed!')


def enhanced_queues_utilization(predefined_date = False, mode='daily'):

    from_date = datetime.strftime(localized_now(), "%Y-%m-%d %H:%M:%S") \
        if not predefined_date else str(predefined_date)

#    from_date, to_date = set_time_period(predefined_date, n_hours=24 if mode=='daily' else 1)

    if not check_for_data_existance(f'{mode}_enhanced_queues_utilization', from_date,  accuracy='hour', delete=True):
        postgreSQL_connection = PostgreSQL_engine.connect()
        query = text(open(SQL_DIR + f'/postgreSQL/queues_utilization_enhanced_{mode}.sql').read())
        df = pd.read_sql_query(query, postgreSQL_connection, parse_dates={'datetime': '%Y-%m-%d'},
                               params={'from_date': from_date})
        postgreSQL_connection.close()
        #df.fillna(0, inplace=True)
        insert_to_db(df, f'{mode}_enhanced_queues_utilization')
    else:
        pass


def queues_workload_weighted(predefined_date=False, hours=4, queues='actual'):

    from_date = datetime.strftime(localized_now(), "%Y-%m-%d %H:%M:%S") \
        if not predefined_date else str(predefined_date)

    if not check_for_data_existance(f'queues_utilization_weighted', from_date, accuracy='hour', delete=True,
                                    dt='tend'):
        PanDA_connection = PanDA_engine.connect()
        query = text(open(SQL_DIR + f'/PanDA/queues_utilization_weighted.sql').read())
        df = pd.read_sql_query(query, PanDA_connection, parse_dates={'datetime': '%Y-%m-%d %H:%M:%S'},
                               params={'from_date': from_date,
                                       'hours': hours})
        if queues == 'actual':
            from_cric = cric.cric_json_api.enhance_queues()
        elif queues == 'db':
            PostgreSQL_connection = PostgreSQL_engine.connect()
            query = text('SELECT queue,cloud,site,resource_type,'
                         'tier_level, status, state, nodes,'
                         'corepower, corecount, region, transferring_limit '
                         'FROM cric_resources WHERE '
                         'datetime = (SELECT max(datetime) FROM cric_resources)'
                         'GROUP by queue,cloud,site,resource_type,'
                         'tier_level, status, state, nodes,'
                         'corepower, corecount, region, transferring_limit')
            from_cric = pd.read_sql_query(query, PostgreSQL_connection)
        result = pd.merge(df, from_cric, left_on='queue', right_on='queue')
        PanDA_connection.close()
        insert_to_db(result, 'queues_utilization_weighted')
    else:
        pass


def queues_workload_weighted_detailed(predefined_date=False, hours=4, queues='actual'):

    from_date = datetime.strftime(localized_now(), "%Y-%m-%d %H:%M:%S") \
        if not predefined_date else str(predefined_date)

    if not check_for_data_existance(f'queues_utilization_weighted_detailed', from_date, accuracy='hour', delete=True,
                                    dt='tend'):
        PanDA_connection = PanDA_engine.connect()
        query = text(open(SQL_DIR + f'/PanDA/queues_utilization_weighted_with_median.sql').read())
        df = pd.read_sql_query(query, PanDA_connection, parse_dates={'datetime': '%Y-%m-%d %H:%M:%S'},
                               params={'from_date': from_date,
                                       'hours': hours})
        if queues == 'actual':
            from_cric = cric.cric_json_api.enhance_queues()
        elif queues == 'db':
            PostgreSQL_connection = PostgreSQL_engine.connect()
            query = text('SELECT queue,cloud,site,resource_type,'
                         'tier_level, status, state, nodes,'
                         'corepower, corecount, region, transferring_limit '
                         'FROM cric_resources WHERE '
                         'datetime = (SELECT max(datetime) FROM cric_resources)'
                         'GROUP by queue,cloud,site,resource_type,'
                         'tier_level, status, state, nodes,'
                         'corepower, corecount, region, transferring_limit')
            from_cric = pd.read_sql_query(query, PostgreSQL_connection)
        result = pd.merge(df, from_cric, left_on='queue', right_on='queue')
        PanDA_connection.close()
        insert_to_db(result, 'queues_utilization_weighted_detailed')
    else:
        pass


def queues_weighted_jobs(predefined_date=False, queues='actual'):

    from_date = datetime.strftime(localized_now(), "%Y-%m-%d %H:%M:%S") \
        if not predefined_date else str(predefined_date)

    if not check_for_data_existance(f'queues_weighted_jobs', from_date, accuracy='hour', delete=True,
                                    dt='tend'):
        PanDA_connection = PanDA_engine.connect()
        query = text(open(SQL_DIR + f'/PanDA/queues_weighted_jobs.sql').read())
        df = pd.read_sql_query(query, PanDA_connection, parse_dates={'datetime': '%Y-%m-%d %H:%M:%S'},
                               params={'from_date': from_date})
        if queues == 'actual':
            from_cric = cric.cric_json_api.enhance_queues()
        elif queues == 'db':
            PostgreSQL_connection = PostgreSQL_engine.connect()
            query = text('SELECT queue,cloud,site,resource_type,'
                         'tier_level, status, state, nodes,'
                         'corepower, corecount, region, transferring_limit '
                         'FROM cric_resources WHERE '
                         'datetime = (SELECT max(datetime) FROM cric_resources)'
                         'GROUP by queue,cloud,site,resource_type,'
                         'tier_level, status, state, nodes,'
                         'corepower, corecount, region, transferring_limit')
            from_cric = pd.read_sql_query(query, PostgreSQL_connection)
        result = pd.merge(df, from_cric, left_on='queue', right_on='queue')
        PanDA_connection.close()
        insert_to_db(result, 'queues_weighted_jobs')
    else:
        pass

def queues_weighted_jobs_wt(predefined_date=False, queues='actual'):

    from_date = datetime.strftime(localized_now(), "%Y-%m-%d %H:%M:%S") \
        if not predefined_date else str(predefined_date)

    if not check_for_data_existance(f'queues_weighted_jobs_wt', from_date, accuracy='hour', delete=True,
                                    dt='tend'):
        PanDA_connection = PanDA_engine.connect()
        query = text(open(SQL_DIR + f'/PanDA/queues_weighted_jobs_wt.sql').read())
        df = pd.read_sql_query(query, PanDA_connection, parse_dates={'datetime': '%Y-%m-%d %H:%M:%S'},
                               params={'from_date': from_date})
        if queues == 'actual':
            try:
                from_cric = cric.cric_json_api.enhance_queues()
            except Exception as e:
                PostgreSQL_connection = PostgreSQL_engine.connect()
                query = text('SELECT queue,cloud,site,resource_type,'
                             'tier_level, status, state, nodes,'
                             'corepower, corecount, region, transferring_limit '
                             'FROM cric_resources WHERE '
                             'datetime = (SELECT max(datetime) FROM cric_resources)'
                             'GROUP by queue,cloud,site,resource_type,'
                             'tier_level, status, state, nodes,'
                             'corepower, corecount, region, transferring_limit')
                from_cric = pd.read_sql_query(query, PostgreSQL_connection)
        elif queues == 'db':
            PostgreSQL_connection = PostgreSQL_engine.connect()
            query = text('SELECT queue,cloud,site,resource_type,'
                         'tier_level, status, state, nodes,'
                         'corepower, corecount, region, transferring_limit '
                         'FROM cric_resources WHERE '
                         'datetime = (SELECT max(datetime) FROM cric_resources)'
                         'GROUP by queue,cloud,site,resource_type,'
                         'tier_level, status, state, nodes,'
                         'corepower, corecount, region, transferring_limit')
            from_cric = pd.read_sql_query(query, PostgreSQL_connection)
        result = pd.merge(df, from_cric, left_on='queue', right_on='queue')
        PanDA_connection.close()
        insert_to_db(result, 'queues_weighted_jobs_wt')
    else:
        pass

def queues_metrics_hourly(predefined_date=False, queues='actual'):

    from_date = datetime.strftime(localized_now(), "%Y-%m-%d %H:%M:%S") \
        if not predefined_date else str(predefined_date)

    if not check_for_data_existance(f'queues_hourly_metrics', from_date, accuracy='hour', delete=True,
                                    dt='datetime'):
        PanDA_connection = PanDA_engine.connect()
        query = text(open(SQL_DIR + f'/PanDA/queues_hourly_metrics.sql').read())
        df = pd.read_sql_query(query, PanDA_connection, parse_dates={'datetime': '%Y-%m-%d %H:%M:%S'},
                               params={'from_date': from_date})
        if queues == 'actual':
            try:
                from_cric = cric.cric_json_api.enhance_queues()
            except Exception as e:
                PostgreSQL_connection = PostgreSQL_engine.connect()
                query = text('SELECT queue,cloud,site,resource_type,'
                             'tier_level, status, state, nodes,'
                             'corepower, corecount, region, transferring_limit '
                             'FROM cric_resources WHERE '
                             'datetime = (SELECT max(datetime) FROM cric_resources)'
                             'GROUP by queue,cloud,site,resource_type,'
                             'tier_level, status, state, nodes,'
                             'corepower, corecount, region, transferring_limit')
                from_cric = pd.read_sql_query(query, PostgreSQL_connection)
        elif queues == 'db':
            PostgreSQL_connection = PostgreSQL_engine.connect()
            query = text('SELECT queue,cloud,site,resource_type,'
                         'tier_level, status, state, nodes,'
                         'corepower, corecount, region, transferring_limit '
                         'FROM cric_resources WHERE '
                         'datetime = (SELECT max(datetime) FROM cric_resources)'
                         'GROUP by queue,cloud,site,resource_type,'
                         'tier_level, status, state, nodes,'
                         'corepower, corecount, region, transferring_limit')
            from_cric = pd.read_sql_query(query, PostgreSQL_connection)
        result = pd.merge(df, from_cric, left_on='queue', right_on='queue')
        PanDA_connection.close()
        insert_to_db(result, 'queues_hourly_metrics')
    else:
        pass


def queues_metrics_hourly_new(predefined_date=False, queues='actual'):

    from_date = datetime.strftime(localized_now(), "%Y-%m-%d %H:%M:%S") \
        if not predefined_date else str(predefined_date)

    if check_postgreSQL('queues_hourly_metrics_new', from_date, accuracy='hour', datetime_col_name='datetime') == True:
        delete_from_pgsql('queues_hourly_metrics_new', from_date, accuracy='hour', datetime_col_name='datetime')

    PanDA_connection = PanDA_engine.connect()
    query = text(open(SQL_DIR + f'/PanDA/queues_hourly_metrics_new.sql').read())
    df = pd.read_sql_query(query, PanDA_connection, parse_dates={'datetime': '%Y-%m-%d %H:%M:%S'},
                           params={'from_date': from_date})
    if queues == 'actual':
        try:
            from_cric = cric.cric_json_api.enhance_queues()
        except Exception as e:
            PostgreSQL_connection = PostgreSQL_engine.connect()
            query = text('SELECT queue,cloud,site,resource_type,'
                         'tier_level, status, state, nodes,'
                         'corepower, corecount, region, transferring_limit '
                         'FROM cric_resources WHERE '
                         'datetime = (SELECT max(datetime) FROM cric_resources)'
                         'GROUP by queue,cloud,site,resource_type,'
                         'tier_level, status, state, nodes,'
                         'corepower, corecount, region, transferring_limit')
            from_cric = pd.read_sql_query(query, PostgreSQL_connection)
    elif queues == 'db':
        PostgreSQL_connection = PostgreSQL_engine.connect()
        query = text('SELECT queue,cloud,site,resource_type,'
                     'tier_level, status, state, nodes,'
                     'corepower, corecount, region, transferring_limit '
                     'FROM cric_resources WHERE '
                     'datetime = (SELECT max(datetime) FROM cric_resources)'
                     'GROUP by queue,cloud,site,resource_type,'
                     'tier_level, status, state, nodes,'
                     'corepower, corecount, region, transferring_limit')
        from_cric = pd.read_sql_query(query, PostgreSQL_connection)
    result = pd.merge(df, from_cric, left_on='queue', right_on='queue')
    PanDA_connection.close()
    write_to_postgreSQL(result, 'queues_hourly_metrics_new')


def queues_hourly_metrics_fixed(predefined_date=False, queues='actual'):

    from_date = datetime.strftime(localized_now(), "%Y-%m-%d %H:%M:%S") \
        if not predefined_date else str(predefined_date)

    # if check_postgreSQL('queues_hourly_metrics_fixed', from_date, accuracy='hour', datetime_col_name='datetime') == True:
    #     delete_from_pgsql('queues_hourly_metrics_fixed', from_date, accuracy='hour', datetime_col_name='datetime')

    # dsn_tns = cx_Oracle.makedsn('itrac54104-v.cern.ch', '10121', service_name='adcr.cern.ch')
    # conn = cx_Oracle.connect(user='atlas_datapopularity_r', password='dpp_ADCmon21', dsn=dsn_tns)
    # curr = conn.cursor()
    # f = open(SQL_DIR + f'/PanDA/queues_hourly_metrics_fixed.sql')
    # q = f.read()
    # print(q)
    # res = curr.execute('SELECT * FROM ATLAS_PANDA.JOBS_STATUSLOG WHERE rownum<=10')
    # print(res)


    engine = PanDA_engine.connect()
    # query = text('''SELECT * FROM ATLAS_PANDA.JOBS_STATUSLOG WHERE rownum<=10''')
    # df = pd.read_sql_query(query, con=engine)
    query = text(open(SQL_DIR + f'/PanDA/queues_hourly_metrics_fixed.sql').read())
    df = pd.read_sql_query(query, con=engine, parse_dates={'datetime': '%Y-%m-%d %H:%M:%S'},
                           params={'from_date': from_date})
    engine.close()
    if queues == 'actual':
        try:
            from_cric = cric.cric_json_api.enhance_queues()
        except Exception as e:
            PostgreSQL_connection = PostgreSQL_engine.connect()
            query = text('SELECT queue,cloud,site,resource_type,'
                         'tier_level, status, state, nodes,'
                         'corepower, corecount, region, transferring_limit '
                         'FROM cric_resources WHERE '
                         'datetime = (SELECT max(datetime) FROM cric_resources)'
                         'GROUP by queue,cloud,site,resource_type,'
                         'tier_level, status, state, nodes,'
                         'corepower, corecount, region, transferring_limit')
            from_cric = pd.read_sql_query(query, PostgreSQL_connection)
    elif queues == 'db':
        PostgreSQL_connection = PostgreSQL_engine.connect()
        query = text('SELECT queue,cloud,site,resource_type,'
                     'tier_level, status, state, nodes,'
                     'corepower, corecount, region, transferring_limit '
                     'FROM cric_resources WHERE '
                     'datetime = (SELECT max(datetime) FROM cric_resources)'
                     'GROUP by queue,cloud,site,resource_type,'
                     'tier_level, status, state, nodes,'
                     'corepower, corecount, region, transferring_limit')
        from_cric = pd.read_sql_query(query, PostgreSQL_connection)
    result = pd.merge(df, from_cric, left_on='queue', right_on='queue')
    write_to_postgreSQL(result, 'queues_hourly_metrics_fixed')


def queues_workload(predefined_date=False, queues='actual'):

    from_date = datetime.strftime(localized_now(), "%Y-%m-%d %H:%M:%S") \
        if not predefined_date else str(predefined_date)

    if check_postgreSQL('queues_workload', from_date, accuracy='hour', datetime_col_name='datetime') == True:
        delete_from_pgsql('queues_workload', from_date, accuracy='hour', datetime_col_name='datetime')

    engine = PanDA_engine.connect()
    query = text(open(SQL_DIR + f'/PanDA/queues_workload.sql').read())
    df = pd.read_sql_query(query, con=engine, parse_dates={'datetime': '%Y-%m-%d %H:%M:%S'},
                           params={'from_date': from_date})
    engine.close()
    if queues == 'actual':
        try:
            from_cric = cric.cric_json_api.enhance_queues()
        except Exception as e:
            PostgreSQL_connection = PostgreSQL_engine.connect()
            query = text('SELECT queue,cloud,site,resource_type,'
                         'tier_level, status, state, nodes,'
                         'corepower, corecount, region, transferring_limit '
                         'FROM cric_resources WHERE '
                         'datetime = (SELECT max(datetime) FROM cric_resources)'
                         'GROUP by queue,cloud,site,resource_type,'
                         'tier_level, status, state, nodes,'
                         'corepower, corecount, region, transferring_limit')
            from_cric = pd.read_sql_query(query, PostgreSQL_connection)
    elif queues == 'db':
        PostgreSQL_connection = PostgreSQL_engine.connect()
        query = text('SELECT queue,cloud,site,resource_type,'
                     'tier_level, status, state, nodes,'
                     'corepower, corecount, region, transferring_limit '
                     'FROM cric_resources WHERE '
                     'datetime = (SELECT max(datetime) FROM cric_resources)'
                     'GROUP by queue,cloud,site,resource_type,'
                     'tier_level, status, state, nodes,'
                     'corepower, corecount, region, transferring_limit')
        from_cric = pd.read_sql_query(query, PostgreSQL_connection)
    result = pd.merge(df, from_cric, left_on='queue', right_on='queue')
    write_to_postgreSQL(result, 'queues_workload')



def queues_workload_extended(predefined_date=False, queues='actual'):

    from_date = datetime.strftime(localized_now(), "%Y-%m-%d %H:%M:%S") \
        if not predefined_date else str(predefined_date)

    if check_postgreSQL('queues_workload_extended', from_date, accuracy='hour', datetime_col_name='end_time') == True:
        delete_from_pgsql('queues_workload_extended', from_date, accuracy='hour', datetime_col_name='end_time')

    engine = PanDA_engine.connect()
    query = text(open(SQL_DIR + f'/PanDA/queues_workload_extended.sql').read())
    df = pd.read_sql_query(query, con=engine, parse_dates={'end_time': '%Y-%m-%d %H:%M:%S', 'start_time': '%Y-%m-%d %H:%M:%S'},
                           params={'from_date': from_date})
    engine.close()
    if queues == 'actual':
        try:
            from_cric = cric.cric_json_api.enhance_queues(all=True)
        except Exception as e:
            PostgreSQL_connection = PostgreSQL_engine.connect()
            query = text('SELECT queue,cloud,site,resource_type,'
                         'tier_level, status, state, nodes,'
                         'corepower, corecount, region, transferring_limit '
                         'FROM cric_resources WHERE '
                         'datetime = (SELECT max(datetime) FROM cric_resources)'
                         'GROUP by queue,cloud,site,resource_type,'
                         'tier_level, status, state, nodes,'
                         'corepower, corecount, region, transferring_limit')
            from_cric = pd.read_sql_query(query, PostgreSQL_connection)
    elif queues == 'db':
        PostgreSQL_connection = PostgreSQL_engine.connect()
        query = text('SELECT queue,cloud,site,resource_type,'
                     'tier_level, status, state, nodes,'
                     'corepower, corecount, region, transferring_limit '
                     'FROM cric_resources WHERE '
                     'datetime = (SELECT max(datetime) FROM cric_resources)'
                     'GROUP by queue,cloud,site,resource_type,'
                     'tier_level, status, state, nodes,'
                     'corepower, corecount, region, transferring_limit')
        from_cric = pd.read_sql_query(query, PostgreSQL_connection)

    from_cric.rename(columns={'status':'cric_status','state':'cric_state','resource_type':'cric_resource_type'},inplace=True)
    from_cric.drop(['nodes','corepower','corecount','transferring_limit'],axis=1,inplace=True)
    result = pd.merge(df, from_cric, left_on='queue', right_on='queue')
    write_to_postgreSQL(result, 'queues_workload_extended')