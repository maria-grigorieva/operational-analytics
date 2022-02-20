from checker.celery import app
from datetime import datetime, timedelta
from datasets.dataset_replicas_collector import dataset_replicas_to_db as dataset_replicas_to_db_worker
from datasets.datasets_popularity import save_dataset_task_user_to_db as save_dataset_task_user_to_db_worker
from datasets.datasets_popularity import save_historical_popularity_to_db as save_historical_popularity_to_db_worker


@app.task(name="dataset_replicas_to_db",autoretry_for=(Exception,),max_retries=5,default_retry_delay=300)
def dataset_replicas_to_db():
    try:
        return dataset_replicas_to_db_worker()
    except Exception as e:
        raise e


@app.task(name="save_historical_popularity_to_db", autoretry_for=(Exception,), max_retries=5, default_retry_delay=600)
def save_historical_popularity_to_db():
    try:
        return save_historical_popularity_to_db_worker()
    except Exception as e:
        raise e


@app.task(name="save_dataset_task_user_to_db", autoretry_for=(Exception,), max_retries=5, default_retry_delay=600)
def save_popularity_to_db():
    try:
        return save_dataset_task_user_to_db_worker()
    except Exception as e:
        raise e

# @app.task(name="popularity_data_collection", autoretry_for=(Exception,), max_retries=5, default_retry_delay=600)
# def popularity_data_collection():
#     start_date = datetime(2021, 12, 14, 1, 0, 0)
#     end_date = datetime(2022, 1, 1, 1, 00, 0)
#     delta_day = timedelta(days=1)
#
#     while start_date <= end_date:
#         save_historical_popularity_to_db_worker(datetime.strftime(start_date, "%Y-%m-%d %H:%M:%S"))
#         start_date += delta_day
#         print('Data has been written!')