from checker.celery import app
from queues.queues_metrics import calculate_metric as calculate_metric_worker
from queues.queues_metrics import exclude_outliers as exclude_outliers_worker
from queues.queues_metrics import popularity_by_tasks as popularity_by_tasks_worker
from queues.queues_metrics import queues_to_db as queues_to_db_worker
from queues.datasets_popularity import save_popularity_to_db as save_popularity_to_db_worker
from queues.queues_metrics import queues_hourly_to_db as queues_hourly_to_db_worker
from queues.datasets_popularity import save_dataset_task_user_to_db as save_dataset_task_user_to_db_worker

@app.task(name="merged_queues_metrics",autoretry_for=(Exception,),max_retries=5,default_retry_delay=600)
def merged_queues_metrics(metric='merged_queues_metrics'):
    try:
        return calculate_metric_worker(metric)
    except Exception as e:
        raise e


@app.task(name="queues_statuslog_actual",autoretry_for=(Exception,),max_retries=5,default_retry_delay=600)
def queues_statuslog_actual(metric='queues_statuslog_actual'):
    try:
        return queues_to_db_worker(metric)
    except Exception as e:
        raise e


@app.task(name="exclude_outliers",autoretry_for=(Exception,),max_retries=5,default_retry_delay=600)
def exclude_outliers():
    try:
        return exclude_outliers_worker()
    except Exception as e:
        raise e


@app.task(name="check")
def check():
    print("I'm checking your stuff!")


@app.task(name="popularity_by_tasks",autoretry_for=(Exception,),max_retries=5,default_retry_delay=600)
def popularity_by_tasks(from_date):
    try:
        return popularity_by_tasks_worker(from_date)
    except Exception as e:
        raise e


@app.task(name="save_popularity_to_db", autoretry_for=(Exception,), max_retries=5, default_retry_delay=600)
def save_popularity_to_db():
    try:
        return save_popularity_to_db_worker()
    except Exception as e:
        raise e


@app.task(name="save_dataset_task_user_to_db", autoretry_for=(Exception,), max_retries=5, default_retry_delay=600)
def save_popularity_to_db():
    try:
        return save_dataset_task_user_to_db_worker()
    except Exception as e:
        raise e

@app.task(name="queues_1hour",autoretry_for=(Exception,),max_retries=5,default_retry_delay=600)
def queues_1hour(metric='queues_statuslog_hourly',n_hours=1):
    try:
        return queues_hourly_to_db_worker(metric,n_hours=n_hours)
    except Exception as e:
        raise e

@app.task(name="queues_3hours",autoretry_for=(Exception,),max_retries=5,default_retry_delay=600)
def queues_1hour(metric='queues_statuslog_hourly',n_hours=3):
    try:
        return queues_hourly_to_db_worker(metric,n_hours=n_hours)
    except Exception as e:
        raise e

@app.task(name="queues_6hours",autoretry_for=(Exception,),max_retries=5,default_retry_delay=600)
def queues_1hour(metric='queues_statuslog_hourly',n_hours=6):
    try:
        return queues_hourly_to_db_worker(metric,n_hours=n_hours)
    except Exception as e:
        raise e

@app.task(name="queues_12hours",autoretry_for=(Exception,),max_retries=5,default_retry_delay=600)
def queues_1hour(metric='queues_statuslog_hourly',n_hours=12):
    try:
        return queues_hourly_to_db_worker(metric,n_hours=n_hours)
    except Exception as e:
        raise e

@app.task(name="queues_1day",autoretry_for=(Exception,),max_retries=5,default_retry_delay=600)
def queues_1hour(metric='queues_statuslog_hourly',n_hours=24):
    try:
        return queues_hourly_to_db_worker(metric,n_hours=n_hours)
    except Exception as e:
        raise e

@app.task(name="queues_2days",autoretry_for=(Exception,),max_retries=5,default_retry_delay=600)
def queues_1hour(metric='queues_statuslog_hourly',n_hours=48):
    try:
        return queues_hourly_to_db_worker(metric,n_hours=n_hours)
    except Exception as e:
        raise e

@app.task(name="queues_3days",autoretry_for=(Exception,),max_retries=5,default_retry_delay=600)
def queues_1hour(metric='queues_statuslog_hourly',n_hours=82):
    try:
        return queues_hourly_to_db_worker(metric,n_hours=n_hours)
    except Exception as e:
        raise e

@app.task(name="queues_1week",autoretry_for=(Exception,),max_retries=5,default_retry_delay=600)
def queues_1hour(metric='queues_statuslog_hourly',n_hours=168):
    try:
        return queues_hourly_to_db_worker(metric,n_hours=n_hours)
    except Exception as e:
        raise e