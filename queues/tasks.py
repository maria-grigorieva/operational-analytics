from checker.celery import app
from queues.queues_metrics import calculate_metric as calculate_metric_worker
from queues.queues_metrics import exclude_outliers as exclude_outliers_worker

@app.task(name="efficiency",autoretry_for=(Exception,),max_retries=5,default_retry_delay=600)
def efficiency(metric='efficiency'):
    try:
        return calculate_metric_worker(metric)
    except Exception as e:
        raise e

@app.task(name="occupancy",autoretry_for=(Exception,),max_retries=5,default_retry_delay=600)
def occupancy(metric='occupancy'):
    try:
        return calculate_metric_worker(metric)
    except Exception as e:
        raise e

@app.task(name="job_shares",autoretry_for=(Exception,),max_retries=5,default_retry_delay=600)
def job_shares(metric='job_shares'):
    try:
        return calculate_metric_worker(metric)
    except Exception as e:
        raise e

@app.task(name="queueing_time",autoretry_for=(Exception,),max_retries=5,default_retry_delay=600)
def queueing_time(metric='queueing_time'):
    try:
        return calculate_metric_worker(metric)
    except Exception as e:
        raise e


@app.task(name="merged_queues_metrics",autoretry_for=(Exception,),max_retries=5,default_retry_delay=600)
def queueing_time(metric='merged_queues_metrics'):
    try:
        return calculate_metric_worker(metric)
    except Exception as e:
        raise e


@app.task(name="exclude_outliers",autoretry_for=(Exception,),max_retries=5,default_retry_delay=600)
def exclude_outliers():
    try:
        return exclude_outliers_worker()
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
