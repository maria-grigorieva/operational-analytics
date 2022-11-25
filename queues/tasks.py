from workers.celery import app
# from queues.queues_metrics import queues_to_db as queues_to_db_worker
from queues.queues_metrics import queues_hourly_to_db as queues_hourly_to_db_worker
from queues.queues_metrics import queues_workload_weighted_detailed as queues_workload_weighted_detailed_worker
from queues.queues_metrics import queues_weighted_jobs as queues_weighted_jobs_worker


@app.task(name="queues_weighted_jobs", autoretry_for=(Exception,), max_retries=5, default_retry_delay=600)
def queues_weighted_jobs():
    try:
        return queues_weighted_jobs_worker()
    except Exception as e:
        raise e

# @app.task(name="queues_statuslog_actual",autoretry_for=(Exception,),max_retries=5,default_retry_delay=600)
# def queues_statuslog_actual(metric='queues_statuslog_actual'):
#     try:
#         return queues_to_db_worker(metric)
#     except Exception as e:
#         raise e


# @app.task(name="check")
# def check():
#     print("I'm checking your stuff!")

#
# @app.task(name="queues_1hour",autoretry_for=(Exception,),max_retries=5,default_retry_delay=600)
# def queues_1hour(metric='queues_hourly',n_hours=1):
#     try:
#         return queues_hourly_to_db_worker(metric,n_hours=n_hours)
#     except Exception as e:
#         raise e
#
# @app.task(name="queues_3hours",autoretry_for=(Exception,),max_retries=5,default_retry_delay=600)
# def queues_1hour(metric='queues_hourly',n_hours=3):
#     try:
#         return queues_hourly_to_db_worker(metric,n_hours=n_hours)
#     except Exception as e:
#         raise e
#
# @app.task(name="queues_6hours",autoretry_for=(Exception,),max_retries=5,default_retry_delay=600)
# def queues_1hour(metric='queues_hourly',n_hours=6):
#     try:
#         return queues_hourly_to_db_worker(metric,n_hours=n_hours)
#     except Exception as e:
#         raise e
#
# @app.task(name="queues_12hours",autoretry_for=(Exception,),max_retries=5,default_retry_delay=600)
# def queues_1hour(metric='queues_hourly',n_hours=12):
#     try:
#         return queues_hourly_to_db_worker(metric,n_hours=n_hours)
#     except Exception as e:
#         raise e
#
# @app.task(name="queues_1day",autoretry_for=(Exception,),max_retries=5,default_retry_delay=600)
# def queues_1hour(metric='queues_hourly',n_hours=24):
#     try:
#         return queues_hourly_to_db_worker(metric,n_hours=n_hours)
#     except Exception as e:
#         raise e
#
# @app.task(name="queues_2days",autoretry_for=(Exception,),max_retries=5,default_retry_delay=600)
# def queues_1hour(metric='queues_statuslog_hourly',n_hours=48):
#     try:
#         return queues_hourly_to_db_worker(metric,n_hours=n_hours)
#     except Exception as e:
#         raise e
#
# @app.task(name="queues_3days",autoretry_for=(Exception,),max_retries=5,default_retry_delay=600)
# def queues_1hour(metric='queues_statuslog_hourly',n_hours=82):
#     try:
#         return queues_hourly_to_db_worker(metric,n_hours=n_hours)
#     except Exception as e:
#         raise e

# @app.task(name="queues_1week",autoretry_for=(Exception,),max_retries=5,default_retry_delay=600)
# def queues_1hour(metric='queues_statuslog_hourly',n_hours=168):
#     try:
#         return queues_hourly_to_db_worker(metric,n_hours=n_hours)
#     except Exception as e:
#         raise e