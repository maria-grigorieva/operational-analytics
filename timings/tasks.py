from workers.celery import app
from timings.timings import task_timings_to_db as task_timings_to_db_worker
from timings.timings import job_timings_to_db as job_timings_to_db_worker

@app.task(name="task_timings_to_db",autoretry_for=(Exception,),max_retries=5,default_retry_delay=600)
def task_timings_to_db(hours=24):
    try:
        return task_timings_to_db_worker(hours=hours)
    except Exception as e:
        raise e

@app.task(name="job_timings_to_db",autoretry_for=(Exception,),max_retries=5,default_retry_delay=600)
def job_timings_to_db(hours=1):
    try:
        return job_timings_to_db_worker(hours=hours)
    except Exception as e:
        raise e