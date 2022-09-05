from checker.celery import app
from timings.timings import task_timings_to_db as task_timings_to_db_worker

@app.task(name="task_timings_to_db",autoretry_for=(Exception,),max_retries=5,default_retry_delay=600)
def task_timings_to_db():
    try:
        return task_timings_to_db_worker()
    except Exception as e:
        raise e