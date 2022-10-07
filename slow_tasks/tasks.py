from workers.celery import app
from slow_tasks.tasks_analysis import long_tasks_to_db as long_tasks_to_db_worker
from rse_info.distances import save_distances_to_db as save_distances_to_db_worker

@app.task(name="long_tasks_to_db",autoretry_for=(Exception,),max_retries=5,default_retry_delay=600)
def long_tasks_to_db():
    try:
        return long_tasks_to_db_worker()
    except Exception as e:
        raise e