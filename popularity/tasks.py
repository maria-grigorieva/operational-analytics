from workers.celery import app
from popularity.group_popularity import group_popularity_to_db as group_popularity_to_db_worker

@app.task(name="group_popularity_to_db",autoretry_for=(Exception,),max_retries=5,default_retry_delay=600)
def group_popularity_to_db(hours=24):
    try:
        return group_popularity_to_db_worker()
    except Exception as e:
        raise e