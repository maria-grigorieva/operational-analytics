from workers.celery import app
from popularity.group_popularity import group_popularity_to_db as group_popularity_to_db_worker
from popularity.dataset_popularity import datasets_popularity_to_db as datasets_popularity_to_db_worker

@app.task(name="group_popularity_to_db",autoretry_for=(Exception,),max_retries=5,default_retry_delay=600)
def group_popularity_to_db():
    try:
        return group_popularity_to_db_worker()
    except Exception as e:
        raise e


@app.task(name="datasets_popularity_to_db",autoretry_for=(Exception,),max_retries=5,default_retry_delay=600)
def datasets_popularity_to_db():
    try:
        return datasets_popularity_to_db_worker()
    except Exception as e:
        raise e