from checker.celery import app
from datasets.dataset_replicas_collector import dataset_replicas_to_db as dataset_replicas_to_db_worker


@app.task(name="dataset_replicas_to_db",autoretry_for=(Exception,),max_retries=5,default_retry_delay=300)
def dataset_replicas_to_db():
    try:
        return dataset_replicas_to_db_worker()
    except Exception as e:
        raise e