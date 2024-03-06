from workers.celery import app
from datetime import datetime, timedelta
from datasets.datasets_popularity import datasets_at_queues as datasets_at_queues_worker

@app.task(name="datasets_at_queues", autoretry_for=(Exception,), max_retries=5, default_retry_delay=600)
def datasets_at_queues():
    try:
        return datasets_at_queues_worker()
    except Exception as e:
        raise e
