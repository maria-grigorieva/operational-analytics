from checker.celery import app
from distances.distances import get_distances as get_distances_worker

@app.task(name="save_distances_to_db",autoretry_for=(Exception,),max_retries=5,default_retry_delay=600)
def save_distances_to_db():
    try:
        return get_distances_worker()
    except Exception as e:
        raise e