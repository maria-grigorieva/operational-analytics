from checker.celery import app
from cric.cric_json_api import cric_resources_to_db as cric_resources_to_db_worker

@app.task(name="cric_resources_to_db",autoretry_for=(Exception,),max_retries=5,default_retry_delay=600)
def cric_resources_to_db():
    try:
        return cric_resources_to_db_worker()
    except Exception as e:
        raise e