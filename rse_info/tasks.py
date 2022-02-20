from checker.celery import app
from rse_info.storage_info import save_storage_attrs_to_db as save_storage_attrs_to_db_worker
from rse_info.distances import save_distances_to_db as save_distances_to_db_worker

@app.task(name="save_storage_attrs_to_db",autoretry_for=(Exception,),max_retries=5,default_retry_delay=600)
def save_storage_attrs_to_db():
    try:
        return save_storage_attrs_to_db_worker()
    except Exception as e:
        raise e

#
# @app.task(name="save_distances_to_db", autoretry_for=(Exception,), max_retries=5, default_retry_delay=600)
# def save_distances_to_db():
#     try:
#         return save_distances_to_db_worker()
#     except Exception as e:
#         raise e
