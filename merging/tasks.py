import os, sys
ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
BASE_DIR = os.path.join(ROOT_DIR, '..' )
sys.path.append(os.path.abspath(BASE_DIR))
from checker.celery import app
from queues.tasks import queues_statuslog_actual
from rse_info.tasks import save_storage_attrs_to_db
from cric.tasks import cric_resources_to_db
from celery import chain, group
from merging.merge_tables import queues_rse_cric as queues_rse_cric_worker
from merging.merge_tables import dataset_cric_replicas as dataset_cric_replicas_worker


# @app.task(name="etl", ignore_result=True)
# def etl():
#     """Extract, transform and load."""
#     job = group([
#         queues_statuslog_actual(),
#         save_storage_attrs_to_db(),
#         cric_resources_to_db()
#     ])
#     result = job.apply_async()
#     print(result)

@app.task(name="merge", ignore_result=True)
def merge():
    try:
        return queues_rse_cric_worker()
    except Exception as e:
        raise e


@app.task(name="merge_datasets", ignore_result=True)
def merge_datasets():
    try:
        return dataset_cric_replicas_worker()
    except Exception as e:
        raise e


