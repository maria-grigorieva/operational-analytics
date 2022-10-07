import os, sys
ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
BASE_DIR = os.path.join(ROOT_DIR, '..' )
sys.path.append(os.path.abspath(BASE_DIR))
from workers.celery import app
from decision_maker.resource_weights import calculate_weights as calculate_weights_worker
from decision_maker.resource_weights import calculate_queue_weights as calculate_queue_weights_worker


@app.task(name="calculate_weights", ignore_result=True)
def calculate_weights():
    try:
        return calculate_weights_worker()
    except Exception as e:
        raise e


@app.task(name="calculate_queue_weights", ignore_result=True)
def calculate_queue_weights():
    try:
        return calculate_queue_weights_worker()
    except Exception as e:
        raise e