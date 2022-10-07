from workers import Celery
import configparser
from workers.schedules import crontab
#from merging.tasks import etl

config = configparser.ConfigParser()
config.read('config.ini')

app = Celery('workers',
             broker=config['Redis']['connection_string'],
             backend=config['Redis']['connection_string'],
             include=['queues.tasks',
                      'datasets.tasks',
                      'rse_info.tasks',
                      'cric.tasks',
                      'merging.tasks',
                      'decision_maker.tasks',
                      'slow_tasks.tasks'])


app.conf.beat_schedule = {
    'weekly_distances': {
        'task': 'save_distances_to_db',
        'schedule': crontab(minute=15, hour=22, day_of_week='sunday')
    },
    # 'long_tasks_to_db': {
    #     'task': 'long_tasks_to_db',
    #     'schedule': crontab(minute=10, hour=1)
    # },
    'storage_info': {
        'task': 'save_storage_attrs_to_db',
        'schedule': crontab(minute=5, hour=23)
    },
    'cric': {
        'task': 'cric_resources_to_db',
        'schedule': crontab(minute=30, hour=23)
    },
    'tasks_timings': {
        'task': 'task_timings_to_db',
        'schedule': crontab(minute=30, hour=0)
    },
    'jobs_timings': {
        'task': 'job_timings_to_db',
        'schedule': crontab(minute=0, hour='*1')
    },
    'queues_statuslog_actual': {
        'task': 'queues_statuslog_actual',
        'schedule': crontab(minute=10, hour=3)
    },
    'calculate_queue_weights': {
        'task': 'calculate_queue_weights',
        'schedule': crontab(minute=30, hour=4)
    },
    # 'datasets_info_daily_v1': {
    #   'task': 'save_dataset_task_user_to_db',
    #   'schedule': crontab(minute=40, hour=3)
    # },
    # 'dataset_replicas_to_db': {
    #     'task': 'dataset_replicas_to_db',
    #     'schedule': crontab(minute=00, hour=7)
    # },
    # 'merge': {
    #     'task': 'merge',
    #     'schedule': crontab(minute=00, hour=5)
    # },
    # 'merge_datasets': {
    #     'task': 'merge_datasets',
    #     'schedule': crontab(minute=00, hour=12)
    # },
    # 'historical_popularity': {
    #     'task': 'save_historical_popularity_to_db',
    #     'schedule': crontab(minute=00, hour=8)
    # },
    # 'resource_weights': {
    #     'task': 'calculate_weights',
    #     'schedule': crontab(minute=00, hour=8)
    # },
    # 'run-me-every-10-sec': {
    #     'task': 'checker.tasks.check',
    #     'schedule': 10.0
    # }
    # 'queues_1hour': {
    #     'task': 'queues_1hour',
    #     'schedule': crontab(minute=0, hour='0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23')
    # },
    # 'queues_3hours': {
    #     'task': 'queues_3hours',
    #     'schedule': crontab(minute=0, hour='0,3,6,9,12,15,18,21')
    # },
    # 'queues_6hours': {
    #     'task': 'queues_6hours',
    #     'schedule': crontab(minute=0, hour='0,6,12,18')
    # },
    # 'queues_12hours': {
    #     'task': 'queues_12hours',
    #     'schedule': crontab(minute=0, hour='0,12')
    # },
    # 'queues_1day': {
    #     'task': 'queues_1day',
    #     'schedule': crontab(minute=0, hour=3)
    # }
}
# app.conf.enable_utc = True
# app.conf.timezone = 'Africa/Accra'
#app.conf.timezone = 'Europe/London'

app.conf.update(
    result_expires=3600,
    # enable_utc=True,
    timezone='Europe/Zurich'
)

if __name__ == '__main__':
    app.start()