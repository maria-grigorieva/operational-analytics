from celery import Celery
import configparser
from celery.schedules import crontab
#from merging.tasks import etl

config = configparser.ConfigParser()
config.read('config.ini')

app = Celery('checker',
             broker=config['Redis']['connection_string'],
             backend=config['Redis']['connection_string'],
             include=['queues.tasks',
                      'datasets.tasks',
                      'rse_info.tasks',
                      'cric.tasks',
                      'merging.tasks'])


app.conf.beat_schedule = {
    # 'popularity_every_4_hours': {
    #     'task': 'popularity_by_tasks',
    #     'schedule': 3600,
    #     'args': ('2021-12-01 00:00:00',),
    # },
    # 'daily_queueing_time': {
    #     'task': 'merged_queues_metrics',
    #     'schedule': 86400
    # },
    # 'daily_distances': {
    #     'task': 'get_distances',
    #     'schedule': 86400
    # },
    # 'daily_datasets': {
    #     'task': 'datasets_collector',
    #     'schedule': crontab(minute=30, hour=23)
    # },
    'storage_info': {
        'task': 'save_storage_attrs_to_db',
        'schedule': crontab(minute=5, hour=22)
    },
    'cric': {
        'task': 'cric_resources_to_db',
        'schedule': crontab(minute=30, hour=22)
    },
    'queues_statuslog_actual': {
        'task': 'queues_statuslog_actual',
        'schedule': crontab(minute=10, hour=00)
    },
    'datasets_info_daily': {
        'task': 'save_popularity_to_db',
        'schedule': crontab(minute=30, hour=00)
    },
    'datasets_info_daily_v1': {
      'task': 'save_dataset_task_user_to_db',
      'schedule': crontab(minute=40, hour=00)
    },
    'dataset_replicas_to_db': {
        'task': 'dataset_replicas_to_db',
        'schedule': crontab(minute=00, hour=1)
    },
    'merge': {
        'task': 'merge',
        'schedule': crontab(minute=00, hour=3)
    },
    'merge_datasets': {
        'task': 'merge_datasets',
        'schedule': crontab(minute=15, hour=3)
    },
    'merge_datasets_v1': {
        'task': 'merge_datasets_v1',
        'schedule': crontab(minute=20, hour=3)
    },
    # 'run-me-every-10-sec': {
    #     'task': 'checker.tasks.check',
    #     'schedule': 10.0
    # }
    'queues_1hour': {
        'task': 'queues_1hour',
        'schedule': 3600
    },
    'queues_3hours': {
        'task': 'queues_3hours',
        'schedule': 10800
    },
    'queues_6hours': {
        'task': 'queues_6hours',
        'schedule': 21600
    },
    'queues_12hours': {
        'task': 'queues_12hours',
        'schedule': 43200
    },
    'queues_1day': {
        'task': 'queues_1day',
        'schedule': 86400
    },
    'queues_3days': {
        'task': 'queues_3days',
        'schedule': 259200
    },
    'queues_1week': {
        'task': 'queues_1week',
        'schedule': 604800
    }
}
app.conf.timezone = 'Europe/Berlin'

#app.autodiscover_tasks(["tasks"])

app.conf.update(
    result_expires=3600,
)

if __name__ == '__main__':
    app.start()