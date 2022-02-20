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
                      'merging.tasks',
                      'decision_maker.tasks'])


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
    'weekly_distances': {
        'task': 'save_distances_to_db',
        'schedule': crontab(minute=15, hour=22, day_of_week='sunday')
    },
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
    # 'datasets_info_daily': {
    #     'task': 'save_popularity_to_db',
    #     'schedule': crontab(minute=30, hour=00)
    # },
    'datasets_info_daily_v1': {
      'task': 'save_dataset_task_user_to_db',
      'schedule': crontab(minute=40, hour=00)
    },
    'dataset_replicas_to_db': {
        'task': 'dataset_replicas_to_db',
        'schedule': crontab(minute=00, hour=5)
    },
    'merge': {
        'task': 'merge',
        'schedule': crontab(minute=00, hour=3)
    },
    'merge_datasets': {
        'task': 'merge_datasets',
        'schedule': crontab(minute=00, hour=12)
    },
    'historical_popularity': {
        'task': 'save_historical_popularity_to_db',
        'schedule': crontab(minute=00, hour=8)
    },
    'resource_weights': {
        'task': 'calculate_weights',
        'schedule': crontab(minute=00, hour=6)
    },
    # 'run-me-every-10-sec': {
    #     'task': 'checker.tasks.check',
    #     'schedule': 10.0
    # }
    'queues_1hour': {
        'task': 'queues_1hour',
        'schedule': crontab(minute=0, hour='0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23')
    },
    'queues_3hours': {
        'task': 'queues_3hours',
        'schedule': crontab(minute=0, hour='0,3,6,9,12,15,18,21')
    },
    'queues_6hours': {
        'task': 'queues_6hours',
        'schedule': crontab(minute=0, hour='0,6,12,18')
    },
    'queues_12hours': {
        'task': 'queues_12hours',
        'schedule': crontab(minute=0, hour='0,12')
    },
    'queues_1day': {
        'task': 'queues_1day',
        'schedule': crontab(minute=10, hour=0)
    }
}
app.conf.timezone = 'Europe/Berlin'

#app.autodiscover_tasks(["tasks"])

app.conf.update(
    result_expires=3600,
)

if __name__ == '__main__':
    app.start()