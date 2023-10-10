from celery import Celery
import configparser
from celery.schedules import crontab
#from merging.tasks import etl

config = configparser.ConfigParser()
config.read('config.ini')

app = Celery('workers',
             broker=config['Redis']['connection_string'],
             backend=config['Redis']['connection_string'],
             include=['queues.tasks',
                      #'datasets.tasks',
                      'rse_info.tasks',
                      'cric.tasks',
                      #'merging.tasks',
                      #'decision_maker.tasks',
                      #'slow_tasks.tasks',
                      'popularity.tasks'])


app.conf.beat_schedule = {
    # 'weekly_distances': {
    #     'task': 'save_distances_to_db',
    #     'schedule': crontab(minute=15, hour=22, day_of_week='sunday')
    # },
    'storage_info': {
        'task': 'save_storage_attrs_to_db',
        'schedule': crontab(minute=55, hour=23)
    },
    'cric': {
        'task': 'cric_resources_to_db',
        'schedule': crontab(minute=30, hour=23)
    },
    # 'tasks_timings': {
    #     'task': 'task_timings_to_db',
    #     'schedule': crontab(minute=0, hour=4)
    # },
    # 'jobs_timings': {
    #     'task': 'job_timings_to_db',
    #     'schedule': crontab(minute=10, hour=3)
    # },
    # 'queues_workload': {
    #     'task': 'queues_workload',
    #     'schedule': crontab(minute=0, hour='0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23')
    # },
    'queues_workload_extended': {
        'task': 'queues_workload_extended',
        'schedule': crontab(minute=0, hour='0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23')
    },
    # 'queues_workload_weighted': {
    #     'task': 'queues_workload_weighted',
    #     'schedule': crontab(minute=0, hour='1,5,9,13,17,21')
    # },
    'group_popularity_to_db': {
        'task': 'group_popularity_to_db',
        'schedule': crontab(minute=0, hour=3, day_of_week='monday')
    },
    'group_popularity_daily_to_db': {
        'task': 'group_popularity_daily_to_db',
        'schedule': crontab(minute=3, hour=3)
    },
    'datasets_popularity_to_db': {
        'task': 'datasets_popularity_to_db',
        'schedule': crontab(minute=30, hour=3, day_of_week='monday')
    },
    'aggregation_week': {
        'task': 'aggregation_week',
        'schedule': crontab(minute=0, hour=10, day_of_week='monday')
    }
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