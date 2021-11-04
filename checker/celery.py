from celery import Celery
import configparser
from celery.schedules import crontab

config = configparser.ConfigParser()
config.read('config.ini')

app = Celery('checker',
             broker=config['Redis']['connection_string'],
             backend=config['Redis']['connection_string'],
             include=['queues.tasks'])

# app.conf.beat_schedule = {
#     'daily_queueing_time': {
#         'task': 'queueing_time',
#         'schedule': crontab(minute=0, hour=2)
#     },
#     'daily_efficiency': {
#         'task': 'efficiency',
#         'schedule': crontab(minute=10, hour=2)
#     },
#     'daily_occupancy': {
#         'task': 'occupancy',
#         'schedule': crontab(minute=20, hour=2)
#     },
#     'daily_shares': {
#         'task': 'job_shares',
#         'schedule': crontab(minute=30, hour=2)
#     },
#     # 'run-me-every-10-sec': {
#     #     'task': 'checker.tasks.check',
#     #     'schedule': 10.0
#     # }
# }

app.conf.beat_schedule = {
    'daily_queueing_time': {
        'task': 'merged_queues_metrics',
        'schedule': 86400
    },
    # 'daily_queueing_time': {
    #     'task': 'queueing_time',
    #     'schedule': 86400
    # },
    # 'daily_efficiency': {
    #     'task': 'efficiency',
    #     'schedule': 86400
    # },
    # 'daily_occupancy': {
    #     'task': 'occupancy',
    #     'schedule': 86400
    # },
    # 'daily_shares': {
    #     'task': 'job_shares',
    #     'schedule': 86400
    # },
    # 'run-me-every-10-sec': {
    #     'task': 'checker.tasks.check',
    #     'schedule': 10.0
    # }
}
app.conf.timezone = 'Europe/Berlin'

#app.autodiscover_tasks(["tasks"])

app.conf.update(
    result_expires=3600,
)