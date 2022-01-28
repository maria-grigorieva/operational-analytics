from celery import Celery
import configparser
from celery.schedules import crontab
#from merging.tasks import etl

config = configparser.ConfigParser()
config.read('config.ini')

app = Celery('queues_snapshots',
             broker=config['Redis']['connection_string'],
             backend=config['Redis']['connection_string'],
             include=['queues.tasks'])


app.conf.beat_schedule = {
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

app.conf.update(
    result_expires=3600,
)

if __name__ == '__main__':
    app.start()