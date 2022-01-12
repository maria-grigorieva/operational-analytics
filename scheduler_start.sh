#!/bin/bash
# start Celery workers
nohup /opt/venv/py37/bin/celery -A checker worker -l info -f logs/celery.log > /dev/null 2>&1&
echo "Celery Worker started"

# start Celerybeat
nohup /opt/venv/py37/bin/celery -A checker beat -l info -f logs/celerybeat.log > /dev/null 2>&1&
echo "Celerybeat started"

# start Flower
nohup /opt/venv/py37/bin/celery -A checker flower -l debug -f logs/flower.log > /dev/null 2>&1&
echo "Flower started"
