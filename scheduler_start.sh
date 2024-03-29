#!/bin/bash
# start Celery workers
nohup /opt/celery_venv/bin/celery -A workers worker -l info -f logs/celery.log > /dev/null 2>&1&
echo "Celery Worker started"

# start Celerybeat
nohup /opt/celery_venv/bin/celery -A workers beat -l info -f logs/celerybeat.log > /dev/null 2>&1&
echo "Celerybeat started"

# start Flower
nohup /opt/celery_venv/bin/celery -A workers flower --port=5555 -l info > /dev/null 2>&1&
echo "Flower started"
