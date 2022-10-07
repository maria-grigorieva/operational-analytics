#!/bin/bash

# kill all celery processes
kill -9 $(ps aux | grep celery | grep -v grep | awk '{print $2}' | tr '\n' ' ') > /dev/null 2>&1

# kill Flower
kill -9 $(ps aux | grep flower | grep -v grep | awk '{print $2}' | tr '\n' ' ') > /dev/null 2>&1

# start Celery workers
nohup /opt/venv/py37/bin/celery -A workers worker -l info -f logs/celery.log > /dev/null 2>&1&

# start Celerybeat
nohup /opt/venv/py37/bin/celery -A workers beat -l info -f logs/celerybeat.log > /dev/null 2>&1&

# start Flower
nohup /opt/venv/py37/bin/celery -A workers flower -l info -f logs/flower.log > /dev/null 2>&1&