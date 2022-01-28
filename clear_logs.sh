#!/bin/bash

rm -r /opt/data_placement/logs/*
echo "Celery Log Files has been removed"

>/opt/data_placement/logs/celery.log

>/opt/data_placement/logs/celerybeat.log

echo "New Log Files were created"