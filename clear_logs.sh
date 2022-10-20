#!/bin/bash

rm -r /opt/4maria/operational-analytics/logs/*
echo "Celery Log Files has been removed"

>/opt/4maria/operational-analytics/logs/celery.log

>/opt/4maria/operational-analytics/logs/celerybeat.log

echo "New Log Files were created"