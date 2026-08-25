#!/bin/bash
# Define the log directory
LOG_DIR="/opt/4maria/operational-analytics/logs"

# Find and truncate all log files (modify the pattern if needed)
find "$LOG_DIR" -type f -name "*.log" -exec truncate -s 0 {} \;

# Optionally, log the cleanup activity
echo "$(date): Cleared log files in $LOG_DIR" >> /opt/4maria/operational-analytics/log_cleanup.log

#rm -r /opt/4maria/operational-analytics/logs/*
#echo "Celery Log Files has been removed"

#>/opt/4maria/operational-analytics/logs/celery.log

#>/opt/4maria/operational-analytics/logs/celerybeat.log

#echo "New Log Files were created"
