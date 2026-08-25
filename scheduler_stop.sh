#!/bin/bash

# kill all celery processes
kill -9 $(ps aux | grep celery | grep -v grep | awk '{print $2}' | tr '\n' ' ') > /dev/null 2>&1

# kill Flower
kill -9 $(ps aux | grep flower | grep -v grep | awk '{print $2}' | tr '\n' ' ') > /dev/null 2>&1