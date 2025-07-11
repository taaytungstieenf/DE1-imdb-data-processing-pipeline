#!/bin/bash

PROJECT_DIR="/home/tae/Projects/Capstone 1 – DATA ENGINEERING/DE1-imdb-data-processing-pipeline"
PYTHON="$PROJECT_DIR/.venv/bin/python3"
SCRIPT="$PROJECT_DIR/data_pipeline/step2-from_local_to_mysql.py"
LOGFILE="$PROJECT_DIR/cron_step2.log"

$PYTHON $SCRIPT >> $LOGFILE 2>&1
