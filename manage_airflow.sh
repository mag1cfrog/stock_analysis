#!/bin/bash


case "$1" in
  start)
    echo "Starting Airflow webserver and scheduler..."
    airflow scheduler -D
    airflow webserver -D
    ;;
  stop)
    echo "Stopping Airflow webserver and scheduler..."
    pkill -f 'airflow-webserver'
    pkill -f 'airflow scheduler'
    ;;
  *)
    echo "Usage: $0 {start|stop}"
    exit 1
    ;;
esac

