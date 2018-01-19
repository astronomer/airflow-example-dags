FROM astronomerinc/ap-airflow:latest-onbuild
MAINTAINER Astronomer <humans@astronomer.io>

COPY dags ${AIRFLOW_HOME}/dags
COPY plugins ${AIRFLOW_HOME}/plugins
