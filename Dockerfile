FROM astronomerinc/ap-airflow:1.10.7-alpine3.10-onbuild
ENV AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION=False
