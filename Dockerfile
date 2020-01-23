FROM astronomerinc/ap-airflow:0.10.3-1.10.5-onbuild
ENV AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION=False