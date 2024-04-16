FROM apache/airflow:2.9.0

ENV AIRFLOW_HOME=/opt/airflow

ADD requirements.txt .
RUN pip install -r requirements.txt

COPY ..config/table_schemas $AIRFLOW_HOME/config/table_schemas
COPY ..config/config.yaml $AIRFLOW_HOME/config/table_schemas/config.yaml
COPY ..config/credentials/google_credentials.json $AIRFLOW_HOME/.google/credentials/google_credentials.json