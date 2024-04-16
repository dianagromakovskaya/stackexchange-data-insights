import json
import json
import logging
import os
import re
import xml.etree.ElementTree as  ET

import pandas as pd
import yaml
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyTableOperator, \
    BigQueryDeleteTableOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.utils.task_group import TaskGroup
from google.cloud import storage

with open('config/config.yaml', 'r') as cfg:
    CONFIG = yaml.safe_load(cfg)
INIT_URL = CONFIG['init_url']
BUCKET = f"{CONFIG['gcp']['project_id']}_{CONFIG['gcp']['bucket']}"
AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
GCP_PROJECT = CONFIG['gcp']['project_id']
SERVICES = CONFIG['services']
DATASET = CONFIG['gcp']['dataset']
START_DATE = CONFIG['start_date']


logging.basicConfig(level=logging.INFO)


def get_field(row, field_name):
    try:
        value = row.attrib[field_name]
    except KeyError:
        value = ''
    return value


def xml_to_csv(file):
    logging.info('Converting XML to CSV')
    rows = []
    xmlparse = ET.parse(file)
    root = xmlparse.getroot()

    for row in root:
        record = {f: row.attrib[f] for f in row.attrib}
        rows.append(record)

    df = pd.DataFrame.from_records(rows)
    df.rename(columns=lambda x: re.sub(r'([a-z0-9])([A-Z])', r'\1_\2', x).lower(), inplace=True)
    csv_file = file.replace('xml', 'csv')
    df['id'] = df['id'].fillna(0)
    df.to_csv(csv_file, index=False)
    return csv_file



def upload_to_gcs(bucket, object_name, local_file):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    """
    # # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # # (Ref: https://github.com/googleapis/python-storage/issues/74)
    # storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    # storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB
    logging.info(f'Uploading {local_file} data to GCS')
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB
    client = storage.Client()
    bucket = client.bucket(bucket)
    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)


def get_table_schema(table):
    with open(f'config/table_schemas/{table}.json', 'r') as file:
        table_schema = json.load(file)['schema']
    return table_schema

for SERVICE in SERVICES:
    with DAG(
        dag_id=f'upload_{SERVICE}_stackexchange_data_to_gcs',
        start_date=START_DATE,
        tags=['stackexchange-data-insights'],
        schedule_interval='@daily'
    ) as dag:
        files = ['badges', 'comments', 'post_history', 'post_links', 'posts', 'tags', 'users', 'votes']
        prev_group = None
        for f in files:
            with TaskGroup(f) as tg:
                download_file = BashOperator(
                    task_id=f'download_{f}',
                    bash_command=f'curl -sSLf {INIT_URL}{SERVICE}.stackexchange.com.7z/{"".join([w.title() for w in f.split("_")])}.xml > {AIRFLOW_HOME}/data/{SERVICE}_{f}.xml'
                )
                convert_xml_to_csv = PythonOperator(
                    task_id=f'xml_to_csv_{f}',
                    python_callable=xml_to_csv,
                    op_kwargs=dict(file=f'{AIRFLOW_HOME}/data/{SERVICE}_{f}.xml')
                )
                upload_data_to_gcs = PythonOperator(
                    task_id=f'csv_to_gcs_{f}',
                    python_callable=upload_to_gcs,
                    op_kwargs=dict(bucket=BUCKET,
                                   object_name=f'{SERVICE}/{f}.csv',
                                   local_file=f'{AIRFLOW_HOME}/data/{SERVICE}_{f}.csv')
                )
                delete_bq_table = BigQueryDeleteTableOperator(
                    task_id=f'delete_bq_table_{f}',
                    deletion_dataset_table=f'{GCP_PROJECT}.{DATASET}.{SERVICE}_{f}',
                    ignore_if_missing=True
                )
                create_bq_table = BigQueryCreateEmptyTableOperator(
                    task_id=f'create_bq_table_{f}',
                    dataset_id=DATASET,
                    table_id=f'{SERVICE}_{f}',
                    schema_fields=get_table_schema(f)['fields'],
                    time_partitioning=get_table_schema(f).get('timePartitioning'),
                    project_id=GCP_PROJECT,
                    if_exists='log'
                )
                gcs_to_bq = GCSToBigQueryOperator(
                    task_id=f'gcs_to_bq_{f}',
                    bucket=BUCKET,
                    source_objects=[f'{SERVICE}/{f}.csv'],
                    destination_project_dataset_table=f"{DATASET}.{SERVICE}_{f}",
                    write_disposition='WRITE_TRUNCATE',
                    allow_quoted_newlines=True,
                    max_bad_records=10
                )

                delete_local_file = BashOperator(
                    task_id='delete_local_file',
                    bash_command=f'rm -f {AIRFLOW_HOME}/data/{f}.xml {AIRFLOW_HOME}/data/{f}.csv'
                )


                if prev_group:
                    prev_group >> download_file
                download_file >> convert_xml_to_csv >> upload_data_to_gcs >> delete_bq_table >> create_bq_table >> gcs_to_bq
                prev_group = tg

        delete_local_files = BashOperator(
            task_id='delete_local_files',
            bash_command=f"rm -f {AIRFLOW_HOME}/data/*",
            dag=dag
        )
        prev_group >> delete_local_files
