#dags/example_hello_world.py
"""Example dag for development. We create a dag with two tasks hello_world_1
and hello_world_2 we then trigger 1 and then 2"""

import datetime
import logging
import os
import re
import xml.etree.ElementTree as  ET

import pandas as pd
import requests
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator, BigQueryCreateEmptyDatasetOperator
from google.cloud import storage


init_url = 'https://archive.org/download/stackexchange/'
xml_file = 'Tags.xml'
BUCKET = 'dtc-de-course-412710_stackexchange-data' # TODO: read from config file
AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
logging.basicConfig(level=logging.INFO)

# TODO: read from config
# COLS = [
#     'AcceptedAnswerId',
#     'AnswerCount',
#     'Body',
#     'ClosedDate',
#     'CommentCount',
#     'CommunityOwnedDate',
#     'ContentLicense',
#     'CreationDate',
#     'FavoriteCount',
#     'Id',
#     'LastActivityDate',
#     'LastEditDate',
#     'LastEditorDisplayName',
#     'LastEditorUserId',
#     'OwnerDisplayName',
#     'OwnerUserId',
#     'ParentId',
#     'PostTypeId',
#     'Score',
#     'Tags',
#     'Title',
#     'ViewCount'
# ]
COLS = ["Id", "TagName", "Count", "ExcerptPostId", 'WikiPostId']


def get_field(row, field_name):
    try:
        value = row.attrib[field_name]
    except KeyError:
        value = ''
    return value


def xml_to_csv(file, cols):
    logging.info('Converting XML to CSV')
    rows = []
    xmlparse = ET.parse(file)
    root = xmlparse.getroot()

    for row in root:
        post_info = [get_field(row, f) for f in cols]
        rows.append(post_info)

    df = pd.DataFrame(rows, columns=cols)
    df.rename(columns=lambda x: re.sub(r'([a-z0-9])([A-Z])', r'\1_\2', x).lower(), inplace=True)
    csv_file = file.replace('xml', 'csv').lower()
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


def test_conn():
    logging.info(os.environ.get('AIRFLOW_CONN_GOOGLE_CLOUD_DEFAULT'))


with DAG(
    dag_id='upload_stackexchange_data_to_gcs',
    start_date=datetime.datetime(2024, 4, 10),
    tags=['stackexchange-data-insights'],
    schedule_interval='@daily'
) as dag:
    download_file = BashOperator(
        task_id='download_tags',
        bash_command=f'curl -sSLf {init_url}datascience.stackexchange.com.7z/{xml_file} > {AIRFLOW_HOME}/{xml_file}'
    )
    convert_xml_to_csv = PythonOperator(
        task_id=f'xml_to_csv',
        python_callable=xml_to_csv,
        op_kwargs=dict(file=f'{AIRFLOW_HOME}/{xml_file}',
                       cols=COLS)
    )
    upload_data_to_gcs = PythonOperator(
        task_id=f'data_to_gcs',
        python_callable=upload_to_gcs,
        op_kwargs=dict(bucket=BUCKET,
                       object_name='datascience/tags.csv',
                       local_file=f'{AIRFLOW_HOME}/tags.csv')
    )
    # bigquery_external_table_task = BigQueryCreateExternalTableOperator(
    #     task_id='bigquery_external_table_task',
    #     table_resource={
    #         'tableReference': {
    #             'projectId': 'dtc-de-course-412710',
    #             'datasetId': 'datascience_stackexchange',
    #             "tableId": 'tags'
    #         },
    #         'externalDataConfiguration': {
    #             'sourceFormat': 'CSV',
    #             'sourceUris': [f'gs://{BUCKET}/datascience/tags.csv']
    #         },
    #     },
    #
    # )
    # bigquery_external_table_task = BigQueryCreateExternalTableOperator(
    #     task_id=f"create_external_table",
    #     bucket=BUCKET,
    #     source_objects=["/datascience/tags.csv"], #pass a list
    #     destination_project_dataset_table=f"dtc-de-course-412710.datascience_stackexchange.tags",
    #     source_format='CSV', #use source_format instead of file_format
    # )
    # "Count", "ExcerptPostId", 'WikiPostId'
    create_external_table = BigQueryCreateExternalTableOperator(
        task_id="create_external_table",
        destination_project_dataset_table=f"datascience_stackexchange.tags",
        bucket=BUCKET,
        source_objects=["datascience/tags.csv"],
        schema_fields=[
            {"name": "id", "type": "INTEGER", "mode": "REQUIRED"},
            {"name": "tag_name", "type": "STRING", "mode": "NULLABLE"},
            {'name': 'count', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'excerpt_post_id', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'wiki_post_id', 'type': 'INTEGER', 'mode': 'NULLABLE'}
        ],
        skip_leading_rows=1
    )
    # create_dataset_task = BigQueryCreateEmptyDatasetOperator(
    #     task_id="create_dataset",
    #     dataset_id="test_dataset",
    #     location="eu",
    # )
    # test_conn = PythonOperator(
    #     task_id=f'test_conn',
    #     python_callable=test_conn
    # )

    download_file >> convert_xml_to_csv >> upload_data_to_gcs >> create_external_table