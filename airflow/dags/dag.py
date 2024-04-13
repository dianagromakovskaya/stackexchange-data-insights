#dags/example_hello_world.py
"""Example dag for development. We create a dag with two tasks hello_world_1
and hello_world_2 we then trigger 1 and then 2"""

import datetime
import logging
import os
import re
import xml.etree.ElementTree as  ET

import pandas as pd
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator, BigQueryCreateEmptyDatasetOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from google.cloud import storage
from airflow.utils.task_group import TaskGroup


init_url = 'https://archive.org/download/stackexchange/'
xml_file = 'Posts.xml'
BUCKET = 'dtc-de-course-412710_stackexchange-data' # TODO: read from config file
AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
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
    # df = df[['id', 'post_type_id', 'creation_date', 'score', 'view_count', 'body',
    #    'owner_user_id', 'last_activity_date', 'title', 'tags', 'answer_count',
    #    'comment_count', 'closed_date', 'content_license', 'accepted_answer_id',
    #    'last_editor_user_id', 'last_edit_date', 'parent_id',
    #    'owner_display_name', 'community_owned_date',
    #    'last_editor_display_name', 'favorite_count']]
    # df['body'] = df['body'].apply(lambda x: x.replace("\r", " "))
    # df['body'] = df['body'].apply(lambda x: x.replace("\n", " "))
    df.to_csv(csv_file, index=False)
    logging.info(df.columns)
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


with DAG(
    dag_id='upload_stackexchange_data_to_gcs',
    start_date=datetime.datetime(2024, 4, 10),
    tags=['stackexchange-data-insights'],
    schedule_interval='@daily'
) as dag:
    files = ['Posts', 'Tags', 'Badges']
    start = BashOperator(
        task_id='start',
        bash_command=f'echo HELLO'
    )
    for f in files:
        with TaskGroup(f) as tg:
            download_file = BashOperator(
                task_id=f'download_{f}',
                bash_command=f'curl -sSLf {init_url}datascience.stackexchange.com.7z/{f}.xml > {AIRFLOW_HOME}/{f}.xml'
            )
            convert_xml_to_csv = PythonOperator(
                task_id=f'xml_to_csv_{f}',
                python_callable=xml_to_csv,
                op_kwargs=dict(file=f'{AIRFLOW_HOME}/{f}.xml')
            )
            upload_data_to_gcs = PythonOperator(
                task_id=f'data_to_gcs',
                python_callable=upload_to_gcs,
                op_kwargs=dict(bucket=BUCKET,
                               object_name=f'datascience/{f}.csv',
                               local_file=f'{AIRFLOW_HOME}/{f}.csv')
            )
            download_file >> convert_xml_to_csv >> upload_data_to_gcs
    start >> tg

    # download_file = BashOperator(
    #     task_id='download_posts',
    #     bash_command=f'curl -sSLf {init_url}datascience.stackexchange.com.7z/{xml_file} > {AIRFLOW_HOME}/{xml_file}'
    # )
    # convert_xml_to_csv = PythonOperator(
    #     task_id=f'xml_to_csv',
    #     python_callable=xml_to_csv,
    #     op_kwargs=dict(file=f'{AIRFLOW_HOME}/{xml_file}')
    # )
    # upload_data_to_gcs = PythonOperator(
    #     task_id=f'data_to_gcs',
    #     python_callable=upload_to_gcs,
    #     op_kwargs=dict(bucket=BUCKET,
    #                    object_name='datascience/tags.csv',
    #                    local_file=f'{AIRFLOW_HOME}/tags.csv')
    # )
    #
    # load_csv = GCSToBigQueryOperator(
    #     task_id='gcs_to_bigquery_example',
    #     bucket=BUCKET,
    #     source_objects=['datascience/tags.csv'],
    #     destination_project_dataset_table=f"datascience_stackexchange.tags",
    #     write_disposition='WRITE_TRUNCATE'
    # )


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
    # create_external_table = BigQueryCreateExternalTableOperator(
    #     task_id="create_external_table",
    #     destination_project_dataset_table=f"datascience_stackexchange.tags",
    #     bucket=BUCKET,
    #     source_objects=["datascience/tags.csv"],
    #     source_format='CSV',
        # schema_fields=[
        #     {"name": "id", "type": "INTEGER", "mode": "REQUIRED"},
        #     {"name": "tag_name", "type": "STRING", "mode": "NULLABLE"},
        #     {'name': 'count', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        #     {'name': 'excerpt_post_id', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        #     {'name': 'wiki_post_id', 'type': 'INTEGER', 'mode': 'NULLABLE'}
        # ],
        # schema_fields = [
        #     {'name': 'id', 'type': 'STRING', 'mode': 'NULLABLE'},
        #     {'name': 'post_type_id', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        #     {'name': 'creation_date', 'type': 'DATETIME', 'mode': 'NULLABLE'},
        #     {'name': 'score', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        #     {'name': 'view_count', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        #     {'name': 'body', 'type': 'STRING', 'mode': 'NULLABLE'},
        #     {'name': 'owner_user_id', 'type': 'STRING', 'mode': 'NULLABLE'},
        #     {"name": "last_activity_date", "type": "DATETIME", "mode": "NULLABLE"},
        #     {'name': 'title', 'type': 'STRING', 'mode': 'NULLABLE'},
        #     {'name': 'tags', 'type': 'STRING', 'mode': 'NULLABLE'},
        #     {"name": "answer_count", "type": "INTEGER", "mode": "NULLABLE"},
        #     {'name': 'comment_count', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        #     {'name': 'closed_date', 'type': 'DATETIME', 'mode': 'NULLABLE'},
        #     {"name": "content_license", "type": "STRING", "mode": "NULLABLE"},
        #     {"name": "accepted_answer_id", "type": "INTEGER", "mode": "NULLABLE"},
        #     {'name': 'last_editor_user_id', 'type': 'STRING', 'mode': 'NULLABLE'},
        #     {"name": "last_edit_date", "type": "DATETIME", "mode": "NULLABLE"},
        #     {"name": "parent_id", "type": "INTEGER", "mode": "NULLABLE"},
        #     {'name': 'owner_display_name', 'type': 'STRING', 'mode': 'NULLABLE'},
        #     {"name": "community_owned_date", "type": "DATETIME", "mode": "NULLABLE"},
        #     {'name': 'last_editor_display_name', 'type': 'STRING', 'mode': 'NULLABLE'},
        #     {'name': 'favourite_count', 'type': 'INTEGER', 'mode': 'NULLABLE'}
        # ],
        # skip_leading_rows=1
    # )
    # create_dataset_task = BigQueryCreateEmptyDatasetOperator(
    #     task_id="create_dataset",
    #     dataset_id="test_dataset",
    #     location="eu",
    # )
    # test_conn = PythonOperator(
    #     task_id=f'test_conn',
    #     python_callable=test_conn
    # )

    # download_file >> convert_xml_to_csv >> upload_data_to_gcs >> load_csv