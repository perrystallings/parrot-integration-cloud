import uuid

import jsonschema
import pytest
from google.cloud import bigquery
from parrot_integrations.core.common import load_integration_module

from parrot_integrations.gcp.bigquery import create_client


@pytest.fixture(scope="module")
def bigquery_integration(gcp_integration):
    gcp_integration['extra_attributes']['options'] = dict(api_endpoint="http://bigquery:9050")
    return gcp_integration


@pytest.fixture(scope="module")
def dataset(bigquery_integration):
    client = create_client(**bigquery_integration)
    dataset_name = str(uuid.uuid4()).replace('-', '_')
    client.create_dataset(dataset_name)
    return dataset_name


@pytest.fixture(scope="module")
def table(bigquery_integration, dataset):
    client = create_client(**bigquery_integration)
    schema = [
        bigquery.SchemaField("id", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("first_name", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("last_name", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("address", "STRING", mode="NULLABLE")
    ]
    table_name = str(uuid.uuid4()).replace('-', '_')
    # Create a new table instance
    table_ref = client.dataset(dataset).table(table_name)
    table = bigquery.Table(table_ref, schema=schema)
    # Create the table on BigQuery
    client.create_table(table)
    return table_name


@pytest.fixture(scope="module")
def rows(bigquery_integration, dataset, table):
    client = create_client(**bigquery_integration)
    table_ref = client.dataset(dataset).table(table)
    rows_to_insert = [
        {"id": "1", "first_name": "Adam", "last_name": "Smith", "address": "12 Main Street"},
        {"id": "2", "first_name": "Jacques", "last_name": "Dupont", "address": "12 rue de la Ville"},
        {"id": "3", "first_name": "Antonio", "last_name": "Perez", "address": "12 calle Madrid"}
    ]
    client.insert_rows_json(table_ref, rows_to_insert)
    return rows_to_insert


@pytest.fixture(scope="module")
def job(bigquery_integration, dataset, integration_key, table):
    inputs = dict(
        query='select count(*) from `{dataset}.{table}` as num',
        write_behavior='TRUNCATE',
        output_table=dict(
            project_id='test',
            dataset_id=dataset,
            table_id='query_test'
        )
    )
    operation = load_integration_module(integration_key=integration_key, operation_key='bigquery.execute_query')
    results = operation.process(
        integration=bigquery_integration, inputs=inputs
    )
    return results['job_id']


def test_execute_query(bigquery_integration, integration_key, dataset, table):
    inputs = dict(
        query='select count(*) from `{dataset}.{table}` as num',
        write_behavior='TRUNCATE',
        output_table=dict(
            project_id='test',
            dataset_id=dataset,
            table_id='query_test'
        )
    )
    operation = load_integration_module(integration_key=integration_key, operation_key='bigquery.execute_query')
    results = operation.process(
        integration=bigquery_integration, inputs=inputs
    )
    jsonschema.validate(results, operation.get_schema()['schema']['properties']['outputs'])


def test_get_job(bigquery_integration, integration_key, job, gcp_project_id):
    inputs = dict(
        job_id=job,
        project_id=gcp_project_id
    )
    operation = load_integration_module(integration_key=integration_key, operation_key='bigquery.get_job')
    results = operation.process(
        integration=bigquery_integration, inputs=inputs
    )
    jsonschema.validate(results, operation.get_schema()['schema']['properties']['outputs'])


def test_insert_rows(bigquery_integration, integration_key, dataset, table, gcp_project_id):
    inputs = dict(
        project_id=gcp_project_id,
        dataset_id=dataset,
        table_id=table,
        records=[
            {"id": "1", "first_name": "Adam", "last_name": "Smith", "address": "12 Main Street"},
            {"id": "2", "first_name": "Jacques", "last_name": "Dupont", "address": "12 rue de la Ville"},
            {"id": "3", "first_name": "Antonio", "last_name": "Perez", "address": "12 calle Madrid"}
        ]
    )
    operation = load_integration_module(integration_key=integration_key, operation_key='bigquery.insert_rows')
    results = operation.process(
        integration=bigquery_integration, inputs=inputs
    )
    jsonschema.validate(results, operation.get_schema()['schema']['properties']['outputs'])


def test_export_table_to_known_bucket(bigquery_integration, integration_key, dataset, table, gcp_project_id):
    from datetime import datetime
    inputs = dict(
        project_id=gcp_project_id,
        dataset_id=dataset,
        table_id=table,
        output_file=dict(
            bucket_name='base',
            file_pattern='test/*.csv',
            compressed=True,
            file_type='CSV',
            delimiter=','
        )
    )

    operation = load_integration_module(integration_key=integration_key, operation_key='bigquery.export_table')
    results = operation.process(
        integration=bigquery_integration, inputs=inputs, bucket='base', processed_ts=datetime.utcnow()
    )
    jsonschema.validate(results, operation.get_schema()['schema']['properties']['outputs'])


def test_export_table_to_default_bucket(bigquery_integration, integration_key, dataset, table, gcp_project_id):
    from datetime import datetime
    inputs = dict(
        project_id=gcp_project_id,
        dataset_id=dataset,
        table_id=table,
        output_file=dict(
            file_type='JSONL',
            compressed=False
        )
    )

    operation = load_integration_module(integration_key=integration_key, operation_key='bigquery.export_table')
    results = operation.process(
        integration=bigquery_integration, inputs=inputs, bucket='base', processed_ts=datetime.utcnow(), workflow_uuid=str(uuid.uuid4()),
        node_uuid=str(uuid.uuid4()), message_id=str(uuid.uuid4())
    )
    jsonschema.validate(results, operation.get_schema()['schema']['properties']['outputs'])
