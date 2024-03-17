import os
import jsonschema
import pytest
from parrot_integrations.core.common import load_integration_module


@pytest.fixture(scope="module", autouse=True)
def storage_emulator_host_env():
    os.environ["STORAGE_EMULATOR_HOST"] = "http://localhost:9023"


@pytest.fixture(scope="module")
def bucket_name():
    return 'base'


@pytest.fixture(scope="module")
def files(gcp_credentials, run_uuid, bucket_name):
    from parrot_integrations.gcp.storage import create_client
    storage_client = create_client(credentials=gcp_credentials)
    bucket = storage_client.bucket(bucket_name='base')
    file_paths = []
    files = ['file1.txt', 'file2.csv', 'file3.jsonl']

    for file_name in files:
        file_path = f'{run_uuid}/{file_name}'
        bucket.blob(file_path).upload_from_filename('tests/mocks/' + file_name)
        file_paths.append(file_path)
    return file_paths


def test_list_files(gcp_integration, files, run_uuid, bucket_name, integration_key):
    operation = load_integration_module(integration_key=integration_key, operation_key='storage.list_files')
    inputs = dict(
        bucket_name=bucket_name,
        file_prefix=run_uuid
    )
    results = operation.process(
        integration=gcp_integration, inputs=inputs
    )
    assert len(results['files']) == 3
    jsonschema.validate(results, operation.get_schema()['schema']['properties']['outputs'])


def test_read_file(gcp_integration, files, run_uuid, bucket_name, integration_key):
    operation = load_integration_module(integration_key=integration_key, operation_key='storage.read_file')
    for file in files:
        file_type = file.split('.')[-1].upper()
        if file_type == 'TXT':
            file_type = 'TEXT'
        inputs = dict(
            bucket_name=bucket_name,
            file_name=file,
            file_type=file_type
        )
        results = operation.process(
            integration=gcp_integration, inputs=inputs
        )
        jsonschema.validate(results, operation.get_schema()['schema']['properties']['outputs'])