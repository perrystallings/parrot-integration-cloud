import uuid

import pytest
from parrot_integrations.core.common import list_operations

INTEGRATION_KEY = 'parrot_integrations.gcp'
OPERATION_KEYS = list(list_operations(integration_key=INTEGRATION_KEY))


@pytest.fixture(scope='session')
def integration_key():
    return INTEGRATION_KEY


@pytest.fixture(params=OPERATION_KEYS)
def gcp_operation(request):
    return request.param


@pytest.fixture(scope='session')
def gcp_credentials():
    import json
    with open('client.json', 'rt') as f:
        client = json.load(f)
    return dict(service_account=client)


@pytest.fixture(scope='session')
def gcp_project_id():
    return 'test-project'


@pytest.fixture(scope='session')
def gcp_integration(gcp_credentials, gcp_project_id):
    return dict(
        extra_attributes=dict(
            project_id=gcp_project_id
        ),
        credentials=gcp_credentials
    )
