from parrot_integrations.core.validation import validate_integration, validate_operation
from parrot_integrations.core.common import load_integration_module


def test_valid_integration_schema(integration_key):
    validate_integration(integration_key=integration_key)


def test_valid_operation_schemas(integration_key, gcp_operation):
    validate_operation(integration_key=integration_key, operation_key=gcp_operation)

def test_integration(integration_key, gcp_integration):
    integration = load_integration_module(integration_key=integration_key)
    integration.connect(extra_attributes=gcp_integration['extra_attributes'], credentials=gcp_integration['credentials'])