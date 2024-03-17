import pytest
import uuid

@pytest.fixture()
def test_directory():
    import os
    return os.path.abspath(__file__).replace(
        'conftest.py', '')

@pytest.fixture(scope='session')
def run_uuid():
    return str(uuid.uuid4())