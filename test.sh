#!/bin/bash
gcp-storage-emulator start --host=localhost --port=9023 --in-memory --default-bucket=base &
pytest --verbose --cov=./parrot_integrations --cov-fail-under=90 -rfE -p no:warnings