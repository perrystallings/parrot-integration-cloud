def create_client(credentials, extra_attributes):
    from google.cloud import bigquery
    from google.oauth2 import service_account
    credentials = service_account.Credentials.from_service_account_info(credentials['service_account'])
    client = bigquery.Client(credentials=credentials, project=extra_attributes['project_id'])
    return client
