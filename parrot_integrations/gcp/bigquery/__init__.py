def create_client(credentials, extra_attributes):
    from google.cloud import bigquery
    from google.oauth2 import service_account
    from google.api_core.client_options import ClientOptions
    service_account = service_account.Credentials.from_service_account_info(credentials['service_account'])

    options = ClientOptions(**extra_attributes.get('options', dict()))
    client = bigquery.Client(credentials=service_account, project=extra_attributes['project_id'], client_options=options)
    return client
