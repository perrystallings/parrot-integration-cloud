def create_client(credentials):
    from google.cloud.storage import Client
    from google.oauth2 import service_account
    credentials = service_account.Credentials.from_service_account_info(credentials['service_account'])
    client = Client(credentials=credentials)
    return client