def get_details():
    return dict(
        type='object',
        additionalProperties=False,
        description='Insert Row into BigQuery Table',
        required=['inputs', 'outputs'],
        properties=dict(
            inputs=dict(
                type='object',
                additionalProperties=False,
                required=['dataset_id', 'table_id', 'columns'],
                properties=dict(
                    project_id=dict(
                        type='string'
                    ),
                    dataset_id=dict(
                        type='string'
                    ),
                    table_id=dict(
                        type='string'
                    ),
                    records=dict(
                        type='array',
                        items=dict(
                            type='object',
                            additionalProperties=True
                        )
                    )
                )
            ),
            outputs=dict(
                type='object',
                additionalProperties=False,
                required=['num_rows', 'errors'],
                properties=dict(
                    num_rows=dict(
                        type='integer'
                    ),
                    errors=dict(
                        type='array',
                        items=dict(
                            type='object',
                        )
                    )
                )
            ),
        )
    )


def process(workflow_uuid, account_uuid, node_uuid, processed_ts, inputs, integration,
            **kwargs):
    from google.cloud import bigquery
    from google.oauth2 import service_account
    credentials = service_account.Credentials.from_service_account_info(integration['credentials']['service_account'])
    client = bigquery.Client(credentials=credentials)
    table = client.get_table(
        table='{project_id}.{dataset_id}.{table_id}'.format(
            project_id=inputs['project_id'],
            dataset_id=inputs['dataset_id'],
            table_id=inputs['table_id'],
        )
    )
    errors = client.insert_rows_json(
        table=table,
        json_rows=format_rows(data=inputs['records'], workflow_uuid=workflow_uuid, account_uuid=account_uuid,
                              node_uuid=node_uuid, processed_ts=processed_ts)
    )
    return dict(
        num_rows=len(inputs['records']) - len(errors),
        errors=errors
    )


def format_rows(data, workflow_uuid, account_uuid, node_uuid, processed_ts):
    if not isinstance(data, list):
        data = [data]
    rows = list()
    for record in data:
        row = dict(
            account_uuid=account_uuid,
            workflow_uuid=workflow_uuid,
            node_uuid=node_uuid,
            processed_ts=processed_ts,
            **record
        )
        rows.append(row)
    return rows
