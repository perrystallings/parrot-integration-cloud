def get_schema():
    return dict(
        name=f'Insert Rows',
        description='Insert Row into BigQuery Table',
        is_trigger=False,
        schema=dict(
            type='object',
            additionalProperties=False,
            required=['inputs', 'outputs'],
            properties=dict(
                inputs=dict(
                    type='object',
                    additionalProperties=False,
                    required=['project_id', 'dataset_id', 'table_id', 'records'],
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
    )


def process(inputs, integration, **kwargs):
    from parrot_integrations.gcp.bigquery import create_client
    client = create_client(credentials=integration['credentials'], extra_attributes=integration['extra_attributes'])

    table = client.get_table(
        table='{project_id}.{dataset_id}.{table_id}'.format(
            project_id=inputs['project_id'],
            dataset_id=inputs['dataset_id'],
            table_id=inputs['table_id'],
        )
    )
    errors = client.insert_rows_json(
        table=table,
        json_rows=inputs['records']
    )
    return dict(
        num_rows=len(inputs['records']) - len(errors),
        errors=errors
    )
