def get_schema() -> dict:
    return dict(
        type='object',
        additionalProperties=False,
        description='Create BQ Job',
        required=['inputs', 'outputs'],
        properties=dict(
            inputs=dict(
                type='object',
                additionalProperties=False,
                required=['query', 'output_table'],
                properties=dict(
                    query=dict(
                        type='string',
                    ),
                    write_behavior=dict(
                        type='string',
                        enum=['TRUNCATE', "APPEND"],
                        default='TRUNCATE'
                    ),
                    output_table=dict(
                        type='object',
                        required=['project_id', 'dataset_id', 'table_id'],
                        properties=dict(
                            project_id=dict(
                                type='string'
                            ),
                            dataset_id=dict(
                                type='string'
                            ),
                            table_id=dict(
                                type='string'
                            )
                        )
                    )
                )
            ),
            outputs=dict(
                type='object',
                additionalProperties=False,
                required=['job_id'],
                properties=dict(
                    job_id=dict(
                        type='string',
                    ),
                )
            )
        )
    )


def process(integration, inputs, **kwargs):
    from parrot_integrations.gcp.bigquery import create_client
    from google.cloud import bigquery
    from google.cloud.bigquery.job import QueryPriority
    client = create_client(credentials=integration['credentials'], extra_attributes=integration['extra_attributes'])

    dataset_ref = bigquery.DatasetReference(
        dataset_id=inputs['output_table']['dataset_id'],
        project=inputs['output_table']['project_id']
    )
    table_ref = dataset_ref.table(table_id=inputs['output_table']['table_id'])
    job_config = bigquery.QueryJobConfig()
    job_config.default_dataset = dataset_ref
    job_config.destination = table_ref
    job_config.write_disposition = "WRITE_{0}".format(inputs['write_disposition'])
    job_config.priority = QueryPriority.BATCH
    resp = client.query(
        query=inputs['query'],
        job_config=job_config
    )
    return dict(
        job_id=resp.job_id,
        project_id=resp.project_id,
    )
