DEFAULT_PATTERN = '{workflow_uuid}/{node_uuid}/{processed}/{message_id}.{file_type}'


def get_schema() -> dict:
    return dict(
        name=f'Export Table',
        description='Export BQ Table',
        is_trigger=False,
        schema=dict(
            type='object',
            additionalProperties=False,
            required=['inputs', 'outputs'],
            properties=dict(
                inputs=dict(
                    type='object',
                    additionalProperties=False,
                    required=['project_id', 'dataset_id', 'table_id', 'output_file'],
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
                        output_file={
                            "type": 'object',
                            "required": ['file_type'],
                            "properties": dict(
                                bucket_name=dict(
                                    type='string'
                                ),
                                file_pattern=dict(
                                    type='string',
                                ),
                                compressed=dict(
                                    type='boolean',
                                    default=False
                                ),
                                file_type=dict(
                                    type='string',
                                    enum=['CSV', "JSONL", ]
                                ),
                            ),
                            "if": {
                                "properties": {
                                    "file_type": {"const": "CSV"}
                                }
                            },
                            "then": {
                                "properties": {
                                    "delimiter": dict(
                                        type='string',
                                        default=','
                                    )
                                }
                            }
                        }
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
                        project_id=dict(
                            type='string',
                        ),
                        bucket_name=dict(
                            type='string',
                        ),
                        file_pattern=dict(
                            type='string',
                        ),
                    )
                )
            )
        )
    )


def process(integration, inputs, bucket, processed_ts, **kwargs):
    """Export a table from the service data set to the local service bucket based on a desired file pattern.
    Must include * wildcard"""

    from parrot_integrations.gcp.bigquery import create_client
    from google.cloud import bigquery

    bq_client = create_client(credentials=integration['credentials'], extra_attributes=integration['extra_attributes'])
    dataset_ref = bigquery.DatasetReference(
        dataset_id=inputs['dataset_id'],
        project=inputs['project_id']
    )
    table_ref = dataset_ref.table(table_id=inputs['table_id'])

    job_config = bigquery.ExtractJobConfig()

    if inputs['output_file']['file_type'] == 'CSV':
        job_config.field_delimiter = inputs['output_file']['delimiter']
        job_config.destination_format = bigquery.DestinationFormat.CSV
        job_config.print_header = True
    elif inputs['output_file']['file_type'] == 'JSONL':
        job_config.destination_format = bigquery.DestinationFormat.NEWLINE_DELIMITED_JSON
    if inputs['output_file'].get('bucket_name'):
        output_bucket = inputs['output_file']['bucket_name']
        file_pattern = inputs['output_file']['file_pattern']
    else:
        output_bucket = bucket
        file_pattern = DEFAULT_PATTERN.format(
            processed=processed_ts.strftime('%Y/%m/%d/%H/%M/%S'),
            file_type=inputs['output_file']['file_type'].lower(),
            **kwargs
        )
    if inputs['output_file'].get('compressed', False):
        job_config.compression = bigquery.job.Compression.GZIP
        file_pattern += '.gz'
    resp = bq_client.extract_table(
        source=table_ref,
        destination_uris='gs://' + output_bucket + '/' + file_pattern,
        job_config=job_config,
    )
    return dict(
        job_id=resp.job_id,
        project_id=resp.project,
        bucket_name=bucket,
        file_pattern=file_pattern
    )
