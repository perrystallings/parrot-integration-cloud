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
                required=['project_id', 'job_id'],
                properties=dict(
                    project_id=dict(
                        type='string'
                    ),
                    job_id=dict(
                        type='string'
                    )
                )
            ),
            outputs=dict(
                type='object',
                additionalProperties=False,
                required=['job_id', 'status'],
                properties=dict(
                    project_id=dict(
                        type='string'
                    ),
                    job_id=dict(
                        type='string'
                    ),
                    job_type=dict(
                        type='string'
                    ),
                    created_ts=dict(
                        type=['number', 'null']
                    ),
                    status=dict(
                        type='string'
                    )
                )
            ),
        )
    )


def process(workflow_uuid, account_uuid, node_uuid, processed_ts, inputs, integration,
            **kwargs):
    from google.cloud import bigquery
    from google.oauth2 import service_account
    from datetime import datetime
    credentials = service_account.Credentials.from_service_account_info(integration['credentials']['service_account'])
    client = bigquery.Client(credentials=credentials)
    job = client.get_job(job_id=inputs['job_id'], project=inputs['project_id'])
    return dict(
        job_id=job.job_id,
        project_id=job.project,
        job_type=job.job_type,
        created_ts=job.created.timestamp() if isinstance(job.created, datetime) else None,
        status=job.state,
    )
