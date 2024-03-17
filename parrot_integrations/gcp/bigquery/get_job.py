def get_schema():
    return dict(
        name=f'Get Job',
        description='Get BQ Job details',
        is_trigger=False,
        schema=dict(
            type='object',
            additionalProperties=False,
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
    )


def process(inputs, integration,**kwargs):
    from parrot_integrations.gcp.bigquery import create_client
    from datetime import datetime
    client = create_client(credentials=integration['credentials'], extra_attributes=integration['extra_attributes'])
    job = client.get_job(job_id=inputs['job_id'], project=inputs['project_id'])
    return dict(
        job_id=job.job_id,
        project_id=job.project,
        job_type=job.job_type,
        created_ts=job.created.timestamp() if isinstance(job.created, datetime) else None,
        status=job.state,
    )
