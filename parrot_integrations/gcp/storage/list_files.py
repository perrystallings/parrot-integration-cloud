def get_schema() -> dict:
    return dict(
        type='object',
        additionalProperties=False,
        description='List files in bucket path ',
        required=['inputs', 'outputs'],
        properties=dict(
            inputs=dict(
                type='object',
                additionalProperties=False,
                required=['bucket_name', 'file_prefix'],
                properties=dict(
                    bucket_name=dict(
                        type='string'
                    ),
                    file_prefix=dict(
                        type='string'
                    ),
                    expiration_seconds=dict(
                        type='integer',
                        default=86400
                    )

                )
            ),
            outputs=dict(
                type='object',
                additionalProperties=False,
                required=['files'],
                properties=dict(
                    files=dict(
                        type='array',
                        items=dict(
                            type='object',
                            additionalProperties=False,
                            required=[
                                'url',
                                'file_name'
                            ],
                            properties=dict(
                                url=dict(
                                    type="string"
                                ),
                                file_name=dict(
                                    type='string'
                                ),
                            )
                        )
                    )
                )
            )
        )
    )


def process(integration, inputs, **kwargs):
    """List files in a bucket that match a prefix"""

    from parrot_integrations.gcp.storage import create_client
    from datetime import datetime, timedelta
    client = create_client(credentials=integration['credentials'])
    expiration_ts = datetime.utcnow() + timedelta(seconds=integration.get('expiration_seconds', 86400))
    files = []
    for blob in client.list_blobs(bucket=inputs['bucket_name'], prefix=inputs['file_prefix']):
        files.append(
            dict(
                url=blob.generate_signed_url(expiration=expiration_ts),
                file_name=blob.name,
            )
        )

    return dict(
        files=files
    )
