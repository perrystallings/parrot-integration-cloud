def get_schema() -> dict:
    return dict(
        name=f'Read File',
        description='Read File Contents',
        is_trigger=False,
        schema=dict(
            type='object',
            additionalProperties=False,
            description='Read File Contents',
            required=['inputs', 'outputs'],
            properties=dict(
                inputs=dict(
                    type='object',
                    additionalProperties=False,
                    required=['bucket_name', 'file_name', 'file_type'],
                    properties=dict(
                        bucket_name=dict(
                            type='string'
                        ),
                        file_name=dict(
                            type='string'
                        ),
                        file_type=dict(
                            type='string',
                            enum=['TEXT', 'CSV', 'JSONL']
                        )
                    )
                ),
                outputs=dict(
                    type='object',
                    additionalProperties=False,
                    required=['content'],
                    properties=dict(
                        content=dict(oneOf=[
                            dict(
                                type='array',
                                items=dict(type='object')
                            ),
                            dict(
                                type='string'
                            )
                        ])
                    )
                )
            )
        )
    )


def process(integration, inputs, **kwargs):
    """List files in a bucket that match a prefix"""

    from parrot_integrations.gcp.storage import create_client
    from tempfile import TemporaryFile
    client = create_client(credentials=integration['credentials'])
    blob = client.bucket(bucket_name=inputs['bucket_name']).blob(inputs['file_name'])

    with TemporaryFile('wt+') as file:
        file.write(blob.download_as_string().decode('utf-8'))
        file.seek(0)
        output = process_file(file_type=inputs['file_type'], file=file)

    return dict(
        content=output
    )


def process_file(file_type, file):
    import json
    import csv
    output = None
    if file_type == 'TEXT':
        output = file.read()
    elif file_type == 'CSV':
        reader = csv.DictReader(file)
        output = list(reader)
    elif file_type == 'JSONL':
        output = list()
        for line in file.readlines():
            output.append(json.loads(line))
    return output
