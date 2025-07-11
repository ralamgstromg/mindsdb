from collections import OrderedDict

from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE


connection_args = OrderedDict(
    endpoint_url={
        'type': ARG_TYPE.STR,
        'description': 'The AWS endpoint to connect to. Default is `Amazon S3`.',
        'required': False,
        'label': 'AWS Endpoint URL'
    },
    aws_access_key_id={
        'type': ARG_TYPE.STR,
        'description': 'The AWS access key that identifies the user or IAM role.',
        'required': True,
        'label': 'AWS Access Key'
    },
    aws_secret_access_key={
        'type': ARG_TYPE.STR,
        'description': 'The AWS secret access key that identifies the user or IAM role.',
        'secret': True,
        'required': True,
        'label': 'AWS Secret Access Key'
    },
    bucket={
        'type': ARG_TYPE.STR,
        'description': 'The name of the Amazon S3 bucket.',
        'required': True,
        'label': 'Amazon S3 Bucket'
    },
    region_name={
        'type': ARG_TYPE.STR,
        'description': 'The AWS region to connect to. Default is `us-east-1`.',
        'required': False,
        'label': 'AWS Region'
    },
    aws_session_token={
        'type': ARG_TYPE.STR,
        'description': 'The AWS session token that identifies the user or IAM role. This becomes necessary when using temporary security credentials.',
        'secret': True,
        'required': False,
        'label': 'AWS Session Token'
    },
    use_ssl={
        'type': ARG_TYPE.BOOL,
        'description': 'Specify if endpoint URL use SSL or not. Default is `true`.',
        'required': False,
        'label': 'USE SSL'
    },
    file_format={
        'type': ARG_TYPE.STR,
        'description': 'Set the format for file as parquet, csv, tsv, others',
        'required': False,
        'label': 'File Format'
    },
    file_compressionfile_format={
        'type': ARG_TYPE.STR,
        'description': 'Set the compression algorithm as: uncompressed, snappy, gzip, zstd, brotli, lz4, lz4_raw',
        'required': False,
        'label': 'File Compression'
    },
    file_compression_level={
        'type': ARG_TYPE.INT,
        'description': 'Set the compression level between 1 (lowest) and 22 (highest)',
        'required': False,
        'label': 'File Compression Level'
    },
    parquet_version={
        'type': ARG_TYPE.STR,
        'description': 'Set the default parquet version: V1, V2',
        'required': False,
        'label': 'File Compression'
    },
)

connection_args_example = OrderedDict(
    endpoint_url='s3.amazonaws.com',
    aws_access_key_id='AQAXEQK89OX07YS34OP',
    aws_secret_access_key='wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY',
    aws_session_token='FQoGZXIvYXdzEHcaDmJjJj...',
    region_name='us-east-2',
    bucket='my-bucket',
    use_ssl=True,
)
