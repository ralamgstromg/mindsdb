from typing import List
from contextlib import contextmanager

import boto3
import duckdb
from duckdb import HTTPException
from mindsdb_sql_parser import parse_sql

#import pandas as pd
import polars as pd

from typing import Text, Dict, Optional
from botocore.exceptions import ClientError
from botocore.client import Config

from mindsdb_sql_parser.ast.base import ASTNode
from mindsdb_sql_parser.ast import Select, Identifier, Insert, Star, Constant, DropTables, CreateTable

from mindsdb.utilities import log
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response,
    RESPONSE_TYPE
)

import re

#from mindsdb.integrations.libs.api_handler import APIResource, APIHandler

from mindsdb.integrations.libs.base import DatabaseHandler

#from mindsdb.integrations.utilities.sql_utils import FilterCondition, FilterOperator

from sqlalchemy.sql import sqltypes

logger = log.getLogger(__name__)


class S3Handler(DatabaseHandler):
    """
    This handler handles connection and execution of the SQL statements on AWS S3.
    """

    name = 's3'

    def __init__(self, name: Text, connection_data: Optional[Dict], **kwargs):
        """
        Initializes the handler.

        Args:
            name (Text): The name of the handler instance.
            connection_data (Dict): The connection data required to connect to the AWS (S3) account.
            kwargs: Arbitrary keyword arguments.
        """
        super().__init__(name)
        self.connection_data = connection_data
        self.kwargs = kwargs

        self.connection = None
        self.is_connected = False
        self.thread_safe = True
        self.bucket = self.connection_data.get('bucket')
        self._regions = {}

        self.file_format = self.connection_data.get('file_format', 'parquet')
        self.file_compression = self.connection_data.get('file_compression', 'zstd')
        self.file_compression_level = self.connection_data.get('file_compression_level', 8)
        self.parquet_version = self.connection_data.get('parquet_version', 'V2')

        self.resource = None

    def __del__(self):
        if self.is_connected is True:
            self.disconnect()

    def connect(self):
        """
        Establishes a connection to the AWS (S3) account.

        Raises:
            ValueError: If the required connection parameters are not provided.

        Returns:
            boto3.client: A client object to the AWS (S3) account.
        """
        if self.is_connected is True:
            return self.connection

        # Validate mandatory parameters.
        if not all(key in self.connection_data for key in ['aws_access_key_id', 'aws_secret_access_key']):
            raise ValueError('Required parameters (aws_access_key_id, aws_secret_access_key) must be provided.')

        # Connect to S3 and configure mandatory credentials.
        self.connection = self._connect_boto3()
        self.is_connected = True

        return self.connection

    @contextmanager
    def _connect_duckdb(self, bucket):
        """
        Creates temporal duckdb database which is able to connect to the AWS (S3) account.
        Have to be used as context manager

        Returns:
            DuckDBPyConnection
        """
        # Connect to S3 via DuckDB.
        duckdb_conn = duckdb.connect(":memory:")
        try:
            duckdb_conn.execute("INSTALL httpfs;")
        except HTTPException as http_error:
            logger.debug(f"Error installing the httpfs extension, {http_error}! Forcing installation.")
            duckdb_conn.execute("FORCE INSTALL httpfs;")

        duckdb_conn.execute("LOAD httpfs;")

        # detect region for bucket
        if bucket not in self._regions:
            client = self.connect()
            self._regions[bucket] = client.get_bucket_location(Bucket=bucket)['LocationConstraint']

        region = self._regions[bucket]

        endpoint_url = self.connection_data.get('endpoint_url', 's3.amazonaws.com')
        use_ssl = self.connection_data.get('use_ssl', True)

        duckdb_conn.execute(f"""
        CREATE SECRET local_secret_s3 (
        TYPE S3,
        KEY_ID '{self.connection_data['aws_access_key_id']}',
        SECRET '{self.connection_data['aws_secret_access_key']}',
        REGION '{region}',
        ENDPOINT '{endpoint_url}',
        URL_STYLE 'path',
        USE_SSL {use_ssl}
        );
        """)

        try:
            yield duckdb_conn
        finally:
            duckdb_conn.close()

    def _connect_boto3(self) -> boto3.client:
        """
        Establishes a connection to the AWS (S3) account.

        Returns:
            boto3.client: A client object to the AWS (S3) account.
        """
        endpoint_url = self.connection_data.get('endpoint_url', 's3.amazonaws.com')
        if self.connection_data.get('use_ssl', True):
            endpoint_url = f'https://{endpoint_url}'
        else:
            endpoint_url = f'http://{endpoint_url}'

        # Connect to S3 and configure mandatory credentials.

        # Configure mandatory credentials.
        config = {
            'aws_access_key_id': self.connection_data['aws_access_key_id'],
            'aws_secret_access_key': self.connection_data['aws_secret_access_key'],
            "endpoint_url": endpoint_url
        }

        # Configure optional parameters.
        if 'aws_session_token' in self.connection_data:
            config['aws_session_token'] = self.connection_data['aws_session_token']

        client = boto3.client('s3', **config)

        # check connection
        if self.bucket is not None:
            client.head_bucket(Bucket=self.bucket)
        else:
            client.list_buckets()

        self.resource = boto3.resource(
            's3',
            aws_access_key_id=self.connection_data['aws_access_key_id'],
            aws_secret_access_key=self.connection_data['aws_secret_access_key'],
            endpoint_url=endpoint_url,
            region_name=self.connection_data.get('region_name', 'us-east-1'),
            config=Config(signature_version='s3v4')
        )

        return client

    def disconnect(self):
        """
        Closes the connection to the AWS (S3) account if it's currently open.
        """
        if not self.is_connected:
            return
        self.connection.close()
        self.is_connected = False

    def check_connection(self) -> StatusResponse:
        """
        Checks the status of the connection to the S3 bucket.

        Returns:
            StatusResponse: An object containing the success status and an error message if an error occurs.
        """
        response = StatusResponse(False)
        need_to_close = self.is_connected is False

        # Check connection via boto3.
        try:
            self._connect_boto3()
            response.success = True
        except (ClientError, ValueError) as e:
            logger.error(f'Error connecting to S3 with the given credentials, {e}!')
            response.error_message = str(e)

        if response.success and need_to_close:
            self.disconnect()

        elif not response.success and self.is_connected:
            self.is_connected = False

        return response

        
    def read_from_sql(self, sql) -> pd.DataFrame:
        """
        Read object as dataframe. Uses duckdb
        """        
        patron = r'`(.*?)`'
        sql_modificado = re.sub(patron, rf"'s3://{self.bucket}/\1'", sql)

        with self._connect_duckdb(self.bucket) as connection:
            data = connection.execute(sql_modificado).pl()
            return data
        
    def read_from_sql_dataframe(self, sql, files) -> pd.DataFrame:
        """
        Read object as dataframe. Uses duckdb
        """        
        with self._connect_duckdb(self.bucket) as connection:
            data = connection.execute(sql).pl()
            return data


    def add_data_to_table(self, key, query: Insert) -> None: #df) -> None:
        """
        Writes the table to a file in the S3 bucket.

        Raises:
            CatalogException: If the table does not exist in the DuckDB connection.
        """

        # Check if the file exists in the S3 bucket.
        #bucket, key = self._get_bucket(key)

        exists = False
        try:
            client = self.connect()
            client.head_object(Bucket=self.bucket, Key=key)
            exists = True
        except Exception as e:
            exists = False

        df = query.values

        with self._connect_duckdb(self.bucket) as connection:
            # copy
            if exists:
                connection.execute(f"CREATE TABLE tmp_table AS SELECT * FROM 's3://{self.bucket}/{key}'")
                # insert
                connection.execute("INSERT INTO tmp_table BY NAME SELECT * FROM df")
                # upload
                connection.execute(f"COPY tmp_table TO 's3://{self.bucket}/{key}' (FORMAT {self.file_format}, PARQUET_VERSION {self.parquet_version}, OVERWRITE_OR_IGNORE true, COMPRESSION {self.file_compression}, COMPRESSION_LEVEL {self.file_compression_level});")
            else:
                connection.execute(f"COPY df TO 's3://{self.bucket}/{key}' (FORMAT {self.file_format}, PARQUET_VERSION {self.parquet_version}, COMPRESSION {self.file_compression}, COMPRESSION_LEVEL {self.file_compression_level});")

    def _create_table(self, key, df) -> None:
        """
        Create a table in the S3 bucket.
        """
        client = self.connect()
        exists = False
        try:
            client.head_object(Bucket=self.bucket, Key=key)
            exists = True
        except Exception as e:
            with self._connect_duckdb(self.bucket) as connection:
                connection.execute(f"COPY df TO 's3://{self.bucket}/{key}' (FORMAT {self.file_format}, PARQUET_VERSION {self.parquet_version}, COMPRESSION {self.file_compression}, COMPRESSION_LEVEL {self.file_compression_level});")

        if exists:
            logger.error(f'Table {key} already exists in the bucket {self.bucket}')
            raise ValueError(f'Table {key} already exists in the bucket {self.bucket}')


    def _get_s3_objects(self, limit:int = None ) -> list[dict]:
        s3_objects = self.resource.Bucket(self.bucket).objects.all()
        arr_files = []
        rid = 0
        for obj in s3_objects:
            path = obj.key.replace('`', '')        
            item = {
                'path': path,
                'name': path[path.rfind('/') + 1:],
                'extension': path[path.rfind('.') + 1:],
                'bucket': obj.bucket_name,              
                'content': None                             
            }
            if item["extension"] == self.file_format:
                arr_files.append(item)
                rid+=1

            if limit is not None and rid >= limit:
                break

        return arr_files

    def query(self, query: ASTNode) -> Response:
        """
        Executes a SQL query represented by an ASTNode and retrieves the data.

        Args:
            query (ASTNode): An ASTNode representing the SQL query to be executed.

        Raises:
            ValueError: If the file format is not supported or the file does not exist in the S3 bucket.

        Returns:
            Response: A response object containing the result of the query or an error message.
        """        

        self.connect()                

        if isinstance(query, DropTables):
            for table_identifier in query.tables:
                if len(table_identifier.parts) == 2 and table_identifier.parts[0] != self.name:
                    return Response(
                        RESPONSE_TYPE.ERROR,
                        error_message=f"Can't delete table from database '{table_identifier.parts[0]}'",
                    )
                table_name = table_identifier.parts[-1].replace(f"{self.bucket}/", "")
                try:
                    self.connection.delete_object(Bucket=self.bucket, Key=table_name)                    
                except Exception as e:
                    return Response(
                        RESPONSE_TYPE.ERROR,
                        error_message=f"Can't delete table '{table_name}': {e}",
                    )
            response = Response(RESPONSE_TYPE.OK)

        elif isinstance(query, CreateTable):
            table = query.name.parts[-1]

            df = pd.DataFrame([], schema=[col.name for col in query.columns])

            for col in query.columns:     
                dtype = pd.String
                if col.type in (sqltypes.TEXT, sqltypes.VARCHAR,):
                    dtype=pd.String
                elif col.type in (sqltypes.INTEGER,):
                    dtype=pd.Int64
                elif col.type in (sqltypes.FLOAT,):
                    dtype=pd.Float64
                elif col.type in (sqltypes.DATE, sqltypes.Date,):
                    dtype=pd.Date
                elif col.type in (sqltypes.DATETIME, sqltypes.DateTime,):
                    dtype=pd.Datetime
                elif col.type in (sqltypes.BOOLEAN,):
                    dtype=pd.Boolean
                else:
                    logger.error(f'Unsupported data type {col.type} for column {col.name}')
                    raise ValueError(f'Unsupported data type {col.type} for column {col.name}')
                
                df = df.with_columns([
                    pd.col(col.name).cast(dtype).alias(col.name)
                ])
                
            self._create_table(table, df)            
            response = Response(RESPONSE_TYPE.OK)

        elif isinstance(query, Select):
            if isinstance(query.from_table, Identifier) and query.from_table.parts[-1] == "files":
                arr_files = self._get_s3_objects()
                files = pd.DataFrame(data=arr_files, orient="row")
                df = self.read_from_sql_dataframe(query.to_string(), files)
            else:
                df = self.read_from_sql(query.to_string())

            response = Response(
                RESPONSE_TYPE.TABLE,
                data_frame=df
            )
        elif isinstance(query, Insert):     
            table_name = query.table.parts[-1]
            self.add_data_to_table(table_name, query)
            response = Response(RESPONSE_TYPE.OK)
        else:
            raise NotImplementedError

        return response

    def native_query(self, query: str) -> Response:
        """
        Executes a SQL query and returns the result.

        Args:
            query (str): The SQL query to be executed.

        Returns:
            Response: A response object containing the result of the query or an error message.
        """
        query_ast = parse_sql(query)
        return self.query(query_ast)

    def get_tables(self) -> Response:
        """
        Retrieves a list of tables (objects) in the S3 bucket.

        Each object is considered a table. Only the supported file formats are considered as tables.

        Returns:
            Response: A response object containing the list of tables and views, formatted as per the `Response` class.
        """

        supported_names = [
            f"`{obj['path']}`"
            for obj in self._get_s3_objects(1000)
        ]                

        supported_names.insert(0, 'files')

        response = Response(
            RESPONSE_TYPE.TABLE,
            data_frame=pd.DataFrame(
                supported_names,
                schema=['table_name'],
                orient="row"
            )
        )

        return response

    def get_columns(self, table_name: str) -> Response:
        """
        Retrieves column details for a specified table (object) in the S3 bucket.

        Args:
            table_name (Text): The name of the table for which to retrieve column information.

        Raises:
            ValueError: If the 'table_name' is not a valid string.

        Returns:
            Response: A response object containing the column details, formatted as per the `Response` class.
        """

        print("[table_name]", table_name)
        query = Select(
            targets=[Star()],
            from_table=Identifier(parts=[table_name]),
            limit=Constant(1)
        )

        result = self.query(query)

        response = Response(
            RESPONSE_TYPE.TABLE,
            data_frame=pd.DataFrame(
                {
                    'column_name': result.data_frame.columns,
                    'data_type': [data_type if data_type != 'object' else 'string' for data_type in result.data_frame.dtypes]
                }
            )
        )

        return response
