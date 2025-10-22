from typing import List
from contextlib import contextmanager

import duckdb
from duckdb import HTTPException
from mindsdb.sql_parser import parse_sql

#import pandas as pd
import polars as pd
import paramiko

from typing import Text, Dict, Optional
from botocore.exceptions import ClientError
from botocore.client import Config

from mindsdb.sql_parser.ast.base import ASTNode
from mindsdb.sql_parser.ast import Select, Identifier, Insert, Star, Constant, DropTables, CreateTable, Function

from mindsdb.utilities import log
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response,
    RESPONSE_TYPE
)

import re

from mindsdb.integrations.libs.base import DatabaseHandler

from sqlalchemy.sql import sqltypes

logger = log.getLogger(__name__)


class SFTPHandler(DatabaseHandler):
    """
    This handler handles connection and execution of the SQL statements on AWS S3.
    """

    name = 'sftp'

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

        self.ssh_client = None
        self.is_connected = False

    @contextmanager
    def _connect_duckdb(self):
        """
        Creates temporal duckdb database which is able to connect to the AWS (S3) account.
        Have to be used as context manager

        Returns:
            DuckDBPyConnection
        """
        # Connect to S3 via DuckDB.
        duckdb_conn = duckdb.connect(":memory:")
        try:
            yield duckdb_conn
        finally:
            duckdb_conn.close()


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
            return self.ssh_client

        # Validate mandatory parameters.
        if not all(key in self.connection_data for key in ['server', 'username', 'password']):
            raise ValueError('Required parameters (server, username, password) must be provided.')

        # Connect to S3 and configure mandatory credentials.
        self.ssh_client = self._connect_sftp()
        self.is_connected = True

        return self.ssh_client


    def _connect_sftp(self) -> paramiko.SSHClient:
        """
        Establishes a connection to the AWS (S3) account.

        Returns:
            boto3.client: A client object to the AWS (S3) account.
        """
        ssh_client = paramiko.SSHClient()
        ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh_client.connect(
            self.connection_data.get('server', 'localhost'), 
            self.connection_data.get('port', 22), 
            self.connection_data.get('username', 'user'), 
            self.connection_data.get('password', 'password')
        )
        return ssh_client


    def disconnect(self):
        """
        Closes the connection to the AWS (S3) account if it's currently open.
        """
        if not self.is_connected:
            return

        self.ssh_client.close()
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
            self._connect_sftp()
            response.success = True
        except (ClientError, ValueError) as e:
            logger.error(f'Error connecting to S3 with the given credentials, {e}!')
            response.error_message = str(e)

        if response.success and need_to_close:
            self.disconnect()

        elif not response.success and self.is_connected:
            self.is_connected = False

        return response

        
    def read_from_sql(self, sql, using: dict) -> pd.DataFrame:
        """
        Read object as dataframe. Uses duckdb
        """        
        sql_modificado = str(sql).replace('`id`', 'id')
        sql_modificado = sql_modificado.replace('`', '"')
        sql_modificado = sql_modificado.split('USING')[0].strip()
        with self._connect_duckdb() as connection:
            data = connection.execute(sql_modificado).pl()
            return data
        
    def read_from_sql_dataframe(self, sql, file, using: dict) -> pd.DataFrame:
        """
        Read object as dataframe. Uses duckdb
        """        
        sql_modificado = f"{sql.to_string()} FROM df"
        sql_modificado = sql_modificado.split('USING')[0].strip()
        with self._connect_duckdb() as connection:
            data = connection.execute(sql_modificado).pl()
            return data
        
    # def _parse_using(self, using: dict) -> dict:
    #     config_duckdb = {}
    #     if using is not None:
    #         config_duckdb["FORMAT"] = str(using.get("format", "parquet"))
            
    #         if config_duckdb["FORMAT"] == "parquet":
    #             config_duckdb["PARQUET_VERSION"] = str(using.get("parquet_version", "V2"))
    #             config_duckdb["COMPRESSION"] = str(using.get("compression", "gzip"))
    #             config_duckdb["ROW_GROUP_SIZE"] = int(using.get("row_group_size", 245760))

    #             if config_duckdb["COMPRESSION"] == "zstd" and "compression_level" in using:
    #                 config_duckdb["COMPRESSION_LEVEL"] = int(using.get("compression_level", 18))

    #         elif config_duckdb["FORMAT"] == "csv":

    #             if "compression" in using:
    #                 config_duckdb["COMPRESSION"] = f'{using["compression"]}'

    #             if "dateformat" in using:
    #                 config_duckdb["DATEFORMAT"] = f'{using["dateformat"]}'

    #             if "delim" in using:
    #                 config_duckdb["DELIM"] = f'{using["delim"]}'

    #             if "escape" in using:
    #                 config_duckdb["ESCAPE"] = f'{using["escape"]}'

    #             if "header" in using:
    #                 config_duckdb["HEADER"] = f'{using["header"]}'

    #             if "nullstr" in using:
    #                 config_duckdb["NULLSTR"] = f'{using["nullstr"]}'

    #             if "quote" in using:
    #                 config_duckdb["QUOTE"] = f'{using["quote"]}'

    #             if "timestampformat" in using:
    #                 config_duckdb["TIMESTAMPFORMAT"] = f'{using["timestampformat"]}'

    #         if "partition_by" in using:
    #             config_duckdb["PARTITION_BY"] = using["partition_by"]

    #         config_duckdb["OVERWRITE_OR_IGNORE"] = bool(using.get("overwrite_or_ignore", False))            
                
    #     return config_duckdb

    def _config_to_sql(self, config_sql: dict):
        config_arr = []
        for key, val in config_sql.items():
            if isinstance(val, list):
                val = '(' + ', '.join([f"{v}" for v in val]) + ')'
            config_arr.append(f"{key} {val}")
        if len(config_arr)>0:
            return "(" + ", ".join(config_arr) + ")"
        else:
            return ""


    def add_data_to_table(self, key: str, query: Insert, using: dict = {}) -> None: #df) -> None:
        """
        Writes the table to a file in the S3 bucket.

        Raises:
            CatalogException: If the table does not exist in the DuckDB connection.
        """

        # Check if the file exists in the S3 bucket.
        #bucket, key = self._get_bucket(key)

        # exists = False
        # try:
        #     client = self.connect()
        #     client.head_object(Bucket=self.bucket, Key=key)
        #     exists = True
        # except Exception as e:
        #     exists = False

        # df = query.values

        # config_duckdb = self._parse_using(query.using)
        # #print("[add_data_to_table]", config_duckdb)
        # config_str = self._config_to_sql(config_duckdb)

        #print(config_str)

        # with self._connect_duckdb(self.bucket) as connection:
        #     # copy
        #     if exists:
        #         connection.execute(f"CREATE TABLE tmp_table AS SELECT * FROM 's3://{self.bucket}/{key}'")
        #         # insert
        #         connection.execute("INSERT INTO tmp_table BY NAME SELECT * FROM df")
        #         # upload
        #         connection.execute(f"COPY tmp_table TO 's3://{self.bucket}/{key}' {config_str};")
        #     else:
        #         connection.execute(f"COPY df TO 's3://{self.bucket}/{key}' {config_str};")

        pass


        


    def _create_table(self, query, df) -> None:
        """
        Create a table in the S3 bucket.
        """
        # table = query.name.parts[-1]

        # client = self.connect()
        # exists = False
        # try:
        #     client.head_object(Bucket=self.bucket, Key=table)
        #     exists = True
        # except Exception as e:
        #     config_duckdb = self._parse_using(query.using)
        #     config_str = self._config_to_sql(config_duckdb)
        #     with self._connect_duckdb(self.bucket) as connection:
        #         connection.execute(f"COPY df TO 's3://{self.bucket}/{table}' {config_str};")

        # if exists:
        #     logger.error(f'Table {table} already exists in the bucket {self.bucket}')
        #     raise ValueError(f'Table {table} already exists in the bucket {self.bucket}')
        pass


    def _get_s3_objects(self, limit:int = None ) -> list[dict]:
        # s3_objects = self.resource.Bucket(self.bucket).objects.all()
        # arr_files = []
        # rid = 0
        # for obj in s3_objects:
        #     path = obj.key.replace('`', '')        
        #     item = {
        #         'path': path,
        #         'name': path[path.rfind('/') + 1:],
        #         'extension': path[path.rfind('.') + 1:],
        #         'bucket': obj.bucket_name,              
        #         'content': None                             
        #     }
        #     if item["extension"] in self.supported_files:
        #         arr_files.append(item)
        #         rid+=1

        #     if limit is not None and rid >= limit:
        #         break

        # return arr_files
        pass

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
            #print("[DROP TABLE]")
            # for table_identifier in query.tables:
            #     if len(table_identifier.parts) == 2 and table_identifier.parts[0] != self.name:
            #         return Response(
            #             RESPONSE_TYPE.ERROR,
            #             error_message=f"Can't delete table from database '{table_identifier.parts[0]}'",
            #         )
            #     table_name = table_identifier.parts[-1].replace(f"{self.bucket}/", "")
            #     try:
            #         self.connection.delete_object(Bucket=self.bucket, Key=table_name)                    
            #     except Exception as e:
            #         return Response(
            #             RESPONSE_TYPE.ERROR,
            #             error_message=f"Can't delete table '{table_name}': {e}",
            #         )
            response = Response(RESPONSE_TYPE.OK)

        elif isinstance(query, CreateTable):
            # #table = query.name.parts[-1]
            # #print("[S3_CREATE_TABLE]", query.using)
            # # print(query)
            # df = pd.DataFrame([], schema=[col.name for col in query.columns])

            # for col in query.columns:     
            #     dtype = pd.String
            #     if col.type in (sqltypes.TEXT, sqltypes.VARCHAR,):
            #         dtype=pd.String
            #     elif col.type in (sqltypes.INTEGER,):
            #         dtype=pd.Int64
            #     elif col.type in (sqltypes.FLOAT,):
            #         dtype=pd.Float64
            #     elif col.type in (sqltypes.DATE, sqltypes.Date,):
            #         dtype=pd.Date
            #     elif col.type in (sqltypes.DATETIME, sqltypes.DateTime,):
            #         dtype=pd.Datetime
            #     elif col.type in (sqltypes.BOOLEAN,):
            #         dtype=pd.UInt8
            #     else:
            #         logger.error(f'Unsupported data type {col.type} for column {col.name}')
            #         raise ValueError(f'Unsupported data type {col.type} for column {col.name}')
                
            #     df = df.with_columns([
            #         pd.col(col.name).cast(dtype).alias(col.name)
            #     ])
                
            # #self._create_table(table, df)            
            # self._create_table(query, df)
            response = Response(RESPONSE_TYPE.OK) #Response(RESPONSE_TYPE.OK, affected_rows=df.shape[0])

        elif isinstance(query, Select):
            #print(type(query.from_table), query.from_table)
            # if isinstance(query.from_table, Identifier) and query.from_table.parts[-1] == "files":
            #     arr_files = self._get_s3_objects()
            #     files = pd.DataFrame(data=arr_files, orient="row")
            #     df = self.read_from_sql_dataframe(query.to_string(), files, query.using)
            # else:
            #     #print(query)
            #     query.from_table = Identifier(parts=[f"s3://{self.bucket}/{str(query.from_table).replace('`', '')}"])
            #     df = self.read_from_sql(query.to_string(), query.using)            
            sftp_client = self.ssh_client.open_sftp()            
            
            remote_path = "/".join(str(query.from_table).replace("`", "").split("/")[:-1])
            file = str(query.from_table).replace("`", "").split("/")[-1]

            print(remote_path, file)

            files = sftp_client.listdir(remote_path)
            #compiled_regex = re.compile(file)            
            compiled_regex = re.compile(file.replace('*', '.*').replace('?', '.') + '$')

            #print(files)

            matching_files = []
            matching_files = [f for f in files if compiled_regex.match(f)]
            
            #print(matching_files)
            matching_local_files = []
            query = ""
            for fl in matching_files:
                local_file = f'/tmp/{fl}'
                sftp_client.get(f"{remote_path}/{fl}", local_file)
                matching_local_files.append(local_file)

            arr_files = "'" + "','".join(matching_local_files) + "'"
            query = f"SELECT * FROM read_csv([{arr_files}])"
            print(query)

            #print("matching_local_files", )

            
            #print(str(query.from_table).replace("`", ""))



            #print(sftp_client.listdir(str(query.from_table).replace("`", ""))) 
            #local_file = f'/tmp/{str(query.from_table).split('/')[-1].replace("`", "").replace(".txt", ".csv")}'            
            
            #query.from_table = Identifier(parts=[local_file])
            #print(query.to_tree())
            #query.from_table = Function(alias="read_csv", from_arg=[f'/tmp/{str(query.from_table).split('/')[-1].replace("`", "")}'])
            print(query)
            #query.from_fun()
            #print("table", query.from_table)
            #print(query.to_string())
            df = self.read_from_sql(query, {})            

            response = Response(
                RESPONSE_TYPE.TABLE,
                data_frame=df,
                affected_rows=df.shape[0]
            )
        elif isinstance(query, Insert):     
            # table_name = query.table.parts[-1]
            # self.add_data_to_table(table_name, query)
            #response = Response(RESPONSE_TYPE.OK, affected_rows=query.values.shape[0])
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
        # supported_names = [
        #     f"`{obj['path']}`"
        #     for obj in self._get_s3_objects(10000)
        # ]                

        # supported_names.insert(0, 'files')

        # response = Response(
        #     RESPONSE_TYPE.TABLE,
        #     data_frame=pd.DataFrame(
        #         supported_names,
        #         schema=['table_name'],
        #         orient="row"
        #     )
        # )

        # return response
        pass

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

        # #print("[table_name]", table_name)
        # query = Select(
        #     targets=[Star()],
        #     from_table=Identifier(parts=[table_name]),
        #     limit=Constant(1)
        # )

        # result = self.query(query)

        # #print("[get_columns]", result

        # response = Response(
        #     RESPONSE_TYPE.TABLE,
        #     data_frame=pd.DataFrame(
        #         {
        #             'column_name': result.data_frame.columns,
        #             'data_type': [data_type if data_type != 'object' else 'string' for data_type in result.data_frame.dtypes]
        #         }
        #     )
        # )

        # return response
        pass
