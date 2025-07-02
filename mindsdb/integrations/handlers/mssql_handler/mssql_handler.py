from typing import Any
import datetime


from urllib import parse
import connectorx as cx
#import pymssql
#from pymssql import OperationalError
# import pandas as pd
# from pandas.api import types as pd_types
#from sqlalchemy.ext.asyncio import create_async_engine
import polars as pd
# from polars import pandas as pd_types


#from mindsdb_sql_parser import parse_sql
from mindsdb_sql_parser.ast.base import ASTNode

from mindsdb.integrations.libs.base import DatabaseHandler
from mindsdb.utilities import log
from mindsdb.utilities.render.sqlalchemy_render import SqlalchemyRender
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response,
    RESPONSE_TYPE
)
from mindsdb.api.mysql.mysql_proxy.libs.constants.mysql import MYSQL_DATA_TYPE


logger = log.getLogger(__name__)


class SqlServerNgxHandler(DatabaseHandler):
    """
    This handler handles connection and execution of the Microsoft SQL Server statements.
    """
    name = 'mssql'

    def __init__(self, name, **kwargs):
        super().__init__(name)
        #self.parser = parse_sql
        self.connection_args = kwargs.get('connection_data')
        self.dialect = 'mssql'
        self.database = self.connection_args.get('database')
        self.renderer = SqlalchemyRender('mssql')
        self.uncommitted = self.connection_args.get('uncommitted', True)

        self.uri = f"mssql://{self.connection_args.get('user')}:{parse.quote_plus(self.connection_args.get('password'))}@{self.connection_args.get('host')}:{self.connection_args.get('port', 1433)}/{self.connection_args.get('database')}?encrypt=true&trust_server_certificate=true"

    def __del__(self):
        pass

    def connect(self):
        """
        Establishes a connection to a Microsoft SQL Server database.

        Raises:
            pymssql._mssql.OperationalError: If an error occurs while connecting to the Microsoft SQL Server database.

        Returns:
            pymssql.Connection: A connection object to the Microsoft SQL Server database.
        """      
        pass

    def disconnect(self):
        """
        Closes the connection to the Microsoft SQL Server database if it's currently open.
        """
        pass


    def check_connection(self) -> StatusResponse:
        """
        Checks the status of the connection to the Microsoft SQL Server database.

        Returns:
            StatusResponse: An object containing the success status and an error message if an error occurs.
        """
        response = StatusResponse(False)
        try:
            #print(self.uri)
            cx.read_sql(conn=self.uri, query="SELECT 1 as resp;")
            logger.info(f'Connected to Microsoft SQL Server {self.database}')
            response.success = True
        except Exception as e:
            logger.error(f'Error connecting to Microsoft SQL Server {self.database}, {e}!')
            response.error_message = str(e)
        return response

    def native_query(self, query: str, lower_col_names: bool = True) -> Response:
        """
        Executes a SQL query on the Microsoft SQL Server database and returns the result.

        Args:
            query (str): The SQL query to be executed.

        Returns:
            Response: A response object containing the result of the query or an error message.
        """
        sql = ""
        if self.uncommitted:
            sql = "SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED; SET NOCOUNT ON; "
        sql = f"{sql}{query}"

        result = pd.read_database_uri(query=sql, uri=self.uri, engine="connectorx")

        mysql_types: list[MYSQL_DATA_TYPE] = []
        for dtype in result.dtypes:
            match dtype:
                case pd.String | pd.Object: # Varchar
                    mysql_types.append(MYSQL_DATA_TYPE.VARCHAR)
                case pd.Date: # Date
                    mysql_types.append(MYSQL_DATA_TYPE.DATE)
                case pd.Int8 | pd.Int16 | pd.Int32 | pd.Int64 | pd.UInt8 | pd.UInt16 | pd.UInt32 | pd.UInt64: # Integer
                    mysql_types.append(MYSQL_DATA_TYPE.BIGINT)
                case pd.Datetime: # Datetime
                    mysql_types.append(MYSQL_DATA_TYPE.DATETIME)
                case pd.Float32 | pd.Float64: # Decimal
                    mysql_types.append(MYSQL_DATA_TYPE.FLOAT)
                case pd.Binary: # Binary
                    mysql_types.append(MYSQL_DATA_TYPE.BINARY)
                case pd.Boolean: # Binary
                    mysql_types.append(MYSQL_DATA_TYPE.BIT)
                case _:
                    logger.info(f"Unknown type: {dtype}, use VARCHAR as fallback.")
                    mysql_types.append(MYSQL_DATA_TYPE.VARCHAR)
        
        # por compatibilidad con diferentes motores manejamos los nombres de las columnas en minuscula
        if lower_col_names:
            result.columns = [col.lower() for col in result.columns]
        response = Response(RESPONSE_TYPE.TABLE, data_frame=result, mysql_types=mysql_types) 

        return response

    def query(self, query: ASTNode) -> Response:
        """
        Executes a SQL query represented by an ASTNode and retrieves the data.

        Args:
            query (ASTNode): An ASTNode representing the SQL query to be executed.

        Returns:
            Response: The response from the `native_query` method, containing the result of the SQL query execution.
        """
        query_str = self.renderer.get_string(query, with_failback=True)        
        logger.debug(f"Executing SQL query: {query_str}")
        return self.native_query(query_str)

    def get_tables(self) -> Response:
        """
        Retrieves a list of all non-system tables and views in the current schema of the Microsoft SQL Server database.

        Returns:
            Response: A response object containing the list of tables and views, formatted as per the `Response` class.
        """

        query = f"""
            SELECT
                table_schema,
                table_name,
                table_type
            FROM {self.database}.INFORMATION_SCHEMA.TABLES
            WHERE TABLE_TYPE in ('BASE TABLE', 'VIEW');
        """
        resp = self.native_query(query, lower_col_names=False)
        print(resp)
        return resp

    def get_columns(self, table_name) -> Response:
        """
        Retrieves column details for a specified table in the Microsoft SQL Server database.

        Args:
            table_name (str): The name of the table for which to retrieve column information.

        Returns:
            Response: A response object containing the column details, formatted as per the `Response` class.
        Raises:
            ValueError: If the 'table_name' is not a valid string.
        """

        query = f"""
            SELECT
                COLUMN_NAME,
                DATA_TYPE,
                ORDINAL_POSITION,
                COLUMN_DEFAULT,
                IS_NULLABLE,
                CHARACTER_MAXIMUM_LENGTH,
                CHARACTER_OCTET_LENGTH,
                NUMERIC_PRECISION,
                NUMERIC_SCALE,
                DATETIME_PRECISION,
                CHARACTER_SET_NAME,
                COLLATION_NAME
            FROM
                information_schema.columns
            WHERE
                table_name = '{table_name}'
        """
        result = self.native_query(query, lower_col_names=False)
        result.resp_type = RESPONSE_TYPE.COLUMNS_TABLE
        return result
