from typing import Any
import datetime


from urllib import parse
import connectorx as cx

import polars as pd

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
        self.connection_data = kwargs.get('connection_data')
        self.dialect = 'mssql'
        self.database = self.connection_data.get('database')
        self.renderer = SqlalchemyRender('mssql')
        self.uncommitted = self.connection_data.get('uncommitted', True)

        self.uri = f"mssql://{self.connection_data.get('user')}:{parse.quote_plus(self.connection_data.get('password'))}@{self.connection_data.get('host')}:{self.connection_data.get('port', 1433)}/{self.connection_data.get('database')}?encrypt=true&trust_server_certificate=true"

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
            cx.read_sql(conn=self.uri, query="SELECT 1 as resp;")
            logger.info(f'Connected to Microsoft SQL Server {self.database}')
            response.success = True
        except Exception as e:
            logger.error(f'Error connecting to Microsoft SQL Server {self.database}, {e}!')
            response.error_message = str(e)
        return response

    def native_query(self, query: str, lower_col_names: bool = True, column_types_pl: dict = None) -> Response:
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

        try:            
            result = pd.read_database_uri(query=sql, uri=self.uri, engine="connectorx", protocol="binary")

            if column_types_pl is None:
                column_types_pl = {
                    col[0]: col[1] for col in result.schema
                }
            
            if lower_col_names:
                result.cast({col: column_types_pl.get(col, pd.String) for col in result.columns})
                result.columns = [col.lower() for col in result.columns]
            else:
                result.cast({col: column_types_pl.get(col, pd.String) for col in result.columns})

            response = Response(RESPONSE_TYPE.TABLE, data_frame=result)
        except Exception as e:
            logger.error(f"Error running query: {query} on {self.connection_data['database']}!")
            response = Response(RESPONSE_TYPE.ERROR, error_message=str(e))
        except pd.exceptions.PanicException as e:
            logger.error(f"Error running query: {query} on {self.connection_data['database']}!")
            response = Response(RESPONSE_TYPE.ERROR, error_message=str(e))
        
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
