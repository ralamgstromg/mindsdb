#import pandas as pd
import polars as pd
from urllib import parse
import connectorx as cx
#import mysql.connector

#from mindsdb_sql_parser import parse_sql
import re
from mindsdb.utilities.render.sqlalchemy_render import SqlalchemyRender
from mindsdb_sql_parser.ast.base import ASTNode
from mindsdb_sql_parser.ast.select import Identifier, Star, Function, Constant

from mindsdb.utilities import log
from mindsdb.integrations.libs.base import DatabaseHandler
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponseNgx as Response,
    RESPONSE_TYPE,
)

import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

#from mindsdb.integrations.handlers.mysql_handler.settings import ConnectionConfig
# from mindsdb.api.mysql.mysql_proxy.libs.constants.mysql import MYSQL_DATA_TYPE
# from mindsdb.api.mysql.mysql_proxy.libs.constants.mysql import C_TYPES, DATA_C_TYPE_MAP

from mindsdb.api.mysql.mysql_proxy.libs.constants.mysql import MYSQL_DATA_TYPE

logger = log.getLogger(__name__)

class MySQLHandler(DatabaseHandler):
    """
    This handler handles connection and execution of the MySQL statements.
    """

    name = "mysql"

    # class Config:
    #     protected_namespaces = ()

    def __init__(self, name, **kwargs):
        super().__init__(name)
        #self.parser = parse_sql
        self.dialect = "mysql"
        self.connection_data = kwargs.get("connection_data", {})
        self.database = self.connection_data.get("database")
        self.renderer = SqlalchemyRender('mysql')

        # self.connection = None
        self.uri = f"mysql://{self.connection_data.get('user')}:{parse.quote_plus(self.connection_data.get('password'))}@{self.connection_data.get('host')}:{self.connection_data.get('port', 3306)}/{self.connection_data.get('database')}"

    def __del__(self):
        pass
        # if self.is_connected:
        #     self.disconnect()

    # def _unpack_config(self):
    #     """
    #     Unpacks the config from the connection_data by validation all parameters.

    #     Returns:
    #         dict: A dictionary containing the validated connection parameters.
    #     """
    #     try:
    #         config = ConnectionConfig(**self.connection_data)
    #         return config.model_dump(exclude_unset=True)
    #     except ValueError as e:
    #         raise ValueError(str(e))

    # @property
    # def is_connected(self):
    #     """
    #     Checks if the handler is connected to the MySQL database.

    #     Returns:
    #         bool: True if the handler is connected, False otherwise.
    #     """
    #     return self.connection is not None and self.connection.is_connected()

    # @is_connected.setter
    # def is_connected(self, value):
    #     pass

    def connect(self):
        """
        Establishes a connection to a MySQL database.

        Returns:
            MySQLConnection: An active connection to the database.
        """
        pass

    def disconnect(self):
        """
        Closes the connection to the MySQL database if it's currently open.
        """
        pass

    def check_connection(self) -> StatusResponse:
        """
        Checks the status of the connection to the MySQL database.

        Returns:
            StatusResponse: An object containing the success status and an error message if an error occurs.
        """
        response = StatusResponse(False)
        try:
            cx.read_sql(conn=self.uri, query="SELECT 1 as resp;")
            logger.info(f'Connected to MySQL {self.database}')
            response.success = True
        except Exception as e:
            logger.error(f'Error connecting to MySQL {self.database}, {e}!')
            response.error_message = str(e)
        return response


    def native_query(self, query: str, lower_col_names: bool = True, using: dict = None) -> Response:
        """
        Executes a SQL query on the MySQL database and returns the result.

        Args:
            query (str): The SQL query to be executed.

        Returns:
            Response: A response object containing the result of the query or an error message.
        """ 
        response = None       
        try:            
            result = pd.read_database_uri(query=query, uri=self.uri, engine="connectorx", protocol="binary")

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
        except Exception as e:
            logger.error(f"Error running query: {query} on {self.connection_data['database']}!")
            response = Response(RESPONSE_TYPE.ERROR, error_message=str(e))
        except pd.exceptions.PanicException as e:
            logger.error(f"Error running query: {query} on {self.connection_data['database']}!")
            response = Response(RESPONSE_TYPE.ERROR, error_message=str(e))
        
        return response 


    def query(self, query: ASTNode) -> Response:
        """
        Retrieve the data from the SQL statement.
        """
        for tar in query.targets:
            if isinstance(tar, Star):                
                cols = cx.get_meta(conn=self.uri,
                    query=f'SELECT * FROM {query.from_table}',
                    protocol="binary"
                )
                identifiers_arr = []
                for col, dtype in cols.dtypes.items():
                    if query.using is not None and "cast_date_nullif" in query.using and query.using["cast_date_nullif"] == True:
                        if dtype in ('date', 'datetime', 'timestamp', 'datetime64[ns]', 'datetime64[ns, tz]'):
                            identifiers_arr.append(Function(op="nullif", distinct=False, alias=Identifier(col), args=[Identifier(col), Constant("0000-00-00 00:00:00")]))
                        else:
                            identifiers_arr.append(Identifier(col))
                    else:
                        identifiers_arr.append(Identifier(col))
                
                query.targets.remove(tar)
                query.targets = identifiers_arr + query.targets                


        query_str = self.renderer.get_string(query, with_failback=True)        
        return self.native_query(query_str, using=query.using)


    def get_tables(self) -> Response:
        """
        Get a list with all of the tabels in MySQL selected database
        """
        sql = """
            SELECT
                TABLE_SCHEMA AS table_schema,
                TABLE_NAME AS table_name,
                CASE WHEN TABLE_TYPE = 'VIEW' AND FLAGS = 'IS_TABLE_VALUED_FUNCTION' THEN 'TABLE FUNCTION' ELSE TABLE_TYPE END AS table_type
            FROM
                information_schema.TABLES
            WHERE
                TABLE_TYPE IN ('BASE TABLE', 'VIEW')
                AND TABLE_SCHEMA = DATABASE()
            ORDER BY 2
            ;
        """
        result = self.native_query(sql, lower_col_names=False)
        return result

    def get_columns(self, table_name) -> Response:
        """
        Show details about the table
        """
        q = f"""
            select
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
            from
                information_schema.columns
            where
                table_name = '{table_name}';
        """
        result = self.native_query(q, lower_col_names=False)
        result.resp_type = RESPONSE_TYPE.COLUMNS_TABLE
        return result
