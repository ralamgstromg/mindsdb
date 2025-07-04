#import pandas as pd
import polars as pd
from urllib import parse
import connectorx as cx
#import mysql.connector

#from mindsdb_sql_parser import parse_sql
import re
from mindsdb.utilities.render.sqlalchemy_render import SqlalchemyRender
from mindsdb_sql_parser.ast.base import ASTNode
from mindsdb_sql_parser.ast.select import Identifier, Star, Function, Constant, Select, TypeCast

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

#from mindsdb.api.mysql.mysql_proxy.libs.constants.mysql import MYSQL_DATA_TYPE

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


    def native_query(self, query: str, lower_col_names: bool = True, column_types_pl: dict = None) -> Response:
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
        Retrieve the data from the SQL statement.
        """
        if isinstance(query, Select):
            for tar in query.targets:
                if isinstance(tar, Star):                
                    column_types_pl = {}
                    cols = cx.read_sql(conn=self.uri,
                        query=f"SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '{query.from_table}' AND table_schema = '{self.database}'",
                        protocol="binary"
                    )
                    identifiers_arr = []
                    for _, col in cols.iterrows():
                        r_col = col["column_name"]
                        r_type = col["data_type"]
                        if r_type in ('date',):
                            column_types_pl[r_col] = pd.Date
                            identifiers_arr.append(TypeCast(type_name="date", arg=Function(op="nullif", distinct=False, args=[Identifier(r_col), Constant("0000-00-00")]), alias=Identifier(r_col)))
                            continue
                        elif r_type in ('datetime', 'timestamp',):
                            column_types_pl[r_col] = pd.Datetime
                            identifiers_arr.append(TypeCast(type_name="datetime", arg=Function(op="nullif", distinct=False, alias=Identifier(r_col), args=[Identifier(r_col), Constant("0000-00-00 00:00:00")]), alias=Identifier(r_col)))
                            continue
                        elif r_type in ('time',):
                            column_types_pl[r_col] = pd.Time
                        elif r_type in ('bigint',):
                            column_types_pl[r_col] = pd.Int64
                        elif r_type in ('int',):
                            column_types_pl[r_col] = pd.Int32
                        elif r_type in ('smallint','tinyint','enum'):
                            column_types_pl[r_col] = pd.Int16
                        elif r_type in ('bit',):
                            column_types_pl[r_col] = pd.Boolean
                        elif r_type in ('decimal','double', 'float',):
                            column_types_pl[r_col] = pd.Float64
                        elif r_type in ('varchar','json','longblob','longtext','mediumblob','mediumtext', 'char', 'blob', 'text'):
                            column_types_pl[r_col] = pd.String
                        elif r_type in ('varbinary'):
                            column_types_pl[r_col] = pd.Binary
                        else:
                            logger.info(f"Unknown type: {r_type}, use VARCHAR as fallback.")
                            column_types_pl[r_col] = pd.String
                        
                        identifiers_arr.append(Identifier(r_col))
                    
                    query.targets.remove(tar)
                    query.targets = identifiers_arr + query.targets                

        #print(column_types_pl)

        query_str = self.renderer.get_string(query, with_failback=True)        
        return self.native_query(query_str, column_types_pl=column_types_pl)


    def get_tables(self) -> Response:
        """
        Get a list with all of the tabels in MySQL selected database
        """
        sql = """
            SELECT
                TABLE_SCHEMA AS table_schema,
                TABLE_NAME AS table_name,
                TABLE_TYPE AS table_type
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
