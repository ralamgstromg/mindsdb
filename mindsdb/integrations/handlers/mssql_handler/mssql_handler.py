#import pandas as pd
import polars as pd
from urllib import parse
import connectorx as cx
from sqlalchemy import create_engine, text
from sqlalchemy.sql import sqltypes

import re
from mindsdb.utilities.render.sqlalchemy_render import SqlalchemyRender
from mindsdb.sql_parser.ast.base import ASTNode
from mindsdb.sql_parser.ast import Select, Identifier, Insert, Star, Constant, DropTables, CreateTable, Delete, TypeCast, Function, Call


from mindsdb.utilities import log
from mindsdb.integrations.libs.base import DatabaseHandler
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response,
    RESPONSE_TYPE,
)

from joblib import Parallel, delayed

import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

logger = log.getLogger(__name__)

class SqlServerHandler(DatabaseHandler):
    """
    This handler handles connection and execution of the SQL Server statements.
    """

    name = "mssql"

    def __init__(self, name, **kwargs):
        super().__init__(name)

        self.dialect = "mssql"
        self.connection_data = kwargs.get("connection_data", {})
        self.database = self.connection_data.get("database")
        self.renderer = SqlalchemyRender('mssql')
        self.uncommitted = self.connection_data.get('uncommitted', True)

        self.uri = f"mssql://{self.connection_data.get('user')}:{parse.quote_plus(self.connection_data.get('password'))}@{self.connection_data.get('host')}:{self.connection_data.get('port', 1433)}/{self.connection_data.get('database')}?encrypt=true&trust_server_certificate=true"
        self.sqlalchemy_uri = f"mssql+pymssql://{self.connection_data.get('user')}:{parse.quote_plus(self.connection_data.get('password'))}@{self.connection_data.get('host')}:{self.connection_data.get('port', 1433)}/{self.connection_data.get('database')}"

    def __del__(self):
        pass

    def connect(self):
        """
        Establishes a connection to a SqlServer database.

        Returns:
            SqlServerConnection: An active connection to the database.
        """
        pass

    def disconnect(self):
        """
        Closes the connection to the SqlServer database if it's currently open.
        """
        pass

    def check_connection(self) -> StatusResponse:
        """
        Checks the status of the connection to the SqlServer database.

        Returns:
            StatusResponse: An object containing the success status and an error message if an error occurs.
        """
        response = StatusResponse(False)
        try:
            cx.read_sql(conn=self.uri, query="SELECT 1 as resp;")
            logger.info(f'Connected to SqlServer {self.database}')
            response.success = True
        except Exception as e:
            logger.error(f'Error connecting to SqlServer {self.database}, {e}!')
            response.error_message = str(e)
        return response


    def native_query(self, query: str, lower_col_names: bool = True, column_types_pl: dict = None) -> Response:
        """
        Executes a SQL query on the SqlServer database and returns the result.

        Args:
            query (str): The SQL query to be executed.

        Returns:
            Response: A response object containing the result of the query or an error message.
        """ 
        sql = ""
        if self.uncommitted:
            sql = "SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED; SET NOCOUNT ON; "
        sql = f"{sql}{query}"

        response = None
        try:
            result = pd.read_database_uri(query=sql, uri=self.uri, engine="connectorx", protocol="binary")            

            if column_types_pl is None:
                column_types_pl = {
                    col[0]: col[1] for col in result.schema
                }
            
            if lower_col_names:
                result.columns = [col.lower() for col in result.columns]

            if column_types_pl:
                result = result.with_columns([
                    pd.col(col).cast(column_types_pl.get(col, pd.String)) for col in result.columns if col in column_types_pl
                ])

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
            column_types_pl = {}    
            targets = []
            for tar in query.targets:
                if isinstance(tar, Star):  
                    table = f'{query.from_table}'
                    schema = "dbo"                    
                    table_arr = table.split(".")
                    if len(table_arr) == 2:
                        table = f'{table_arr[1]}'
                        schema = table_arr[0]
                    cols = cx.read_sql(conn=self.uri,
                        query=f"SELECT column_name, data_type FROM {self.database}.information_schema.columns WHERE table_name = '{table}' AND table_schema = '{schema}' ORDER BY ordinal_position;",
                        protocol="binary"
                    )
                    identifiers_arr = []
                    for _, col in cols.iterrows():
                        r_col = str(col["column_name"]).replace('`', '')
                        r_col = col["column_name"]
                        r_type = col["data_type"]
                        if r_type in ('date',):
                            column_types_pl[r_col] = pd.Date
                            # identifiers_arr.append(TypeCast(type_name="date", arg=Function(op="nullif", distinct=False, args=[Identifier(r_col), Constant("0000-00-00")]), alias=Identifier(r_col)))
                            # continue
                        elif r_type in ('datetime', 'timestamp', 'datetime2', 'datetimeoffset', 'smalldatetime', 'timestamp without time zone'):
                            column_types_pl[r_col] = pd.Datetime
                            # identifiers_arr.append(TypeCast(type_name="datetime", arg=Function(op="nullif", distinct=False, alias=Identifier(r_col), args=[Identifier(r_col), Constant("0000-00-00 00:00:00")]), alias=Identifier(r_col)))
                            # continue
                        elif r_type in ('time',):
                            column_types_pl[r_col] = pd.Time
                        elif r_type in ('bigint',):
                            column_types_pl[r_col] = pd.Int64
                        elif r_type in ('int', 'integer',):
                            column_types_pl[r_col] = pd.Int32
                        elif r_type in ('smallint','tinyint','enum',):
                            column_types_pl[r_col] = pd.Int16
                        elif r_type in ('bit',):
                            column_types_pl[r_col] = pd.Boolean
                        elif r_type in ('decimal','double', 'float', 'money', 'numeric', 'real', 'double precision', 'smallmoney'):
                            column_types_pl[r_col] = pd.Float64
                        elif r_type in ('varchar','json','longblob','longtext','mediumblob','mediumtext', 'char', 'blob', 'text', 'nchar', 'nvarchar', 'ntext', 'sql_variant', 'uniqueidentifier', 'set', 'character', 'character varying'):
                            column_types_pl[r_col] = pd.String
                        elif r_type in ('varbinary',):
                            column_types_pl[r_col] = pd.Binary
                        elif r_type in ('image',):
                            raise Exception(f"Column type not supported: {r_type}, column: {r_col}")
                        else:
                            logger.info(f"Unknown type: {r_type}, use VARCHAR as fallback.")
                            column_types_pl[r_col] = pd.String
                        
                        identifiers_arr.append(Identifier(r_col))
                    
                    targets += identifiers_arr
                else:
                    targets += [tar]

            query.targets = targets
            query_str = self.renderer.get_string(query, with_failback=True).replace('`', '')            
            return self.native_query(query_str, column_types_pl=column_types_pl)
    
        elif isinstance(query, Insert):
            return self._mssql_table_insert(query.table, query.values, query.using)
        elif isinstance(query, Delete):
            return self._mssql_table_delete(query)
        elif isinstance(query, CreateTable):
            return self._mssql_table_create(query)
        elif isinstance(query, DropTables):
            return self._mssql_exec_ddl(query)
        elif isinstance(query, Call):
            return self._mssql_call_procedure(query)
        else:
            logger.info(f"Operation not supported in SQL Server {type(query)}")
            return Response(RESPONSE_TYPE.TABLE, data_frame=pd.DataFrame())
        
    def _mssql_call_procedure(self, sql):
        engine = create_engine(self.sqlalchemy_uri)
        with engine.connect() as conn:
            try:
                procedure_name = ".".join(sql.name.parts[1:])
                params = sql.query_str
                res = conn.execute(text(f"EXEC {procedure_name} {params};"))
                conn.commit()
                return Response(RESPONSE_TYPE.OK, affected_rows=res.rowcount)
            except Exception as ex:
                conn.rollback()
                logger.error(f"Error executing procedure {procedure_name}({params}), {ex}")                
                #return Response(RESPONSE_TYPE.ERROR, error_message=f"Error executing procedure {procedure_name}({params}), {ex}")
                raise Exception(f"Error executing procedure {procedure_name}({params}), {ex}")
                

    def _mssql_table_delete(self, sql):
        engine = create_engine(self.sqlalchemy_uri)
        with engine.connect() as conn:
            try:
                res = conn.execute(text(f"{sql}"))
                conn.commit()
                return Response(RESPONSE_TYPE.OK, affected_rows=res.rowcount)
            except Exception as ex:
                logger.error(f"Error deleting data from table {sql}, {ex}")
                conn.rollback()
                return Response(RESPONSE_TYPE.ERROR, error_code=10, error_message=f"Error deleting data from table {sql}, {ex}")
            
    def _mssql_table_create(self, query):
        engine = create_engine(self.sqlalchemy_uri)
        with engine.connect() as conn:
            try:
                columns = []
                for col in query.columns:                    
                    if col.type == sqltypes.TEXT:
                        columns.append(f"{col.name} TEXT")
                    elif col.type == sqltypes.INTEGER:
                        columns.append(f"{col.name} INTEGER")
                    elif col.type == sqltypes.FLOAT:
                        columns.append(f"{col.name} FLOAT")
                    elif col.type == sqltypes.Date:
                        columns.append(f"{col.name} DATE")
                    elif col.type == sqltypes.DateTime:
                        columns.append(f"{col.name} DATETIME")
                    else:
                        logger.info(f"Type not supported: {col.name}, {col.type}")
                        columns.append(f"{col.name} TEXT")

                sql = f"CREATE TABLE {query.name} ({', '.join(columns)})"

                res = conn.execute(text(f"{sql}"))
                conn.commit()
                return Response(RESPONSE_TYPE.OK, affected_rows=res.rowcount)
            except Exception as ex:
                logger.error(f"Error executing DDL {sql}, {ex}")
                conn.rollback()
                return Response(RESPONSE_TYPE.ERROR, error_code=10, error_message=f"Error executing DDL {sql}, {ex}")

            

    def _mssql_exec_ddl(self, sql):
        engine = create_engine(self.sqlalchemy_uri)
        with engine.connect() as conn:
            try:
                res = conn.execute(text(f"{sql}"))
                conn.commit()
                return Response(RESPONSE_TYPE.OK, affected_rows=res.rowcount)
            except Exception as ex:
                logger.error(f"Error executing DDL {sql}, {ex}")
                conn.rollback()
                return Response(RESPONSE_TYPE.ERROR, error_code=10, error_message=f"Error executing DDL {sql}, {ex}")


    def _mssql_table_insert(self, table_name: str, df: pd.DataFrame, using: dict = {}):
        try:
            Parallel(n_jobs=int(using.get("n_jobs", 4)))(delayed(chunk_df.write_database)(
                table_name=f"{table_name}",
                connection=self.sqlalchemy_uri,
                if_table_exists='append'
            ) for _, chunk_df in enumerate(df.iter_slices(n_rows=int(using.get("batch_size", 5000)))))
            return Response(RESPONSE_TYPE.OK, affected_rows=df.shape[0])
        except Exception as ex:
            logger.error(f"Error inserting data to table {table_name}, {ex}")
            return Response(RESPONSE_TYPE.ERROR, error_code=10, error_message=f"Error executing INSERT to {table_name}, {ex}")


    def get_tables(self) -> Response:
        """
        Get a list with all of the tabels in SQL Server selected database
        """
        query = f"""
            SELECT
                table_schema,
                table_name,
                table_type
            FROM {self.database}.INFORMATION_SCHEMA.TABLES
            WHERE TABLE_TYPE in ('BASE TABLE', 'VIEW')
            ORDER BY
                table_schema,
                table_name
        """
        resp = self.native_query(query, lower_col_names=False)
        return resp

    def get_columns(self, table_name) -> Response:
        """
        Show details about the table
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
