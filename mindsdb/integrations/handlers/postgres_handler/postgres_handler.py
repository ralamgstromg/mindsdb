from typing import Optional
import polars as pd
from urllib import parse
import connectorx as cx
#import mysql.connector
from sqlalchemy import create_engine, text
from sqlalchemy.sql import sqltypes

#from mindsdb.sql_parser import parse_sql
import re
from mindsdb.utilities.render.sqlalchemy_render import SqlalchemyRender
from mindsdb.sql_parser.ast.base import ASTNode
from mindsdb.sql_parser.ast import Select, Identifier, Insert, Star, Constant, DropTables, CreateTable, Delete, TypeCast, Function, Call
#from mindsdb.sql_parser.ast.select import Star

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

class PostgresHandler(DatabaseHandler):
    """
    This handler handles connection and execution of the PostgreSQL statements.
    """

    name = "postgres"

    def __init__(self, name, **kwargs):
        super().__init__(name)

        self.dialect = "postgres"
        self.connection_data = kwargs.get("connection_data", {})
        self.database = self.connection_data.get("database")
        self.renderer = SqlalchemyRender('postgres')

        self.uri = f"postgres://{self.connection_data.get('user')}:{parse.quote_plus(self.connection_data.get('password'))}@{self.connection_data.get('host')}:{self.connection_data.get('port', 5432)}/{self.connection_data.get('database')}"
        self.sqlalchemy_uri = f"postgresql+psycopg2://{self.connection_data.get('user')}:{parse.quote_plus(self.connection_data.get('password'))}@{self.connection_data.get('host')}:{self.connection_data.get('port', 5432)}/{self.connection_data.get('database')}"

    def __del__(self):
        pass

    def connect(self):
        """
        Establishes a connection to a PostgreSQL database.

        Returns:
            PostgreSQLConnection: An active connection to the database.
        """
        pass

    def disconnect(self):
        """
        Closes the connection to the PostgreSQL database if it's currently open.
        """
        pass

    def check_connection(self) -> StatusResponse:
        """
        Checks the status of the connection to the PostgreSQL database.

        Returns:
            StatusResponse: An object containing the success status and an error message if an error occurs.
        """
        response = StatusResponse(False)
        try:
            cx.read_sql(conn=self.uri, query="SELECT 1 as resp;")
            logger.info(f'Connected to PostgreSQL {self.database}')
            response.success = True
        except Exception as e:
            logger.error(f'Error connecting to PostgreSQL {self.database}, {e}!')
            response.error_message = str(e)
        return response


    def native_query(self, query: str, lower_col_names: bool = True, column_types_pl: dict = None) -> Response:
        """
        Executes a SQL query on the PostgreSQL database and returns the result.

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
                        query=f"SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '{table}' AND table_schema = '{schema}' ORDER BY ordinal_position;",
                        protocol="binary"
                    )
                    identifiers_arr = []
                    for _, col in cols.iterrows():
                        r_col = str(col["column_name"]).replace('`', '')
                        r_type = col["data_type"]
                        if r_type in ('date',):
                            column_types_pl[r_col] = pd.Date
                        elif r_type in ('datetime', 'timestamp', 'datetime2', 'datetimeoffset', 'smalldatetime', 'timestamp without time zone'):
                            column_types_pl[r_col] = pd.Datetime
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
            query_str = self.renderer.get_string(query, with_failback=True)
            return self.native_query(query_str, column_types_pl=column_types_pl)
    
        elif isinstance(query, Insert):
            return self._postgres_table_insert(query.table, query.values, query.using)
        elif isinstance(query, Delete):
            return self._postgres_table_delete(query)
        elif isinstance(query, CreateTable):
            return self._postgres_table_create(query)
        elif isinstance(query, DropTables):
            return self._postgres_exec_ddl(query)
        elif isinstance(query, Call):
            return self._postgres_call_procedure(query)
        else:
            logger.info(f"Operation not supported in PostgreSQL {type(query)}")
            return Response(RESPONSE_TYPE.TABLE, data_frame=pd.DataFrame())
        
    def _postgres_call_procedure(self, sql):
        engine = create_engine(self.sqlalchemy_uri)
        with engine.connect() as conn:
            try:
                procedure_name = ".".join(sql.name.parts[1:])
                params = sql.query_str
                res = conn.execute(text(f"SELECT {procedure_name}({params})"))
                conn.commit()                
                return Response(RESPONSE_TYPE.OK, affected_rows=res.rowcount)
            except Exception as ex:
                conn.rollback()
                logger.error(f"Error executing procedure {procedure_name}({params}), {ex}")                
                #return Response(RESPONSE_TYPE.ERROR, error_message=f"Error executing procedure {procedure_name}({params}), {ex}")
                raise Exception(f"Error executing procedure {procedure_name}({params}), {ex}")
                

    def _postgres_table_delete(self, sql):
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
            
    def _postgres_table_create(self, query):
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

            

    def _postgres_exec_ddl(self, sql):
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


    def _postgres_table_insert(self, table_name: str, df: pd.DataFrame, using: dict = {}):
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


    def get_tables(self, all: bool = False) -> Response:
        """
        Get a list with all of the tabels in PostgreSQL selected database
        """
        all_filter = "and table_schema = current_schema()"
        if all is True:
            all_filter = ""
        query = f"""
            SELECT
                table_schema,
                table_name,
                table_type
            FROM
                information_schema.tables
            WHERE
                table_schema NOT IN ('information_schema', 'pg_catalog')
                and table_type in ('BASE TABLE', 'VIEW')
                {all_filter}
        """
        return self.native_query(query, lower_col_names=False)

    def get_columns(self, table_name: str, schema_name: Optional[str] = None) -> Response:
        """
        Show details about the table
        """
        if not table_name or not isinstance(table_name, str):
            raise ValueError("Invalid table name provided.")
        if isinstance(schema_name, str):
            schema_name = f"'{schema_name}'"
        else:
            schema_name = "current_schema()"
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
            AND
                table_schema = {schema_name}
        """
        result = self.native_query(query, lower_col_names=False)
        result.resp_type = RESPONSE_TYPE.COLUMNS_TABLE
        return result
