import time
import inspect
from dataclasses import astuple
from typing import Iterable, List

#import pandas
#import numpy as np
#import pandas as pd
import polars as pd
from sqlalchemy.types import Integer, Float

from mindsdb_sql_parser.ast.base import ASTNode
from mindsdb_sql_parser.ast import Insert, Identifier, CreateTable, TableColumn, DropTables

from mindsdb.api.executor.datahub.classes.response import DataHubResponse
from mindsdb.api.executor.datahub.datanodes.datanode import DataNode
from mindsdb.api.executor.datahub.classes.tables_row import TablesRow
from mindsdb.api.executor.data_types.response_type import RESPONSE_TYPE
from mindsdb.api.executor.sql_query.result_set import ResultSet
from mindsdb.integrations.libs.response import HandlerResponse, INF_SCHEMA_COLUMNS_NAMES
from mindsdb.integrations.utilities.utils import get_class_name
from mindsdb.metrics import metrics
from mindsdb.utilities import log
from mindsdb.utilities.profiler import profiler
from mindsdb.api.executor.datahub.datanodes.system_tables import infer_mysql_type

# Borrame
import sys

logger = log.getLogger(__name__)


class DBHandlerException(Exception):
    pass


class IntegrationDataNode(DataNode):
    type = "integration"

    def __init__(self, integration_name, ds_type, integration_controller):
        self.integration_name = integration_name
        self.ds_type = ds_type
        self.integration_controller = integration_controller
        self.integration_handler = self.integration_controller.get_data_handler(self.integration_name)

    def get_type(self):
        return self.type

    def get_tables(self):
        response = self.integration_handler.get_tables()
        #print("[get_tables.response]", response)
        if response.type == RESPONSE_TYPE.TABLE:
            #print("AQUI")
            #result_dict = response.data_frame.to_dict(orient="records")
            #print(type(response.data_frame))
            result_dict = response.data_frame.to_dicts()
            # print(result_dict)
            result = []
            for row in result_dict:
                #print(row)
                result.append(TablesRow.from_dict(row))

            # print(result)
            return result
        else:
            print("Excepcion")
            raise Exception(f"Can't get tables: {response.error_message}")

        result_dict = response.data_frame.to_dict(orient="records")
        return [TablesRow.from_dict(row) for row in result_dict]

    def get_table_columns_df(self, table_name: str, schema_name: str | None = None) -> pd.DataFrame:
        """Get a DataFrame containing representation of information_schema.columns for the specified table.

        Args:
            table_name (str): The name of the table to get columns from.
            schema_name (str | None): The name of the schema to get columns from.

        Returns:
            pd.DataFrame: A DataFrame containing representation of information_schema.columns for the specified table.
                          The DataFrame has list of columns as in the integrations.libs.response.INF_SCHEMA_COLUMNS_NAMES.
        """
        if "schema_name" in inspect.signature(self.integration_handler.get_columns).parameters:
            response = self.integration_handler.get_columns(table_name, schema_name)
        else:
            response = self.integration_handler.get_columns(table_name)

        if response.type == RESPONSE_TYPE.COLUMNS_TABLE:
            return response.data_frame

        if response.type != RESPONSE_TYPE.TABLE:
            logger.warning(f"Wrong response type for handler's `get_columns` call: {response.type}")
            return pd.DataFrame([], schema=astuple(INF_SCHEMA_COLUMNS_NAMES), orient="row")

        # region fallback for old handlers
        df = response.data_frame
        df.columns = [name.upper() for name in df.columns]
        if "FIELD" not in df.columns or "TYPE" not in df.columns:
            logger.warning(
                f"Response from the handler's `get_columns` call does not contain required columns: f{df.columns}"
            )
            return pd.DataFrame([], schema=astuple(INF_SCHEMA_COLUMNS_NAMES), orient="row")

        new_df = df[["FIELD", "TYPE"]]
        new_df.columns = ["COLUMN_NAME", "DATA_TYPE"]

        new_df[INF_SCHEMA_COLUMNS_NAMES.MYSQL_DATA_TYPE] = new_df[INF_SCHEMA_COLUMNS_NAMES.DATA_TYPE].apply(
            lambda x: infer_mysql_type(x).value
        )

        for column_name in astuple(INF_SCHEMA_COLUMNS_NAMES):
            if column_name in new_df.columns:
                continue
            new_df[column_name] = None
        # endregion

        return new_df

    def get_table_columns_names(self, table_name: str, schema_name: str | None = None) -> list[str]:
        """Get a list of column names for the specified table.

        Args:
            table_name (str): The name of the table to get columns from.
            schema_name (str | None): The name of the schema to get columns from.

        Returns:
            list[str]: A list of column names for the specified table.
        """
        df = self.get_table_columns_df(table_name, schema_name)
        return df[INF_SCHEMA_COLUMNS_NAMES.COLUMN_NAME].to_list()

    def drop_table(self, name: Identifier, if_exists=False):
        drop_ast = DropTables(tables=[name], if_exists=if_exists)
        self.query(drop_ast)

    def create_table(
        self,
        table_name: Identifier,
        result_set: ResultSet = None,
        columns: List[TableColumn] = None,
        is_replace: bool = False,
        is_create: bool = False,
        raise_if_exists: bool = True,
        using: dict = None,
        **kwargs,
    ) -> DataHubResponse:
        # is_create - create table
        #   if !raise_if_exists: error will be skipped
        # is_replace - drop table if exists
        # is_create==False and is_replace==False: just insert

        # print(table_name)
        # print(result_set)

        table_columns_meta = {}

        if columns is None:
            columns: list[TableColumn] = result_set.get_ast_columns()
            table_columns_meta = {column.name: column.type for column in columns}

        if is_replace:
            # drop
            drop_ast = DropTables(tables=[table_name], if_exists=True)
            self.query(drop_ast)
            is_create = True

        if is_create:
            create_table_ast = CreateTable(name=table_name, columns=columns, is_replace=is_replace, using=using)
            try:
                self.query(create_table_ast)
            except Exception as e:
                if raise_if_exists:
                    raise e

        if result_set is None:
            # it is just a 'create table'
            return DataHubResponse()

        # native insert
        if hasattr(self.integration_handler, "insert"):
            df = result_set.to_df()

            result: HandlerResponse = self.integration_handler.insert(table_name.parts[-1], df)
            return DataHubResponse(affected_rows=result.affected_rows)

        insert_columns = [Identifier(parts=[x.alias]) for x in result_set.columns]

        # adapt table types
        for col_idx, col in enumerate(result_set.columns):
            column_type = table_columns_meta[col.alias]

            if column_type == Integer:
                type_name = "int"
            elif column_type == Float:
                type_name = "float"
            else:
                continue

            try:
                result_set.set_col_type(col_idx, type_name)
            except Exception:
                pass

        values = result_set.get_raw_df() #.to_lists()
        #print(values)

        if len(values) == 0:
            # not need to insert
            return DataHubResponse()

        #print(values)
        #print(self)

        insert_ast = Insert(table=table_name, columns=insert_columns, values=values, is_plain=True, using=using)

        # print(f"{sys.getsizeof(insert_ast)} bytes")
        #print(insert_ast.using)


        try:
            result: DataHubResponse = self.query(insert_ast)
        except Exception as e:
            msg = f"[{self.ds_type}/{self.integration_name}]: {str(e)}"
            raise DBHandlerException(msg) from e

        return DataHubResponse(affected_rows=result.affected_rows)

    def has_support_stream(self) -> bool:
        # checks if data handler has query_stream method
        return hasattr(self.integration_handler, "query_stream") and callable(self.integration_handler.query_stream)

    @profiler.profile()
    def query_stream(self, query: ASTNode, fetch_size: int = None) -> Iterable:
        # print(query.using)
        # returns generator of results from handler (split by chunks)
        return self.integration_handler.query_stream(query, fetch_size=fetch_size)

    @profiler.profile()
    def query(self, query: ASTNode | None = None, native_query: str | None = None, session=None) -> DataHubResponse:
        try:            
            time_before_query = time.perf_counter()            
            # if query.using is not None:
            #     print(query.using)

            #print("[INTEGRATION_DATANODE/QUERY]", query, native_query)

            if query is not None:
                #print(self.integration_handler)
                #print(query)                
                result: HandlerResponse = self.integration_handler.query(query)
            else:
                #print("ELSE")
                # try to fetch native query
                result: HandlerResponse = self.integration_handler.native_query(native_query)
            
            
            # metrics
            elapsed_seconds = time.perf_counter() - time_before_query
            query_time_with_labels = metrics.INTEGRATION_HANDLER_QUERY_TIME.labels(
                get_class_name(self.integration_handler), result.type
            )
            query_time_with_labels.observe(elapsed_seconds)

            num_rows = 0
            if result.data_frame is not None:
                #num_rows = len(result.data_frame.index)
                num_rows = len(result.data_frame)
            response_size_with_labels = metrics.INTEGRATION_HANDLER_RESPONSE_SIZE.labels(
                get_class_name(self.integration_handler), result.type
            )
            response_size_with_labels.observe(num_rows)
        except Exception as e:
            msg = str(e).strip()
            if msg == "":
                msg = e.__class__.__name__
            msg = f"[{self.ds_type}/{self.integration_name}]: {msg}"
            raise DBHandlerException(msg) from e

        if result.type == RESPONSE_TYPE.ERROR:
            raise Exception(f"Error in {self.integration_name}: {result.error_message}")
        if result.type == RESPONSE_TYPE.OK:
            return DataHubResponse(affected_rows=result.affected_rows)
        


        df = result.data_frame
        # region clearing df from NaN values
        # recursion error appears in pandas 1.5.3 https://github.com/pandas-dev/pandas/pull/45749
        if isinstance(df, pd.Series):
            df = pd.DataFrame(df)

        # try:
        #     # replace python's Nan, np.NaN, np.nan and pd.NA to None
        #     # TODO keep all NAN to the end of processing, bacause replacing also changes dtypes
        #     df.replace([np.NaN, pandas.NA, pandas.NaT, pd.Null], None, inplace=True)
        # except Exception as e:
        #     logger.error(f"Issue with clearing DF from NaN values: {e}")
        # endregion
        
        #columns_info = [{"name": k, "type": v} for k, v in df.dtypes.items()]
        #print(df.schema.items())
        columns_info = [{"name": k, "type": v} for k, v in df.schema.items()]

        #print(columns_info)
        #print(df)

        return DataHubResponse(
            data_frame=df, columns=columns_info, affected_rows=result.affected_rows #, mysql_types=result.mysql_types
        )
