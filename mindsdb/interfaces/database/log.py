from typing import List
from copy import deepcopy
from abc import ABC, abstractmethod
from collections import OrderedDict

#import pandas as pd
import polars as pd
from mindsdb_sql_parser import parse_sql
from mindsdb_sql_parser.ast import Select, Identifier, Star, BinaryOperation, Constant, Join, Function
from mindsdb_sql_parser.utils import JoinType

from mindsdb.utilities.render.sqlalchemy_render import SqlalchemyRender
from mindsdb.integrations.utilities.query_traversal import query_traversal
from mindsdb.utilities.functions import resolve_table_identifier
from mindsdb.api.executor.utilities.sql import get_query_tables
from mindsdb.utilities.exception import EntityNotExistsError
import mindsdb.interfaces.storage.db as db
from mindsdb.utilities.context import context as ctx
from mindsdb.api.executor.datahub.classes.response import DataHubResponse
from mindsdb.api.executor.datahub.classes.tables_row import (
    TABLES_ROW_TYPE,
    TablesRow,
)


class LogTable(ABC):
    """Base class for 'table' entitie in internal 'log' database

    Attributes:
        name (str): name of the table
        deletable (bool): is it possible to delete a table
        visible (bool): should be table visible in GUI sidebar
        kind (str): type of the table/view
    """

    name: str
    deletable: bool = False
    visible: bool = True
    kind: str = "table"

    @staticmethod
    @abstractmethod
    def _get_base_subquery() -> Select:
        """Get a query that returns the table from internal db

        Returns:
            Select: 'select' query that returns table
        """
        pass

    @staticmethod
    def company_id_comparison(table_a: str, table_b: str) -> BinaryOperation:
        """Make statement for 'safe' comparison of company_id of two tables

        Args:
            table_a (str): name of first table
            table_b (str): name of second table

        Returns:
            BinaryOperation: statement that can be used for 'safe' comparison
        """
        return BinaryOperation(
            op="=",
            args=(
                Function(op="coalesce", args=(Identifier(f"{table_a}.company_id"), 0)),
                Function(op="coalesce", args=(Identifier(f"{table_b}.company_id"), 0)),
            ),
        )


class LLMLogTable(LogTable):
    name = "llm_log"

    columns = [
        "API_KEY",
        "MODEL_NAME",
        "INPUT",
        "OUTPUT",
        "START_TIME",
        "END_TIME",
        "PROMPT_TOKENS",
        "COMPLETION_TOKENS",
        "TOTAL_TOKENS",
        "SUCCESS",
    ]

    #types_map = {"SUCCESS": "boolean", "START_TIME": "datetime64[ns]", "END_TIME": "datetime64[ns]"}
    #types_map = {"SUCCESS": pd.Boolean, "START_TIME": pd.Datetime, "END_TIME": pd.Datetime}
    schemas_map = {
        "api_key": pd.String,
        "model_name": pd.String,
        "input": pd.String,
        "output": pd.String,
        "start_time": pd.Datetime,
        "end_time": pd.Datetime,
        "prompt_tokens": pd.Int32,
        "completion_tokens": pd.Int32,
        "total_tokens": pd.Int32,
        "success": pd.Boolean,
    }

    @staticmethod
    def _get_base_subquery() -> Select:
        query = Select(
            targets=[
                Identifier("llm_log.api_key", alias=Identifier("api_key")),
                Identifier("predictor.name", alias=Identifier("model_name")),
                Identifier("llm_log.input", alias=Identifier("input")),
                Identifier("llm_log.output", alias=Identifier("output")),
                Identifier("llm_log.start_time", alias=Identifier("start_time")),
                Identifier("llm_log.end_time", alias=Identifier("end_time")),
                Identifier("llm_log.prompt_tokens", alias=Identifier("prompt_tokens")),
                Identifier("llm_log.completion_tokens", alias=Identifier("completion_tokens")),
                Identifier("llm_log.total_tokens", alias=Identifier("total_tokens")),
                Identifier("llm_log.success", alias=Identifier("success")),
            ],
            from_table=Join(
                left=Identifier("llm_log"),
                right=Identifier("predictor"),
                join_type=JoinType.LEFT_JOIN,
                condition=BinaryOperation(
                    op="and",
                    args=(
                        LLMLogTable.company_id_comparison("llm_log", "predictor"),
                        BinaryOperation(op="=", args=(Identifier("llm_log.model_id"), Identifier("predictor.id"))),
                    ),
                ),
            ),
            where=BinaryOperation(
                op="is" if ctx.company_id is None else "=",
                args=(Identifier("llm_log.company_id"), Constant(ctx.company_id)),
            ),
            alias=Identifier("llm_log"),
        )
        return query


class JobsHistoryTable(LogTable):
    name = "jobs_history"

    columns = ["NAME", "PROJECT", "RUN_START", "RUN_END", "ERROR", "QUERY"]
    #types_map = {"RUN_START": "datetime64[ns]", "RUN_END": "datetime64[ns]"}
    #types_map = {"RUN_START": pd.Datetime, "RUN_END": pd.Datetime}
    schemas_map = {
        "name": pd.String,
        "project": pd.String,
        "run_start": pd.Datetime,
        "run_end": pd.Datetime,
        "error": pd.String,
        "query": pd.String,
    }

    @staticmethod
    def _get_base_subquery() -> Select:
        query = Select(
            targets=[
                Identifier("jobs.name", alias=Identifier("name")),
                Identifier("project.name", alias=Identifier("project")),
                Identifier("jobs_history.start_at", alias=Identifier("run_start")),
                Identifier("jobs_history.end_at", alias=Identifier("run_end")),
                Identifier("jobs_history.error", alias=Identifier("error")),
                Identifier("jobs_history.query_str", alias=Identifier("query")),
            ],
            from_table=Join(
                left=Join(
                    left=Identifier("jobs_history"),
                    right=Identifier("jobs"),
                    join_type=JoinType.LEFT_JOIN,
                    condition=BinaryOperation(
                        op="and",
                        args=(
                            LLMLogTable.company_id_comparison("jobs_history", "jobs"),
                            BinaryOperation(op="=", args=(Identifier("jobs_history.job_id"), Identifier("jobs.id"))),
                        ),
                    ),
                ),
                right=Identifier("project"),
                join_type=JoinType.LEFT_JOIN,
                condition=BinaryOperation(
                    op="and",
                    args=(
                        LLMLogTable.company_id_comparison("project", "jobs"),
                        BinaryOperation(op="=", args=(Identifier("project.id"), Identifier("jobs.project_id"))),
                    ),
                ),
            ),
            where=BinaryOperation(
                op="is" if ctx.company_id is None else "=",
                args=(Identifier("jobs_history.company_id"), Constant(ctx.company_id)),
            ),
            alias=Identifier("jobs_history"),
        )
        return query

default_log_types = {
    "name": pd.String,
    "project": pd.String,
    "run_start": pd.String,
    "run_end": pd.String,
    "error": pd.String,
    "query": pd.String,
    "api_key": pd.String,
    "model_name": pd.String,
    "input": pd.String,
    "output": pd.String,
    "start_time": pd.String,
    "end_time": pd.String,
    "prompt_tokens": pd.Int32,
    "completion_tokens": pd.Int32,
    "total_tokens": pd.Int32,
    "success": pd.Boolean,
}

class LogDBController:
    def __init__(self):
        self._tables = OrderedDict()
        self._tables["llm_log"] = LLMLogTable
        self._tables["jobs_history"] = JobsHistoryTable

    def get_list(self) -> List[LogTable]:
        return list(self._tables.values())

    def get(self, name: str = None) -> LogTable:
        try:
            return self._tables[name]
        except KeyError:
            raise EntityNotExistsError(f"Table log.{name} does not exists")

    def get_tables(self) -> OrderedDict:
        return self._tables

    def get_tree_tables(self) -> OrderedDict:
        return self._tables

    def get_tables_rows(self) -> List[TablesRow]:
        return [
            TablesRow(TABLE_TYPE=TABLES_ROW_TYPE.SYSTEM_VIEW, TABLE_NAME=table_name)
            for table_name in self._tables.keys()
        ]

    def query(self, query: Select = None, native_query: str = None, session=None) -> DataHubResponse:
        if native_query is not None:
            if query is not None:
                raise Exception("'query' and 'native_query' arguments can not be used together")
            query = parse_sql(native_query)
        else:
            query = deepcopy(query)

        if type(query) is not Select:
            raise Exception("Only 'SELECT' is allowed for tables in log database")
        tables = get_query_tables(query)
        if len(tables) != 1:
            raise Exception("Only one table may be in query to log database")
        table = tables[0]
        if table[0] is not None and table[0].lower() != "log":
            raise Exception("This is not a query to the log database")
        if table[1].lower() not in self._tables.keys():
            raise Exception(f"There is no table '{table[1]}' in the log database")

        log_table = self._tables[table[1].lower()]

        # region check that only allowed identifiers are used in the query
        available_columns_names = [column.lower() for column in log_table.columns]

        def check_columns(node, is_table, **kwargs):
            # region replace * to available columns
            if type(node) is Select:
                new_targets = []
                for target in node.targets:
                    if type(target) is Star:
                        new_targets += [Identifier(name) for name in available_columns_names]
                    else:
                        new_targets.append(target)
                node.targets = new_targets
            # endregion

            if type(node) is Identifier and is_table is False:
                parts = resolve_table_identifier(node)
                if parts[0] is not None and parts[0].lower() not in self._tables:
                    raise Exception(f"Table '{parts[0]}' can not be used in query")
                if parts[1].lower() not in available_columns_names:
                    raise Exception(f"Column '{parts[1]}' can not be used in query")

        query_traversal(query, check_columns)
        # endregion

        query.from_table = log_table._get_base_subquery()

        render_engine = db.engine.name
        if render_engine == "postgresql":
            "postgres"
        render = SqlalchemyRender(render_engine)
        query_str = render.get_string(query, with_failback=False)      
        
        df = pd.read_database(query_str, db.engine, schema_overrides=default_log_types)                  

        for t_name, t_table in self._tables.items():            
            casts = []
            if t_name in query_str:   
                for column_name, column_type in t_table.schemas_map.items():        
                    if column_name in df.columns:            
                        if column_type in (pd.Datetime, 'datetime[μs]'):
                            casts.append(pd.col(column_name).cast(pd.String).str.to_datetime("%Y-%m-%d %H:%M:%S%.f").alias(column_name))
                        else:
                            casts.append(pd.col(column_name).cast(column_type).alias(column_name))
            
                df = df.with_columns(casts)

        columns_info = [{"name": k, "type": v} for k, v in df.schema.items()]

        return DataHubResponse(data_frame=df, columns=columns_info)
