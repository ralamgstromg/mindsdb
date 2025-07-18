import json

#import pandas as pd
import polars as pd
from mindsdb_sql_parser.ast import BinaryOperation, Constant, Select
from mindsdb_sql_parser.ast.base import ASTNode

from mindsdb.interfaces.agents.agents_controller import AgentsController
from mindsdb.interfaces.jobs.jobs_controller import JobsController
from mindsdb.interfaces.skills.skills_controller import SkillsController
from mindsdb.interfaces.database.views import ViewController
from mindsdb.interfaces.database.projects import ProjectController
from mindsdb.interfaces.query_context.context_controller import query_context_controller

from mindsdb.api.executor.datahub.datanodes.system_tables import Table
from mindsdb.api.mysql.mysql_proxy.libs.constants.mysql import MYSQL_DATA_TYPE

from collections import OrderedDict


def to_json(obj):
    if obj is None:
        return None
    try:
        return json.dumps(obj)
    except TypeError:
        return obj


def get_project_name(query: ASTNode = None):
    project_name = None
    if (
        isinstance(query, Select)
        and type(query.where) is BinaryOperation
        and query.where.op == "="
        and query.where.args[0].parts == ["project"]
        and isinstance(query.where.args[1], Constant)
    ):
        project_name = query.where.args[1].value
    return project_name


class MdbTable(Table):
    visible: bool = True


class ModelsTable(MdbTable):
    name = "MODELS"
    columns = [
        "NAME",
        "ENGINE",
        "PROJECT",
        "ACTIVE",
        "VERSION",
        "STATUS",
        "ACCURACY",
        "PREDICT",
        "UPDATE_STATUS",
        "MINDSDB_VERSION",
        "ERROR",
        "SELECT_DATA_QUERY",
        "TRAINING_OPTIONS",
        "CURRENT_TRAINING_PHASE",
        "TOTAL_TRAINING_PHASES",
        "TRAINING_PHASE_NAME",
        "TAG",
        "CREATED_AT",
        "TRAINING_TIME",
    ]

    # mysql_types = [
    #     MYSQL_DATA_TYPE.VARCHAR,
    #     MYSQL_DATA_TYPE.VARCHAR,
    #     MYSQL_DATA_TYPE.VARCHAR,
    #     MYSQL_DATA_TYPE.VARCHAR,
    #     MYSQL_DATA_TYPE.VARCHAR,
    #     MYSQL_DATA_TYPE.VARCHAR,
    #     MYSQL_DATA_TYPE.VARCHAR,
    #     MYSQL_DATA_TYPE.VARCHAR,
    #     MYSQL_DATA_TYPE.VARCHAR,
    #     MYSQL_DATA_TYPE.VARCHAR,
    #     MYSQL_DATA_TYPE.VARCHAR,
    #     MYSQL_DATA_TYPE.VARCHAR,
    #     MYSQL_DATA_TYPE.VARCHAR,
    #     MYSQL_DATA_TYPE.VARCHAR,
    #     MYSQL_DATA_TYPE.VARCHAR,
    #     MYSQL_DATA_TYPE.VARCHAR,
    #     MYSQL_DATA_TYPE.VARCHAR,
    #     MYSQL_DATA_TYPE.VARCHAR,
    #     MYSQL_DATA_TYPE.VARCHAR,
    # ]

    @classmethod
    def get_data(cls, session, inf_schema, **kwargs):
        data = []
        for project_name in inf_schema.get_projects_names():
            project = inf_schema.database_controller.get_project(name=project_name)
            project_models = project.get_models(active=None, with_secrets=session.show_secrets)
            for row in project_models:
                table_name = row["name"]
                table_meta = row["metadata"]

                data.append(
                    [
                        table_name,
                        table_meta["engine"],
                        project_name,
                        table_meta["active"],
                        table_meta["version"],
                        table_meta["status"],
                        table_meta["accuracy"],
                        table_meta["predict"],
                        table_meta["update_status"],
                        table_meta["mindsdb_version"],
                        table_meta["error"],
                        table_meta["select_data_query"],
                        to_json(table_meta["training_options"]),
                        table_meta["current_training_phase"],
                        table_meta["total_training_phases"],
                        table_meta["training_phase_name"],
                        table_meta["label"],
                        row["created_at"],
                        table_meta["training_time"],
                    ]
                )
            # TODO optimise here
            # if target_table is not None and target_table != project_name:
            #     continue

        df = pd.DataFrame(data, schema=cls.columns, orient="row")
        return df


class DatabasesTable(MdbTable):
    name = "DATABASES"
    columns = ["NAME", "TYPE", "ENGINE", "CONNECTION_DATA"]
    # mysql_types = OrderedDict({
    #     "NAME": MYSQL_DATA_TYPE.VARCHAR,
    #     "TYPE": MYSQL_DATA_TYPE.VARCHAR,
    #     "ENGINE": MYSQL_DATA_TYPE.VARCHAR,
    #     "COLLECTION_DATA": MYSQL_DATA_TYPE.VARCHAR,
    # })

    @classmethod
    def get_data(cls, session, inf_schema, **kwargs):
        project = inf_schema.database_controller.get_list(with_secrets=session.show_secrets)
        data = [[x["name"], x["type"], x["engine"], to_json(x.get("connection_data"))] for x in project]

        df = pd.DataFrame(data, schema=cls.columns, orient="row")
        return df


class MLEnginesTable(MdbTable):
    name = "ML_ENGINES"
    columns = ["NAME", "HANDLER", "CONNECTION_DATA"]
    # mysql_types = [
    #     MYSQL_DATA_TYPE.VARCHAR,
    #     MYSQL_DATA_TYPE.VARCHAR,
    #     MYSQL_DATA_TYPE.VARCHAR,
    # ]

    @classmethod
    def get_data(cls, session, inf_schema, **kwargs):
        integrations = inf_schema.integration_controller.get_all(show_secrets=session.show_secrets)
        ml_integrations = {key: val for key, val in integrations.items() if val["type"] == "ml"}

        data = []
        for _key, val in ml_integrations.items():
            data.append([val["name"], val.get("engine"), to_json(val.get("connection_data"))])

        df = pd.DataFrame(data, schema=cls.columns, orient="row")
        return df


class HandlersTable(MdbTable):
    name = "HANDLERS"
    columns = [
        "NAME",
        "TYPE",
        "TITLE",
        "DESCRIPTION",
        "VERSION",
        "CONNECTION_ARGS",
        "IMPORT_SUCCESS",
        "IMPORT_ERROR",
    ]
    # mysql_types = [
    #     MYSQL_DATA_TYPE.VARCHAR,
    #     MYSQL_DATA_TYPE.VARCHAR,
    #     MYSQL_DATA_TYPE.VARCHAR,
    #     MYSQL_DATA_TYPE.VARCHAR,
    #     MYSQL_DATA_TYPE.VARCHAR,
    #     MYSQL_DATA_TYPE.VARCHAR,
    #     MYSQL_DATA_TYPE.VARCHAR,
    #     MYSQL_DATA_TYPE.VARCHAR,        
    # ]

    @classmethod
    def get_data(cls, inf_schema, **kwargs):
        handlers = inf_schema.integration_controller.get_handlers_import_status()

        data = []
        for _key, val in handlers.items():
            connection_args = val.get("connection_args")
            if connection_args is not None:
                connection_args = to_json(connection_args)
            import_success = val.get("import", {}).get("success")
            import_error = val.get("import", {}).get("error_message")
            data.append(
                [
                    val["name"],
                    val.get("type"),
                    val.get("title"),
                    val.get("description"),
                    val.get("version"),
                    connection_args,
                    import_success,
                    import_error,
                ]
            )

        df = pd.DataFrame(data, schema=cls.columns, orient="row")
        return df


class JobsTable(MdbTable):
    name = "JOBS"
    columns = [
        "NAME",
        "PROJECT",
        "START_AT",
        "END_AT",
        "NEXT_RUN_AT",
        "SCHEDULE_STR",
        "QUERY",
        "IF_QUERY",
        "VARIABLES",
    ]
    # mysql_types = [
    #     MYSQL_DATA_TYPE.VARCHAR,
    #     MYSQL_DATA_TYPE.VARCHAR,
    #     MYSQL_DATA_TYPE.VARCHAR,
    #     MYSQL_DATA_TYPE.VARCHAR,
    #     MYSQL_DATA_TYPE.VARCHAR,
    #     MYSQL_DATA_TYPE.VARCHAR,
    #     MYSQL_DATA_TYPE.VARCHAR,
    #     MYSQL_DATA_TYPE.VARCHAR,
    #     MYSQL_DATA_TYPE.VARCHAR,        
    # ]

    @classmethod
    def get_data(cls, query: ASTNode = None, **kwargs):
        jobs_controller = JobsController()

        project_name = None
        if (
            isinstance(query, Select)
            and type(query.where) is BinaryOperation
            and query.where.op == "="
            and query.where.args[0].parts == ["project"]
            and isinstance(query.where.args[1], Constant)
        ):
            project_name = query.where.args[1].value

        data = jobs_controller.get_list(project_name)

        columns = cls.columns
        columns_lower = [col.lower() for col in columns]

        # to list of lists
        data = [[row[k] for k in columns_lower] for row in data]

        return pd.DataFrame(data, schema=columns, orient="row")


class TriggersTable(MdbTable):
    name = "TRIGGERS"
    columns = [
        "TRIGGER_CATALOG",
        "TRIGGER_SCHEMA",
        "TRIGGER_NAME",
        "EVENT_MANIPULATION",
        "EVENT_OBJECT_CATALOG",
        "EVENT_OBJECT_SCHEMA",
        "EVENT_OBJECT_TABLE",
        "ACTION_ORDER",
        "ACTION_CONDITION",
        "ACTION_STATEMENT",
        "ACTION_ORIENTATION",
        "ACTION_TIMING",
        "ACTION_REFERENCE_OLD_TABLE",
        "ACTION_REFERENCE_NEW_TABLE",
        "ACTION_REFERENCE_OLD_ROW",
        "ACTION_REFERENCE_NEW_ROW",
        "CREATED",
        "SQL_MODE",
        "DEFINER",
        "CHARACTER_SET_CLIENT",
        "COLLATION_CONNECTION",
        "DATABASE_COLLATION",
    ]
    # mysql_types = [
    #     MYSQL_DATA_TYPE.VARCHAR,
    #     MYSQL_DATA_TYPE.VARCHAR,
    #     MYSQL_DATA_TYPE.VARCHAR,
    #     MYSQL_DATA_TYPE.VARCHAR,
    #     MYSQL_DATA_TYPE.VARCHAR,
    #     MYSQL_DATA_TYPE.VARCHAR,
    #     MYSQL_DATA_TYPE.VARCHAR,
    #     MYSQL_DATA_TYPE.VARCHAR,
    #     MYSQL_DATA_TYPE.VARCHAR,
    #     MYSQL_DATA_TYPE.VARCHAR,
    #     MYSQL_DATA_TYPE.VARCHAR,
    #     MYSQL_DATA_TYPE.VARCHAR,
    #     MYSQL_DATA_TYPE.VARCHAR,
    #     MYSQL_DATA_TYPE.VARCHAR,
    #     MYSQL_DATA_TYPE.VARCHAR,
    #     MYSQL_DATA_TYPE.VARCHAR,
    #     MYSQL_DATA_TYPE.VARCHAR,
    #     MYSQL_DATA_TYPE.VARCHAR,
    #     MYSQL_DATA_TYPE.VARCHAR,
    #     MYSQL_DATA_TYPE.VARCHAR,
    #     MYSQL_DATA_TYPE.VARCHAR,
    #     MYSQL_DATA_TYPE.VARCHAR,
    # ]

    mindsdb_columns = ["NAME", "PROJECT", "DATABASE", "TABLE", "QUERY", "LAST_ERROR"]

    @classmethod
    def get_data(cls, query: ASTNode = None, inf_schema=None, **kwargs):
        from mindsdb.interfaces.triggers.triggers_controller import TriggersController

        triggers_controller = TriggersController()

        project_name = None
        if (
            isinstance(query, Select)
            and type(query.where) is BinaryOperation
            and query.where.op == "="
            and query.where.args[0].parts == ["project"]
            and isinstance(query.where.args[1], Constant)
        ):
            project_name = query.where.args[1].value

        data = triggers_controller.get_list(project_name)

        columns = cls.mindsdb_columns
        if inf_schema.session.api_type == "sql":
            columns = columns + cls.columns
        columns_lower = [col.lower() for col in columns]

        # to list of lists
        data = [[row.get(k) for k in columns_lower] for row in data]

        return pd.DataFrame(data, schema=columns, orient="row")


class ChatbotsTable(MdbTable):
    name = "CHATBOTS"
    columns = [
        "NAME",
        "PROJECT",
        "DATABASE",
        "MODEL_NAME",
        "PARAMS",
        "IS_RUNNING",
        "LAST_ERROR",
        "WEBHOOK_TOKEN",
    ]
    # mysql_types = [
    #     MYSQL_DATA_TYPE.VARCHAR,
    #     MYSQL_DATA_TYPE.VARCHAR,
    #     MYSQL_DATA_TYPE.VARCHAR,
    #     MYSQL_DATA_TYPE.VARCHAR,
    #     MYSQL_DATA_TYPE.VARCHAR,
    #     MYSQL_DATA_TYPE.VARCHAR,
    #     MYSQL_DATA_TYPE.VARCHAR,
    #     MYSQL_DATA_TYPE.VARCHAR,        
    # ]

    @classmethod
    def get_data(cls, query: ASTNode = None, **kwargs):
        from mindsdb.interfaces.chatbot.chatbot_controller import ChatBotController

        chatbot_controller = ChatBotController()

        project_name = None
        if (
            isinstance(query, Select)
            and type(query.where) is BinaryOperation
            and query.where.op == "="
            and query.where.args[0].parts == ["project"]
            and isinstance(query.where.args[1], Constant)
        ):
            project_name = query.where.args[1].value

        chatbot_data = chatbot_controller.get_chatbots(project_name=project_name)

        columns = cls.columns
        columns_lower = [col.lower() for col in columns]

        # to list of lists
        data = []
        for row in chatbot_data:
            row["params"] = to_json(row["params"])
            data.append([row[k] for k in columns_lower])

        return pd.DataFrame(data, schema=columns, orient="row")


class KBTable(MdbTable):
    name = "KNOWLEDGE_BASES"
    columns = [
        "NAME",
        "PROJECT",
        "EMBEDDING_MODEL",
        "RERANKING_MODEL",
        "STORAGE",
        "METADATA_COLUMNS",
        "CONTENT_COLUMNS",
        "ID_COLUMN",
        "PARAMS",
        "INSERT_STARTED_AT",
        "INSERT_FINISHED_AT",
        "PROCESSED_ROWS",
        "ERROR",
        "QUERY_ID",
    ]
    # mysql_types = [
    #     MYSQL_DATA_TYPE.VARCHAR,
    #     MYSQL_DATA_TYPE.VARCHAR,
    #     MYSQL_DATA_TYPE.VARCHAR,
    #     MYSQL_DATA_TYPE.VARCHAR,
    #     MYSQL_DATA_TYPE.VARCHAR,
    #     MYSQL_DATA_TYPE.VARCHAR,
    #     MYSQL_DATA_TYPE.VARCHAR,
    #     MYSQL_DATA_TYPE.VARCHAR,
    #     MYSQL_DATA_TYPE.VARCHAR,
    #     MYSQL_DATA_TYPE.VARCHAR,
    #     MYSQL_DATA_TYPE.VARCHAR,
    #     MYSQL_DATA_TYPE.VARCHAR,
    #     MYSQL_DATA_TYPE.VARCHAR,
    #     MYSQL_DATA_TYPE.VARCHAR,        
    # ]

    @classmethod
    def get_data(cls, query: ASTNode = None, inf_schema=None, **kwargs):
        project_name = get_project_name(query)

        from mindsdb.interfaces.knowledge_base.controller import KnowledgeBaseController

        controller = KnowledgeBaseController(inf_schema.session)
        kb_list = controller.list(project_name)

        # shouldn't be a lot of queries, we can fetch them all
        queries_data = {item["id"]: item for item in query_context_controller.list_queries()}

        data = []

        for kb in kb_list:
            query_item = {}
            query_id = kb["query_id"]
            if query_id is not None:
                if query_id in queries_data:
                    query_item = queries_data.get(query_id)
                else:
                    query_id = None

            data.append(
                (
                    kb["name"],
                    kb["project_name"],
                    to_json(kb["embedding_model"]),
                    to_json(kb["reranking_model"]),
                    kb["vector_database"] + "." + kb["vector_database_table"],
                    to_json(kb["metadata_columns"]),
                    to_json(kb["content_columns"]),
                    kb["id_column"],
                    to_json(kb["params"]),
                    query_item.get("started_at"),
                    query_item.get("finished_at"),
                    query_item.get("processed_rows"),
                    query_item.get("error"),
                    query_id,
                )
            )

        return pd.DataFrame(data, schema=cls.columns, orient="row")


class SkillsTable(MdbTable):
    name = "SKILLS"
    columns = ["NAME", "PROJECT", "TYPE", "PARAMS"]
    # mysql_types = [
    #     MYSQL_DATA_TYPE.VARCHAR,
    #     MYSQL_DATA_TYPE.VARCHAR,
    #     MYSQL_DATA_TYPE.VARCHAR,
    #     MYSQL_DATA_TYPE.VARCHAR,        
    # ]

    @classmethod
    def get_data(cls, query: ASTNode = None, **kwargs):
        skills_controller = SkillsController()

        project_name = get_project_name(query)

        all_skills = skills_controller.get_skills(project_name)

        project_controller = ProjectController()
        project_names = {p.id: p.name for p in project_controller.get_list()}

        # NAME, PROJECT, TYPE, PARAMS
        data = [(s.name, project_names[s.project_id], s.type, s.params) for s in all_skills]
        return pd.DataFrame(data, schema=cls.columns, orient="row")


class AgentsTable(MdbTable):
    name = "AGENTS"
    columns = ["NAME", "PROJECT", "MODEL_NAME", "SKILLS", "PARAMS"]
    # mysql_types = [
    #     MYSQL_DATA_TYPE.VARCHAR,
    #     MYSQL_DATA_TYPE.VARCHAR,
    #     MYSQL_DATA_TYPE.VARCHAR,
    #     MYSQL_DATA_TYPE.VARCHAR,
    #     MYSQL_DATA_TYPE.VARCHAR,        
    # ]

    @classmethod
    def get_data(cls, query: ASTNode = None, inf_schema=None, **kwargs):
        agents_controller = AgentsController()

        project_name = get_project_name(query)
        all_agents = agents_controller.get_agents(project_name)

        project_controller = ProjectController()
        project_names = {i.id: i.name for i in project_controller.get_list()}

        # NAME, PROJECT, MODEL, SKILLS, PARAMS
        data = [
            (
                a.name,
                project_names[a.project_id],
                a.model_name,
                [rel.skill.name for rel in a.skills_relationships],
                to_json(a.params),
            )
            for a in all_agents
        ]
        return pd.DataFrame(data, schema=cls.columns, orient="row")


class ViewsTable(MdbTable):
    name = "VIEWS"
    columns = ["NAME", "PROJECT", "QUERY", "TABLE_SCHEMA", "TABLE_NAME", "VIEW_DEFINITION"]
    # mysql_types = [
    #     MYSQL_DATA_TYPE.VARCHAR,
    #     MYSQL_DATA_TYPE.VARCHAR,
    #     MYSQL_DATA_TYPE.VARCHAR,
    #     MYSQL_DATA_TYPE.VARCHAR,
    #     MYSQL_DATA_TYPE.VARCHAR,
    #     MYSQL_DATA_TYPE.VARCHAR,        
    # ]

    @classmethod
    def get_data(cls, query: ASTNode = None, **kwargs):
        project_name = get_project_name(query)

        data = ViewController().list(project_name)

        columns_lower = [col.lower() for col in cls.columns]

        # to list of lists
        data = [[row[k] for k in columns_lower] for row in data]

        return pd.DataFrame(data, schema=cls.columns, orient="row")


class QueriesTable(MdbTable):
    name = "QUERIES"
    columns = [
        "ID",
        "STARTED_AT",
        "FINISHED_AT",
        "PROCESSED_ROWS",
        "ERROR",
        "SQL",
        "DATABASE",
        "PARAMETERS",
        "CONTEXT",
        "UPDATED_AT",
    ]

    # mysql_types = [
    #     MYSQL_DATA_TYPE.BIGINT,
    #     MYSQL_DATA_TYPE.DATETIME,
    #     MYSQL_DATA_TYPE.DATETIME,
    #     MYSQL_DATA_TYPE.BIGINT,
    #     MYSQL_DATA_TYPE.VARCHAR,
    #     MYSQL_DATA_TYPE.VARCHAR,
    #     MYSQL_DATA_TYPE.VARCHAR,
    #     MYSQL_DATA_TYPE.VARCHAR,
    #     MYSQL_DATA_TYPE.VARCHAR,
    #     MYSQL_DATA_TYPE.DATETIME,        
    # ]
    
    cols_dtypes = {
        "ID": pd.Int64,
        "STARTED_AT": pd.Datetime,
        "FINISHED_AT": pd.Datetime,
        "PROCESSED_ROWS": pd.Int64,
        "ERROR": pd.String,
        "SQL": pd.String,
        "DATABASE": pd.String,
        "PARAMETERS": pd.String,
        "CONTEXT": pd.String,
        "UPDATED_AT": pd.Datetime,
    }

    @classmethod
    def get_data(cls, **kwargs):
        """
        Returns all queries in progres or recently completed
        Only queries marked as is_resumable by planner are stored in this table
        :param kwargs:
        :return:
        """

        data = query_context_controller.list_queries()
        #columns_lower = [col.lower() for col in cls.columns]
        #print(cls.cols_dtypes)
        columns_lower = {col.lower(): value for col, value in cls.cols_dtypes.items()}

        # print(data)
        # print(columns_lower)

        #data = [[row[k] for k in columns_lower] for row in data]
        
        # print(cls.columns)

        

        if data is None or len(data) == 0:
            to_return = pd.DataFrame([], schema=columns_lower, orient="row")
        else:
            #to_return = pd.DataFrame(data)
            to_return = pd.DataFrame(data, schema=columns_lower, orient="row")

        #print(to_return)

        # print("ANTES", to_return)

        # to_return = to_return.with_columns([
        #     pd.when(pd.col("parameters") == pd.struct([])).then(pd.Null).otherwise(pd.col("parameters")).alias("parameters"),
        #     pd.when(pd.col("context") == pd.struct([])).then(pd.Null).otherwise(pd.col("context")).alias("context")
        # ])

        # print("DESPUES", to_return)
        
        # to_return = to_return.with_columns([
        #     pd.col(col).struct.json_encode().alias(col)
        #     for col in cls.struct_cols            
        # ])

        #print(to_return)

        return to_return
