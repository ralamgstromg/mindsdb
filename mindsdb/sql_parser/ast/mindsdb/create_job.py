import json
import datetime as dt

from mindsdb.sql_parser.ast.base import ASTNode
from mindsdb.sql_parser.utils import indent


class CreateJob(ASTNode):
    def __init__(self,
                 name,
                 query_str,
                 start_str=None,
                 end_str=None,
                 repeat_str=None,
                 starting_time_str=None,
                 ending_time_str=None,
                 if_query_str=None,
                 if_not_exists=False,
                 *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.name = name
        self.query_str=query_str
        self.start_str = start_str
        self.end_str = end_str
        self.repeat_str = repeat_str
        self.date_format = '%Y-%m-%d %H:%M:%S'
        self.starting_time_str = starting_time_str
        self.ending_time_str = ending_time_str
        self.time_format = '%H:%M:%S'
        self.if_not_exists = if_not_exists
        self.if_query_str = if_query_str

    def to_tree(self, *args, level=0, **kwargs):
        ind = indent(level)
        ind1 = indent(level+1)
        name_str = f'\n{ind1}name={self.name.to_string()},'

        query_str = f'\n{ind1}query_str={repr(self.query_str)},'

        start_str = ''
        if self.start_str is not None:
            start_str = f'\n{ind1}start_str=\'{self.start_str}\','

        end_str = ''
        if self.end_str is not None:
            end_str = f'\n{ind1}end_str=\'{self.end_str}\','

        repeat_str = ''
        if self.repeat_str is not None:
            repeat_str = f'\n{ind1}repeat_str={self.repeat_str},'

        starting_time_str = ''
        if self.starting_time_str is not None:
            starting_time_str = f'\n{ind1}starting_time_str=\'{self.starting_time_str}\','

        ending_time_str = ''
        if self.ending_time_str is not None:
            ending_time_str = f'\n{ind1}ending_time_str=\'{self.ending_time_str}\','

        if_not_exists_str = ''
        if self.if_not_exists:
            if_not_exists_str = f'\n{ind1}if_not_exists=True,'

        if_query_str = ''
        if self.if_query_str is not None:
            if_query_str = f"\n{ind1}if_query='{self.if_query_str}'"

        out_str = f'{ind}CreateJob(' \
                  f'{if_not_exists_str}' \
                  f'{name_str}' \
                  f'{query_str}' \
                  f'{start_str}' \
                  f'{end_str}' \
                  f'{repeat_str}' \
                  f'{starting_time_str}' \
                  f'{ending_time_str}' \
                  f'{if_query_str}' \
                  f'\n{ind})'
        return out_str

    def get_string(self, *args, **kwargs):

        start_str = ''
        if self.start_str is not None:
            start_str = f" START '{self.start_str}'"

        end_str = ''
        if self.end_str is not None:
            end_str = f" END '{self.end_str}'"

        repeat_str = ''
        if self.repeat_str is not None:
            repeat_str = f" EVERY '{self.repeat_str}'"

        starting_time_str = ''
        if self.starting_time_str is not None:
            starting_time_str = f" STARTING_TIME '{self.starting_time_str}'"

        ending_time_str = ''
        if self.ending_time_str is not None:
            ending_time_str = f" ENDING_TIME '{self.ending_time_str}'"

        start_str = ''
        if self.start_str is not None:
            start_str = f" START '{self.start_str}'"

        end_str = ''
        if self.end_str is not None:
            end_str = f" END '{self.end_str}'"

        if_query_str = ''
        if self.if_query_str is not None:
            if_query_str = f" IF ({self.if_query_str})"

        out_str = f'CREATE JOB {"IF NOT EXISTS" if self.if_not_exists else ""} {self.name.to_string()} ({self.query_str}){start_str}{end_str}{starting_time_str}{ending_time_str}{repeat_str}{if_query_str}'
        return out_str
