from mindsdb.sql_parser.utils import indent
from mindsdb.sql_parser.ast.base import ASTNode


class Call(ASTNode):
    def __init__(self,
                 name,
                 query_str,
                 *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.name = name
        self.query_str = query_str or ''

    def to_tree(self, *args, level=0, **kwargs):
        ind = indent(level)
        ind1 = indent(level + 1)
        name_str = f'\n{ind1}name={self.name.to_string()},'

        out_str = f'{ind}Call(' \
                  f'{name_str} ({self.query_str})' \
                  f'\n{ind})'
        #print("[INSIDE_CALL/to_tree]", out_str)
        return out_str

    def get_string(self, *args, **kwargs):
        out_str = f'CALL {self.name.to_string()}({self.query_str});'
        #print("[INSIDE_CALL/get_string]", out_str)
        return out_str
