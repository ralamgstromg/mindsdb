from mindsdb.sql_parser.ast.base import ASTNode
from mindsdb.sql_parser.utils import indent


class Truncate(ASTNode):
    def __init__(self,
                 table,
                 *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.table = table

    def to_tree(self, *args, level=0, **kwargs):
        ind = indent(level)
        ind1 = indent(level + 1)

        out_str = f'{ind}Truncate(\n' \
                  f'{ind1}table={self.table.to_tree()}\n' \
                  f'{ind})\n'
        return out_str

    def get_string(self, *args, **kwargs):
        return f'TRUNCATE TABLE {str(self.table)}'
