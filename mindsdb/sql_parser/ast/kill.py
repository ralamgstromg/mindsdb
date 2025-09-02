from mindsdb.sql_parser.utils import indent
from mindsdb.sql_parser.ast.base import ASTNode


class Kill(ASTNode):
    def __init__(self,
                 process_id=None,
                 *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.process_id = process_id or 0

    def to_tree(self, *args, level=0, **kwargs):
        ind = indent(level)
        ind1 = indent(level + 1)
        process_id_str = f'\n{ind1}process_id={self.process_id},'

        out_str = f'{ind}Kill(' \
                  f'{process_id_str}' \
                  f'\n{ind})'
        #print("[INSIDE_KILL/to_tree]", out_str)
        return out_str

    def get_string(self, *args, **kwargs):
        out_str = f'KILL {self.process_id};'
        #print("[INSIDE_KILL/get_string]", out_str)
        return out_str
