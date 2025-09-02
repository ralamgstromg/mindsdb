from mindsdb.sql_parser.ast.base import ASTNode
from mindsdb.sql_parser.ast.select import Identifier
from mindsdb.sql_parser.utils import indent


class AlterDatabase(ASTNode):
    """
    Alter a database.
    """
    def __init__(self, name: Identifier, altered_params: dict, *args, **kwargs):
        """
        Args:
            name: Identifier -- name of the database to alter.
            altered_params: dict -- parameters to alter in the database.
        """
        super().__init__(*args, **kwargs)
        self.name = name
        self.params = altered_params

    def to_tree(self, *args, level=0, **kwargs):
        ind = indent(level)
        out_str = f'{ind}AlterDatabase(' \
                  f'name={self.name.to_string()}, ' \
                  f'altered_params={self.params})'
        return out_str

    def get_string(self, *args, **kwargs):
        params = self.params.copy()

        set_ar = [f'{k}={repr(v)}' for k, v in params.items()]
        set_str = ', '.join(set_ar)

        out_str = f'ALTER DATABASE {self.name.to_string()} {set_str}'
        return out_str