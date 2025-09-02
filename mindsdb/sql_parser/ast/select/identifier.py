import re
from copy import copy, deepcopy
from typing import List

from mindsdb.sql_parser.ast.base import ASTNode
from mindsdb.sql_parser.utils import indent
from mindsdb.sql_parser.ast.select import Star


no_wrap_identifier_regex = re.compile(r'[a-zA-Z_][a-zA-Z_0-9]*')
path_str_parts_regex = re.compile(r'(?:(?:(`[^`]+`))|([^.]+))')


def path_str_to_parts(path_str: str):
    parts, is_quoted = [], []
    for x in re.finditer(path_str_parts_regex, path_str):
        part = x[0].strip('`')
        parts.append(part)
        is_quoted.append(x[0] != part)

    return parts, is_quoted


RESERVED_KEYWORDS = {
    'PERSIST', 'IF', 'EXISTS', 'NULLS', 'FIRST', 'LAST',
    'ORDER', 'BY', 'GROUP', 'PARTITION'
}


def get_reserved_words():
    from mindsdb.sql_parser.lexer import MindsDBLexer

    reserved = RESERVED_KEYWORDS
    for word in MindsDBLexer.tokens:
        if '_' not in word:
            # exclude combinations
            reserved.add(word)
    return reserved


class Identifier(ASTNode):
    def __init__(self, path_str=None, parts=None, is_outer=False, with_rollup=False, *args, **kwargs):
        super().__init__(*args, **kwargs)
        assert path_str or parts, "Either path_str or parts must be provided for an Identifier"
        assert not (path_str and parts), "Provide either path_str or parts, but not both"
        if isinstance(path_str, Star) and not parts:
            parts = [Star()]

        if path_str and not parts:
            parts, is_quoted = path_str_to_parts(path_str)
        else:
            is_quoted = [False] * len(parts)
        assert isinstance(parts, list)
        self.parts = parts
        # parts which were quoted
        self.is_quoted: List[bool] = is_quoted
        # used to define type of implicit join in oracle
        self.is_outer: bool = is_outer
        self.with_rollup: bool = with_rollup

    @classmethod
    def from_path_str(self, value, *args, **kwargs):
        parts, _ = path_str_to_parts(value)
        return Identifier(parts=parts, *args, **kwargs)

    def parts_to_str(self):
        out_parts = []
        reserved_words = get_reserved_words()
        for part in self.parts:
            if isinstance(part, Star):
                part = str(part)
            else:
                if (
                    not no_wrap_identifier_regex.fullmatch(part)
                    or
                    part.upper() in reserved_words
                ):
                    part = f'`{part}`'

            out_parts.append(part)
        return '.'.join(out_parts)

    def to_tree(self, *args, level=0, **kwargs):
        alias_str = f', alias={self.alias.to_tree()}' if self.alias else ''
        return indent(level) + f'Identifier(parts={[str(i) for i in self.parts]}{alias_str})'

    def get_string(self, *args, **kwargs):
        return self.parts_to_str()

    def __copy__(self):
        identifier = Identifier(parts=copy(self.parts))
        identifier.alias = deepcopy(self.alias)
        identifier.parentheses = self.parentheses
        identifier.is_quoted = deepcopy(self.is_quoted)
        identifier.is_outer = self.is_outer
        identifier.with_rollup = self.with_rollup
        if hasattr(self, 'sub_select'):
            identifier.sub_select = deepcopy(self.sub_select)
        return identifier

    def __deepcopy__(self, memo):
        identifier = Identifier(parts=copy(self.parts))
        identifier.alias = deepcopy(self.alias)
        identifier.parentheses = self.parentheses
        identifier.is_quoted = deepcopy(self.is_quoted)
        identifier.is_outer = self.is_outer
        identifier.with_rollup = self.with_rollup
        if hasattr(self, 'sub_select'):
            identifier.sub_select = deepcopy(self.sub_select)
        return identifier
