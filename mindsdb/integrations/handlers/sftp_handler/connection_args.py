from collections import OrderedDict

from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE


connection_args = OrderedDict(
    server={
        'type': ARG_TYPE.STR,
        'description': 'Ip from the source server',
        'required': True,
        'label': 'Server'
    },
    port={
        'type': ARG_TYPE.INT,
        'description': 'The port for the sftp server',
        'required': True,
        'label': 'SFTP Port'
    },
    username={
        'type': ARG_TYPE.STR,
        'description': 'The username to connect',
        'required': True,
        'label': 'Username to connect'
    },
    password={
        'type': ARG_TYPE.STR,
        'description': 'The password to connect',
        'secret': True,
        'required': True,
        'label': 'Password to connect'
    },    
)

connection_args_example = OrderedDict(
    server='localhost',
    port=2222,
    username='user',
    password='password',
)
