from datetime import datetime, date
from duckdb.typing import BIGINT, DOUBLE, VARCHAR, BLOB, BOOLEAN, TIMESTAMP, TIMESTAMP_TZ

def timediff(f: str, utc: str):    
    return datetime.now()

def utc_timestamp():
    return 'America/Bogota'

mysql_native_functions = {
    "timediff": {
        "callback": timediff,
        'input': [TIMESTAMP_TZ, VARCHAR],
        'output': TIMESTAMP
    },
    "utc_timestamp": {
        'callback': utc_timestamp,
        'input': [],
        'output': VARCHAR
    }
}