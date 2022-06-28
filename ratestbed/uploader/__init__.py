import os
from rf_core.datastore.common.dbconn import DBConn

__all__ = [
    'get_conn',
    'use_extra'
]

DB_URI_MAP = {
    'invest': os.getenv('RF_DB_URI'),
    'local': os.getenv('LOCAL_DB_URI')
}


def get_conn(tag):
    assert tag in DB_URI_MAP
    return DBConn(DB_URI_MAP[tag])


def use_extra():
    return os.getenv('USE_EXTRA') == '1'