from nose.tools import set_trace

from ..core.testing import (
    DatabaseTest,
    _setup,
    _teardown,
)

class ContentDBInfo(object):
    connection = None
    engine = None
    transaction = None

DatabaseTest.DBInfo = ContentDBInfo

def setup():
    _setup(ContentDBInfo)

def teardown():
    _teardown(ContentDBInfo)

