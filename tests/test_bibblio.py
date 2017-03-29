from . import DatabaseTest
from nose.tools import eq_, set_trace

from ..config import (
    Configuration,
    temp_config,
)

from ..bibblio import (
    BibblioAPI,
)

class TestBibblioAPI(DatabaseTest):

    def test_from_config(self):
        # When nothing has been configured, nothing is returned.
        with temp_config() as config:
            config['integrations'][Configuration.BIBBLIO_INTEGRATION] = {}
            result = BibblioAPI.from_config(self._db)
            eq_(None, result)

        # When there's only a partial configuration, None is returned.
        with temp_config() as config:
            config['integrations'][Configuration.BIBBLIO_INTEGRATION] = {
                Configuration.BIBBLIO_ID : 'id'
            }
            result = BibblioAPI.from_config(self._db)
            eq_(None, result)

        with temp_config() as config:
            config['integrations'][Configuration.BIBBLIO_INTEGRATION] = {
                Configuration.BIBBLIO_ID : 'id',
                Configuration.BIBBLIO_SECRET : 'secret'
            }
            result = BibblioAPI.from_config(self._db)
            eq_(True, isinstance(result, BibblioAPI))
            eq_('id', result.client_id)
            eq_('secret', result.client_secret)
