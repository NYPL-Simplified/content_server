from . import DatabaseTest
import re
from datetime import datetime
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

    def test_timestamp(self):
        item = {'name' : 'banana'}
        expected_format = re.compile('\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?Z')
        now = datetime.utcnow()

        # Adds a 'dateModified' timestamp to a dictionary / JSON object
        result = BibblioAPI.set_timestamp(item)
        assert 'dateModified' in result
        # It's a string.
        assert isinstance(result['dateModified'], basestring)
        assert result['dateModified'] > (now.isoformat() + 'Z')

        assert 'dateCreated' not in result

        # Adds 'dateModified' and 'dateCreated' timestamps when specified
        result = BibblioAPI.set_timestamp(item, create=True)
        assert 'dateCreated' in result
        assert 'dateModified' in result
        eq_(result['dateCreated'], result['dateModified'])
        assert isinstance(result['dateCreated'], basestring)
        assert result['dateCreated'] > (now.isoformat() + 'Z')
