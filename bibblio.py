from datetime import datetime, timedelta
from nose.tools import set_trace

from config import Configuration

from core.model import (
    Credential,
    DataSource,
)
from core.util.http import HTTP


class BibblioAPI(object):

    API_ENDPOINT = u"https://api.bibblio.org/v1/"
    
    TOKEN_CONTENT_TYPE = u"application/x-www-form-urlencoded"
    TOKEN_TYPE = u"Bearer "

    @classmethod
    def from_config(cls, _db):
        config = Configuration.integration(Configuration.BIBBLIO_INTEGRATION)
        if not config and len(config.values()) == 2:
            return None

        client_id = config.get(Configuration.BIBBLIO_ID)
        client_secret = config.get(Configuration.BIBBLIO_SECRET)

        return cls(_db, client_id, client_secret)

    def __init__(self, _db, client_id, client_secret):
        self._db = _db
        self.client_id = client_id
        self.client_secret = client_secret
        self._credential = None

    @property
    def source(self):
        return DataSource.lookup(self._db, DataSource.BIBBLIO)

    @property
    def token(self):
        if (self._credential and
            self._credential.expires <= datetime.utcnow()):
            return self._credential.credential

        credential = Credential.lookup(
            self._db, self.source, None, None, self.refresh_credential
        )
        return credential.credential

    def refresh_credential(self, credential):
        url = self.API_ENDPOINT + 'token'
        headers = {'Content-Type' : self.TOKEN_CONTENT_TYPE}
        payload = dict(client_id=self.client_id, client_secret=self.client_secret)

        response = HTTP.post_with_timeout(url, payload, headers=headers)
        data = response.json()

        credential.credential = data.get('access_token')
        expires_in = data.get('expires_in')
        credential.expires = datetime.utcnow() + timedelta(0, expires_in * 0.9)
        self._credential = credential
