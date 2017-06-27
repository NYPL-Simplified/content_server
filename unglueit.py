from nose.tools import set_trace
import requests
import urlparse
from core.model import (
    Representation
)
from core.opds_import import OPDSImporterWithS3Mirror

class UnglueItImporter(OPDSImporterWithS3Mirror):

    @classmethod
    def collection_data(cls):
        return dict(url=u'https://unglue.it/api/opds/epub/')

    def _check_for_gutenberg_first(self, url, headers, **kwargs):
        """Make a HEAD request for the given URL to make sure
        it doesn't redirect to gutenberg.org.
        """
        parsed = urlparse.urlparse(url)
        if parsed.netloc.endswith('unglue.it'):
            # It might be a redirect. Make a HEAD request to see where
            # it leads.
            head_response = requests.head(url, headers=headers)
            if head_response.status_code / 100 == 3:
                # Yes, it's a redirect.
                location = head_response.headers.get('location')
                if location:
                    parsed = urlparse.urlparse(location)
                    if parsed.netloc.endswith('gutenberg.org'):
                        # If we make this request we're going to be in
                        # for some trouble, and we won't even get
                        # anything useful. Act as though we got an
                        # unappetizing representation.
                        self.log.info("Not making request to gutenberg.org.")
                        return (
                            200, 
                            {"content-type" :
                             "application/vnd.librarysimplified-clickthrough"},
                            "Gated behind Gutenberg click-through"
                        )
        return Representation.simple_http_get(url, headers, **kwargs)

    def __init__(self, _db, collection, **kwargs):
        kwargs['http_get'] = self._check_for_gutenberg_first
        super(UnglueItImporter, self).__init__(
            _db, collection, **kwargs
        )
