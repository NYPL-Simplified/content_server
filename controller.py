from nose.tools import set_trace
import logging
import sys
import flask
from core.util.flask_util import languages_for_request

from core.problem_details import *
from core.util.problem_detail import ProblemDetail

from config import (
    Configuration,
    CannotLoadConfiguration,
)

from core.app_server import (
    feed_response,
    cdn_url_for,
    url_for,
    load_facets_from_request,
    load_pagination_from_request,
    load_facets,
    load_pagination,
    HeartbeatController,
)

from core.model import (
    production_session,
)
from core.lane import (
    Lane,
    Facets,
    Pagination,
)
from core.opds import AcquisitionFeed
from opds import (
    ContentServerAnnotator,
    PreloadFeed,
)


class ContentServer(object):
    
    def __init__(self, _db=None, testing=False):

        self.log = logging.getLogger("Content server web app")

        try:
            self.config = Configuration.load()
        except CannotLoadConfiguration, e:
            self.log.error("Could not load configuration file: %s" %e)
            sys.exit()

        if _db is None and not testing:
            _db = production_session()
        self._db = _db

        self.testing = testing

        self.setup_controllers()

    def setup_controllers(self):
        """Set up all the controllers that will be used by the web app."""
        self.opds_feeds = OPDSFeedController(self)

        self.heartbeat = HeartbeatController()


class ContentServerController(object):
    
    def __init__(self, content_server):
        self.content_server = content_server
        self._db = self.content_server._db

    def annotator(self, *args, **kwargs):
        """Create an appropriate OPDS annotator."""
        return ContentServerAnnotator(*args, **kwargs)


class OPDSFeedController(ContentServerController):
    
    def feed(self):
        lane = Lane(self._db, "All books")
        
        url = url_for("feed", _external=True)

        facets = load_facets_from_request(Configuration)
        if isinstance(facets, ProblemDetail):
            return facets
        pagination = load_pagination_from_request()
        if isinstance(pagination, ProblemDetail):
            return pagination

        opds_feed = AcquisitionFeed.page(
            self._db, "Open-Access Content", url, lane,
            annotator=self.annotator(),
            facets=facets,
            pagination=pagination,
        )
        return feed_response(opds_feed.content) 

    def preload(self):
        url = url_for("preload", _external=True)

        opds_feed = PreloadFeed.page(
            self._db, "Content to Preload", url,
            annotator=self.annotator(),
        )
        return feed_response(opds_feed)
        
