from nose.tools import set_trace
import logging
import sys
import flask
from core.util.flask_util import languages_for_request

from problem_details import *

from config import (
    Configuration,
    CannotLoadConfiguration,
)

from core.app_server import (
    feed_response,
    cdn_url_for,
    url_for,
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
from opds import ContentServerAnnotator


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

    def load_facets_from_request(self):
        """Figure out which Facets object this request is asking for."""
        arg = flask.request.args.get

        g = Facets.ORDER_FACET_GROUP_NAME
        order = arg(g, Configuration.default_facet(g))

        g = Facets.AVAILABILITY_FACET_GROUP_NAME
        availability = arg(g, Configuration.default_facet(g))

        g = Facets.COLLECTION_FACET_GROUP_NAME
        collection = arg(g, Configuration.default_facet(g))
        return self.load_facets(order, availability, collection)

    def load_pagination_from_request(self):
        """Figure out which Pagination object this request is asking for."""
        arg = flask.request.args.get
        size = arg('size', Pagination.DEFAULT_SIZE)
        offset = arg('after', 0)
        return self.load_pagination(size, offset)

    @classmethod
    def load_facets(self, order, availability, collection):
        """Turn user input into a Facets object."""
        order_facets = Configuration.enabled_facets(
            Facets.ORDER_FACET_GROUP_NAME
        )
        if order and not order in order_facets:
            return INVALID_INPUT.detailed(
                "I don't know how to order a feed by '%s'" % order,
                400
            )
        availability_facets = Configuration.enabled_facets(
            Facets.AVAILABILITY_FACET_GROUP_NAME
        )
        if availability and not availability in availability_facets:
            return INVALID_INPUT.detailed(
                "I don't understand the availability term '%s'" % availability,
                400
            )

        collection_facets = Configuration.enabled_facets(
            Facets.COLLECTION_FACET_GROUP_NAME
        )
        if collection and not collection in collection_facets:
            return INVALID_INPUT.detailed(
                "I don't understand which collection '%s' refers to." % collection,
                400
            )
        return Facets(
            collection=collection, availability=availability, order=order
        )

    @classmethod
    def load_pagination(self, size, offset):
        """Turn user input into a Pagination object."""
        try:
            size = int(size)
        except ValueError:
            return INVALID_INPUT.detailed("Invalid size: %s" % size)
        size = min(size, 100)
        if offset:
            try:
                offset = int(offset)
            except ValueError:
                return INVALID_INPUT.detailed("Invalid offset: %s" % offset)
        return Pagination(offset, size)


    def annotator(self, *args, **kwargs):
        """Create an appropriate OPDS annotator."""
        return ContentServerAnnotator(*args, **kwargs)


class OPDSFeedController(ContentServerController):
    
    def feed(self):
        lane = Lane(self._db, "All books")
        
        url = url_for("feed", _external=True)

        opds_feed = AcquisitionFeed.page(
            self._db, "Open-Access Content", url, lane,
            annotator=self.annotator(),
            facets=self.load_facets_from_request(),
            pagination=self.load_pagination_from_request()
        )
        return feed_response(opds_feed.content) 
