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
    CustomList,
    DataSource,
    Library
)
from core.lane import (
    Lane,
    Facets,
    Pagination,
)
from core.opds import AcquisitionFeed
from opds import (
    ContentServerAnnotator,
)


class ContentServer(object):
    
    def __init__(self, _db=None, testing=False):

        self.log = logging.getLogger("Content server web app")

        try:
            self.config = Configuration.load(_db)
        except CannotLoadConfiguration, e:
            self.log.error("Could not load configuration file: %s" %e)
            sys.exit()

        if _db is None and not testing:
            _db = production_session()
            Configuration.load(_db)
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
    
    def feed(self, license_source_name=None):
        if license_source_name:
            license_source = DataSource.lookup(self._db, license_source_name)
            if not license_source:
                return UNRECOGNIZED_DATA_SOURCE.detailed(
                    "Unrecognized license source: %s" % license_source_name
                )
            lane_name = "All books from %s" % license_source.name
        else:
            lane_name = "All books"
            license_source=None

        library = Library.default(self._db)
        lane = Lane(library, lane_name, license_source=license_source)

        url = url_for("feed", _external=True)

        flask.request.library = library
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

    def custom_list_feed(self, list_identifier):
        """Creates an OPDS feed with the Works from a CustomList.

        :param list_identifier: a basestring representing either the
            name or the foreign_identifier of an existing CustomList

        :return: an OPDS feed or a ProblemDetail
        """
        # Right now we only allow downloading of staff-created lists.
        source = DataSource.LIBRARY_STAFF
        custom_list = CustomList.find(self._db, source, list_identifier)

        if not custom_list:
            return INVALID_INPUT.detailed(
                "Available CustomList '%s' not found." % list_identifier
            )

        lane_name = 'All books from %s' % custom_list.name
        lane = Lane(
            self._db, lane_name,
            list_identifier=custom_list.foreign_identifier,
        )

        url = url_for(
            'feed_from_custom_list',
            list_identifier=custom_list.foreign_identifier,
            _external=True
        )

        facets = load_facets_from_request(Configuration)
        if isinstance(facets, ProblemDetail):
            return facets
        pagination = load_pagination_from_request()
        if isinstance(pagination, ProblemDetail):
            return pagination

        custom_list_feed = AcquisitionFeed.page(
            self._db, lane_name, url, lane,
            annotator=self.annotator(),
            facets=facets,
            pagination=pagination,
        )
        return feed_response(custom_list_feed.content)
