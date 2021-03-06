import logging
import re
from collections import defaultdict
from datetime import datetime
from nose.tools import set_trace
from flask import url_for
from sqlalchemy.orm import lazyload

from core.app_server import cdn_url_for
from core.classifier import Classifier
from core.lane import Facets
from core.opds import (
    AcquisitionFeed,
    OPDSFeed,
    UnfulfillableWork,
    VerboseAnnotator,
)
from core.model import (
    Identifier,
    Resource,
    Session,
    Subject,
    Work,
    Edition,
    LicensePool,
)
from core.util import slugify

from config import Configuration

class ContentServerAnnotator(VerboseAnnotator):

    @classmethod
    def annotate_work_entry(cls, work, active_license_pool, edition, identifier, feed, entry):
        """Annotate the feed with all open-access links for this book."""
        if not active_license_pool.open_access:
            return

        rel = OPDSFeed.OPEN_ACCESS_REL
        fulfillable = False
        for resource in active_license_pool.open_access_links:
            if not resource.representation:
                continue
            url = resource.representation.mirror_url
            if not url:
                logging.warn(
                    "Problem with %r: open-access link %s not mirrored!", 
                    identifier,
                    resource.representation.url
                )
                continue
            type = resource.representation.media_type
            feed.add_link_to_entry(
                entry, rel=rel, href=url, type=type
            )

            fulfillable = True
        if not fulfillable:
            # This open-access work has no usable open-access links.
            # Don't show it in the OPDS feed.
            raise UnfulfillableWork()

        VerboseAnnotator.annotate_work_entry(
            work, active_license_pool, edition, identifier, feed, entry
        )

    @classmethod
    def default_lane_url(cls):
        return cdn_url_for("feed", _external=True)

    def top_level_title(self):
        return "All Books"

    def feed_url(self, lane, facets, pagination):
        kwargs = dict(facets.items())
        kwargs.update(dict(pagination.items()))
        if lane.license_source:
            view = "feed_from_license_source"
            kwargs['license_source_name'] = lane.license_source.name
        else:
            view = "feed"
            kwargs['lane'] = lane.name
            kwargs['languages'] = lane.languages
        return cdn_url_for(view, _external=True, **kwargs)


class AllCoverLinksAnnotator(ContentServerAnnotator):

    @classmethod
    def cover_links(cls, work):
        """The content server sends out _all_ cover links for the work.

        For books covered by Gutenberg Illustrated, this can be over a
        hundred cover links.
        """
        _db = Session.object_session(work)
        ids = work.all_identifier_ids()
        image_resources = Identifier.resources_for_identifier_ids(
            _db, ids, Resource.IMAGE)
        thumbnails = []
        full = []
        for cover in image_resources:
            if cover.mirrored_path:
                full.append(cover.mirrored_path)
            if cover.scaled_path:
                thumbnails.append(cover.scaled_path)
        return thumbnails, full


class StaticFeedAnnotator(ContentServerAnnotator):

    """An Annotator to work with static feeds generated via script"""

    TOP_LEVEL_LANE_NAME = u'All Books'
    HOME_FILENAME = u'index'

    # Feeds ordered by this facet will be considered the default.
    DEFAULT_ORDER = Facets.ORDER_TITLE

    DEFAULT_LANE_ORDER = [
        'Short Stories', 'Fiction', 'Nonfiction', 'Spanish', 'French'
    ]

    @classmethod
    def slugify_feed_title(cls, feed_title):
        return slugify(feed_title)

    def __init__(self, base_url, lane=None, prefix=None, include_search=None,
                 license_link=None):
        if not base_url.endswith('/'):
            base_url += '/'
        self.base_url = base_url
        self.prefix = prefix or ''
        self.lane = lane
        self.include_search = include_search
        self.license_link = license_link
        self.lanes_by_work = defaultdict(list)

    def reset(self, lane):
        self.lanes_by_work = defaultdict(list)
        self.lane = lane


    def default_lane_url(self):
        return self.base_url + self.prefix + self.HOME_FILENAME + '.xml'


    def search_url(self):
        return self.base_url + 'search'


    def lane_filename(self, lane=None):
        lane = lane or self.lane
        if lane.name == self.TOP_LEVEL_LANE_NAME:
            # This is the home lane.
            # Could be a COPPA navigation feed or an "All Books" lane
            # if a there aren't feeds for multiple age groups.
            return self.prefix + self.HOME_FILENAME

        if not lane.parent:
            # When there is a COPPA navigation feed, this creates a
            # filename for the top of the youth and adult feeds.
            return self.prefix + self.slugify_feed_title(lane.name)

        path = list()
        while lane.parent:
            path.insert(0, self.slugify_feed_title(lane.name))
            lane = lane.parent
        return self.prefix + '_'.join(path)


    def filename_facet_segment(self, facets):
        ordered_by = list(facets.items())[0][1]
        if ordered_by != self.DEFAULT_ORDER:
            return '_' + ordered_by
        return ''


    def facet_url(self, facets):
        """Incoporate order facets into filenames for static feeds"""
        if not self.lane:
            # Due to constraints in AcquisitionFeed, this method
            # is the only one that relies on a lane being set at
            # initialization.
            raise ValueError(
                "StaticFeedAnnotator can't create a facet URL without"\
                " a selected lane."
            )
        filename = self.lane_filename()
        filename += self.filename_facet_segment(facets)
        return self.base_url + filename + '.xml'


    def feed_url(self, lane, facets, pagination):
        """Incorporate pages into filenames for static feeds"""

        filename = self.lane_filename(lane)
        filename += self.filename_facet_segment(facets)

        page_number = (pagination.offset / pagination.size) + 1
        if page_number > 1:
            filename += ('_%i' % page_number)

        return self.base_url + filename + '.xml'


    def group_uri(self, work, license_pool, identifier):
        if not work in self.lanes_by_work:
            return None, ""

        lane = self.lanes_by_work[work][0]['lane']
        self.lanes_by_work[work] = self.lanes_by_work[work][1:]
        return self.lane_url(lane), lane.display_name


    def groups_url(self, lane):
        if lane:
            filename = self.lane_filename(lane)
        else:
            filename = self.HOME_FILENAME
        return self.base_url + filename + '.xml'


    def lane_url(self, lane):
        return self.groups_url(lane)


    def sort_works_for_groups_feed(self, works, lane_order=None):
        """Sorts lanes_by_work by the preferred list order of lanes in
        the feed
        """
        lane_order = lane_order or self.DEFAULT_LANE_ORDER

        lane_names = set([])
        for work, lanes in self.lanes_by_work.items():
            for lane_dict in lanes:
                lane = lane_dict['lane']
                lane_names.add(lane.name)

        missing_names = lane_names.difference(lane_order)
        for name in missing_names:
            # This is a lane that wasn't added to the preference
            # list. Add it to the end so it can be sorted against
            # without an error.
            lane_order.append(name)


        def sort_key(work):
            lanes = [l.get('lane') for l in self.lanes_by_work[work]]
            key = None
            if len(lanes) > 1:
                lanes_by_key = dict()
                for lane in lanes:
                    lane_key = lane_order.index(lane.name)
                    lanes_by_key[lane_key] = lane
                # Get the lowest key
                key = sorted(lanes_by_key)[0]
                lane = lanes_by_key[key]
            else:
                lane = lanes[0]
                key = lane_order.index(lane.name)

            if (lane.name.startswith('General ') or
                lane.name.startswith('All ')):
                # Force general lanes to the bottom of the feed.
                return 50
            return key
        return sorted(works, key=sort_key)

    def annotate_feed(self, feed, lane):
        if self.include_search:
            OPDSFeed.add_link_to_feed(
                feed.feed,
                rel='search',
                href=self.search_url(),
                type='application/opensearchdescription+xml')

        if self.license_link:
            OPDSFeed.add_link_to_feed(
                feed.feed,
                rel='license',
                href=self.license_link,
                type='text/html'
            )



class StaticFeedCOPPAAnnotator(StaticFeedAnnotator):

    TOP_LEVEL_LANE_NAME = u'Instant Classics'
    COPPA_RESTRICTION = u'http://librarysimplified.org/terms/restrictions/coppa'

    def add_gate(self, youth_lane, full_lane, feed_obj):
        details = {
            'restriction' : self.COPPA_RESTRICTION,
            'restriction-not-met' : self.lane_url(youth_lane),
            'restriction-met' : self.lane_url(full_lane)
        }

        gate_tag = OPDSFeed.makeelement(
            "{%s}gate" % OPDSFeed.SIMPLIFIED_NS, details
        )
        feed_obj.feed.append(gate_tag)


class StaticCOPPANavigationFeed(OPDSFeed):

    """Creates an OPDS navigation feed to guide between AcquisitionFeeds
    representing the full 13+ collection and the <13 childrens collection,
    in accordance with COPPA
    """

    @classmethod
    def content(cls, *args, **kwargs):
        kwargs['type'] = 'text'
        return cls.E.content(*args, **kwargs)

    @classmethod
    def childrens_entry(cls, lane_url):
        audience = Classifier.AUDIENCE_CHILDREN
        category_details = cls.audience_details(audience)
        return cls.entry(
            cls.id(lane_url),
            cls.title("I'm Under 13"),
            cls.updated(cls._strftime(datetime.utcnow())),
            cls.content("Read children's books"),
            cls.category(category_details)
        )

    @classmethod
    def full_collection_entry(cls, lane_url):
        audience = Classifier.AUDIENCE_ADULT
        category_details = cls.audience_details(audience)
        return cls.entry(
            cls.id(lane_url),
            cls.title("I'm 13 or Older"),
            cls.updated(cls._strftime(datetime.utcnow())),
            cls.content('See the full collection'),
            cls.category(category_details)
        )

    @classmethod
    def audience_details(cls, audience):
        return dict(
            term=audience, label=audience, scheme='%saudience' % cls.SCHEMA_NS
        )

    def __init__(self, title, base_url, youth_lane, full_lane, **kwargs):
        """Turn a list of lanes into a feed."""
        annotator = StaticFeedCOPPAAnnotator(base_url, **kwargs)
        lane_url = annotator.default_lane_url()

        super(StaticCOPPANavigationFeed, self).__init__(title, lane_url)

        self.create_entry(youth_lane, annotator, youth=True)
        self.create_entry(full_lane, annotator)
        annotator.add_gate(youth_lane, full_lane, self)

    def create_entry(self, lane, annotator, youth=False):
        annotator.reset(lane)
        lane_url = annotator.lane_url(lane)
        if youth:
            entry = self.childrens_entry(lane_url)
        else:
            entry = self.full_collection_entry(lane_url)

        link = dict(
            type=self.ACQUISITION_FEED_TYPE, href=lane_url, rel='subsection'
        )
        self.add_link_to_entry(entry, **link)

        self.feed.append(entry)
