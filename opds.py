import logging
import re
from collections import defaultdict
from nose.tools import set_trace
from flask import url_for
from sqlalchemy.orm import lazyload

from core.app_server import cdn_url_for
from core.opds import (
    VerboseAnnotator,
    AcquisitionFeed,
    OPDSFeed,
    UnfulfillableWork,
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

    @classmethod
    def slugify_feed_title(cls, feed_title):
        slug = re.sub('[.!@#\'$,]', '', feed_title.lower())
        slug = re.sub('&', ' and ', slug)
        slug = re.sub(' {2,}', ' ', slug)
        return unicode('-'.join(slug.split(' ')))

    @classmethod
    def lane_filename(cls, lane):
        if lane.name == cls.TOP_LEVEL_LANE_NAME:
            # This is the home lane.
            return cls.HOME_FILENAME

        path = list()
        while lane.parent:
            path.insert(0, cls.slugify_feed_title(lane.name))
            lane = lane.parent
        return '_'.join(path)

    def __init__(self, base_url, lane, default_order=None, search_link=None):
        self.default_order = default_order
        self.base_url = base_url
        self.lane = lane
        self.search_link = search_link

        self.lanes_by_work = defaultdict(list)

    def default_lane_url(self):
        return self.base_url + '/' + self.HOME_FILENAME + '.opds'

    def filename_facet_segment(self, facets):
        ordered_by = list(facets.items())[0][1]
        if ordered_by != self.default_order:
            return '_' + ordered_by
        return ''

    def facet_url(self, facets):
        """Incoporate order facets into filenames for static feeds"""
        filename = self.lane_filename(self.lane)
        filename += self.filename_facet_segment(facets)
        return self.base_url + '/' + filename + '.opds'

    def feed_url(self, lane, facets, pagination):
        """Incorporate pages into filenames for static feeds"""

        filename = self.lane_filename(lane)
        filename += self.filename_facet_segment(facets)

        page_number = (pagination.offset / pagination.size) + 1
        if page_number > 1:
            filename += ('_%i' % page_number)

        return self.base_url + '/' + filename + '.opds'

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
            filename = self.home_filename
        return self.base_url + '/' + filename + '.opds'

    def lane_url(self, lane):
        return self.groups_url(lane)

    def annotate_feed(self, feed, lane):
        if self.search_link:
            OPDSFeed.add_link_to_feed(
                feed.feed,
                rel="search",
                href=self.search_link,
                type="application/opensearchdescription+xml")
                
