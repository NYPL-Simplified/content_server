from collections import defaultdict
from nose.tools import set_trace
from flask import url_for
from sqlalchemy.orm import lazyload
import logging

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
