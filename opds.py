from collections import defaultdict
from nose.tools import set_trace
from flask import url_for
from sqlalchemy.orm import lazyload

from core.app_server import cdn_url_for
from core.opds import (
    VerboseAnnotator,
    AcquisitionFeed,
    OPDSFeed,
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
        for resource in active_license_pool.open_access_links:
            if not resource.representation:
                continue
            url = resource.representation.mirror_url
            type = resource.representation.media_type
            feed.add_link_to_entry(
                entry, rel=rel, href=url, type=type
            )

    @classmethod
    def default_lane_url(cls):
        return cdn_url_for("feed", _external=True)

    def feed_url(self, lane, facets, pagination):
        kwargs = dict(facets.items())
        kwargs.update(dict(pagination.items()))
        return cdn_url_for(
            "feed", lane_name=lane.name, languages=lane.languages, _external=True, **kwargs)

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


class PreloadFeed(AcquisitionFeed):

    @classmethod
    def page(cls, _db, title, url, annotator=None,
             use_materialized_works=True):

        """Create a feed of content to preload on devices."""
        configured_content = Configuration.get(Configuration.PRELOADED_CONTENT)

        identifiers = [Identifier.parse_urn(_db, urn)[0] for urn in configured_content]
        identifier_ids = [identifier.id for identifier in identifiers]

        if use_materialized_works:
            from core.model import MaterializedWork
            q = _db.query(MaterializedWork)
            q = q.filter(MaterializedWork.primary_identifier_id.in_(identifier_ids))

            # Avoid eager loading of objects that are contained in the 
            # materialized view.
            q = q.options(
                lazyload(MaterializedWork.license_pool, LicensePool.data_source),
                lazyload(MaterializedWork.license_pool, LicensePool.identifier),
                lazyload(MaterializedWork.license_pool, LicensePool.edition),
            )
        else:
            q = _db.query(Work).join(Work.primary_edition)
            q = q.filter(Edition.primary_identifier_id.in_(identifier_ids))

        works = q.all()
        feed = cls(_db, title, url, works, annotator)

        annotator.annotate_feed(feed, None)
        content = unicode(feed)
        return content
                     
