from collections import defaultdict
from nose.tools import set_trace
from flask import url_for

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
)

class ContentServerAnnotator(VerboseAnnotator):

    @classmethod
    def annotate_work_entry(cls, work, active_license_pool, edition, identifier, feed, entry):
        if not active_license_pool.open_access:
            return

        rel = OPDSFeed.OPEN_ACCESS_REL
        best_pool, best_link = active_license_pool.best_license_link
        feed.add_link_to_entry(entry, rel=rel, href=best_link.representation.mirror_url,
                               type=best_link.representation.media_type)


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
