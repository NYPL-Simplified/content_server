from collections import defaultdict
from nose.tools import set_trace
from flask import url_for

from core.opds import (
    Annotator,
    AcquisitionFeed,
    OPDSFeed,
)
from core.model import (
    Identifier,
    Resource,
    Session,
    Subject,
)


class ContentServerAnnotator(Annotator):

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

    @classmethod
    def categories(cls, work):
        """The content server sends out _all_ categories for the work."""
        _db = Session.object_session(work)
        by_scheme = defaultdict(list)
        identifier_ids = work.all_identifier_ids()
        classifications = Identifier.classifications_for_identifier_ids(
            _db, identifier_ids)
        for c in classifications:
            subject = c.subject
            if subject.type in Subject.uri_lookup:
                scheme = Subject.uri_lookup[subject.type]
                value = dict(term=subject.identifier)
                if subject.name:
                    value['label'] = subject.name
                by_scheme[scheme].append(value)
        return by_scheme
