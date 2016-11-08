import datetime
import feedparser
from nose.tools import set_trace
from core.opds import OPDSFeed
from core.opds_import import OPDSImporter
from core.model import (
    Hyperlink,
    Resource,
    Representation,
)

class FeedbooksOPDSImporter(OPDSImporter):

    THIRTY_DAYS = datetime.timedelta(days=30)

    def extract_feed_data(self, feed, feed_url=None):
        metadata, failures = super(FeedbooksOPDSImporter, self).extract_feed_data(
            feed, feed_url
        )
        for id, metadata in metadata.items():
            self.improve_description(id, metadata)
        return metadata, failures
        
    def improve_description(self, id, metadata):
        """Improve the description associated with a book,
        if possible.

        This involves fetching an alternate OPDS entry that might
        contain more detailed descriptions than those available in the
        main feed.
        """
        alternate_links = []
        existing_descriptions = []
        everything_except_descriptions = []
        for x in metadata.links:
            if (x.rel == Hyperlink.ALTERNATE and x.href
                and x.media_type == OPDSFeed.ENTRY_TYPE):
                alternate_links.append(x)
            if x.rel == Hyperlink.DESCRIPTION:
                existing_descriptions.append((x.media_type, x.content))
            else:
                everything_except_descriptions.append(x)

        better_descriptions = []
        for alternate_link in alternate_links:
            # There should only be one alternate link, but we'll keep
            # processing them until we get a good description.

            # Fetch the alternate entry.
            representation, is_new = Representation.get(
                self._db, alternate_link.href, max_age=self.THIRTY_DAYS
            )

            # Parse the alternate entry with feedparser and run it through
            # data_detail_for_feedparser_entry().
            parsed = feedparser.parse(representation.content)
            if len(parsed['entries']) != 1:
                # This is supposed to be a single entry, and it's not.
                continue
            [entry] = parsed['entries']
            detail_id, new_detail, failure = self.data_detail_for_feedparser_entry(
                entry, None
            )

            # TODO: Ideally we could verify that detail_id == id, but
            # right now they are always different -- one is an HTTPS
            # URI and one is an HTTP URI. So we omit this step and
            # assume the 'alternate' links are correct.

            # Find any descriptions present in the alternate view which
            # are not present in the original.
            new_descriptions = [
                x for x in new_detail['links']
                if x.rel == Hyperlink.DESCRIPTION
                and (x.media_type, x.content) not in existing_descriptions
            ]

            if new_descriptions:
                # Replace old descriptions with new descriptions.
                metadata.links = (
                    everything_except_descriptions + new_descriptions
                )
        return metadata
