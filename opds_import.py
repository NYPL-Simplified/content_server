from core.opds_import import OPDSImporter
from core.s3 import S3Uploader
from core.model import (
    Representation,
    Hyperlink,
    DataSource,
)
from copy import deepcopy
from nose.tools import set_trace

class ContentOPDSImporter(OPDSImporter):
    """OPDS Importer that mirrors content to S3."""

    def __init__(self, *args, **kwargs):
        super(ContentOPDSImporter, self).__init__(
            *args, mirror=S3Uploader(), **kwargs
        )

class UnglueItOPDSImporter(ContentOPDSImporter):
    """Importer for unglue.it OPDS feed, which has acquisition links from multiple sources for some entries."""

    def import_from_feed(self, feed):
        return super(UnglueItOPDSImporter, self).import_from_feed(feed, even_if_no_author=True)

    def extract_metadata(self, feed):
        metadata = []

        entry_metadata_objs, status_messages, next_links = super(UnglueItOPDSImporter, self).extract_metadata(feed)

        for metadata_obj in entry_metadata_objs:
            book_links = []
            other_links = []
            
            for link in metadata_obj.links:
                if link.media_type in Representation.BOOK_MEDIA_TYPES:
                    book_links.append(deepcopy(link))
                else:
                    other_links.append(deepcopy(link))

            if len(book_links) > 1:
                # Create a different metadata object for each book link
                for link in book_links:
                    metadata_copy = deepcopy(metadata_obj)
                    metadata_copy.links = [link] + other_links
                    metadata_copy.rights_uri = link.rights_uri
                    metadata.append(metadata_copy)
            else:
                metadata.append(metadata_obj)

        return metadata, status_messages, next_links
        
