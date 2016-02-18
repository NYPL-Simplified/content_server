from core.opds_import import OPDSImporter
from core.s3 import S3Uploader
from core.model import (
    Representation,
    Hyperlink,
    DataSource,
    UnresolvedIdentifier,
)
import requests
from urllib import urlopen
from copy import deepcopy
from nose.tools import set_trace

class ContentOPDSImporter(OPDSImporter):
    """OPDSImporter that sets up an UnresolvedIdentifier for every new
    edition.
    """

    def import_from_feed(self, feed, even_if_no_author=False, cutoff_date=None):
        imported, messages, next_links = super(
            ContentOPDSImporter, self
        ).import_from_feed(
            feed, 
            even_if_no_author=even_if_no_author, 
            cutoff_date=cutoff_date
        )

        # Add an UnresolvedIdentifier for every imported edition.
        for edition in imported:
            UnresolvedIdentifier.register(
                self._db, 
                edition.primary_identifier, 
                force=True
            )

        return imported, messages, next_links


class UnglueItOPDSImporter(ContentOPDSImporter):
    """Importer for unglue.it OPDS feed, which has acquisition links from multiple sources for some entries."""

    def import_from_feed(self, feed, even_if_no_author=True, cutoff_date=None):
        return super(UnglueItOPDSImporter, self).import_from_feed(
            feed, even_if_no_author=True, cutoff_date=cutoff_date
        )

    def extract_metadata(self, feed):
        metadata = []

        entry_metadata_objs, status_messages, next_links = super(
            UnglueItOPDSImporter, self
        ).extract_metadata(feed)

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
        
