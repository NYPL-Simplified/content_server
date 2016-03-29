from core.opds_import import OPDSImporterWithS3Mirror
from core.model import (
    Representation,
)
from copy import deepcopy
from nose.tools import set_trace

class UnglueItOPDSImporter(OPDSImporterWithS3Mirror):
    """Importer for unglue.it OPDS feed, which has acquisition links from multiple sources for some entries."""

    def import_from_feed(
            self, feed, even_if_no_author=True, 
            cutoff_date=None, immediately_presentation_ready=True
    ):
        # Override some of the provided arguments.
        super(UnglueItOPDSImporter, self).import_from_feed(
            feed, even_if_no_author=True, cutoff_date=cutoff_date,
            immediately_presentation_ready=True
        )

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
        
