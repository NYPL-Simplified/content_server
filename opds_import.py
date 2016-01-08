from core.opds_import import OPDSImporter
from core.s3 import S3Uploader
from core.model import (
    Representation,
    Hyperlink,
    DataSource,
)
from urllib import urlopen
from copy import deepcopy
from nose.tools import set_trace

class ContentOPDSImporter(OPDSImporter):
    """OPDS Importer that mirrors content to S3."""

    def import_from_feed(self, feed):
        imported, messages, next_links = super(ContentOPDSImporter, self).import_from_feed(feed)

        # Mirror books and images to S3 for the imported editions
        uploader = S3Uploader()
        for edition in imported:
            links = edition.primary_identifier.links
            for link in links:
                representation = link.resource.representation
                media_type = representation.media_type
                original_url = link.resource.url

                mirror_url = None
                if media_type in Representation.SUPPORTED_BOOK_MEDIA_TYPES:
                    mirror_url = uploader.book_url(edition.primary_identifier, "epub")
                elif link.rel == Hyperlink.THUMBNAIL_IMAGE or link.rel == Hyperlink.IMAGE:
                    data_source = DataSource.lookup(self._db, self.data_source_name)
                    mirror_url = uploader.cover_image_url(data_source, edition.primary_identifier, original_url.split("/")[-1])
               
                if mirror_url:
                    try:
                        representation.mirror_url = mirror_url
                        handle = urlopen(original_url)
                        content = handle.read()
                        handle.close()
                        representation.content = content
                        uploader.mirror_one(representation)
                        representation.content = None
                        edition.work.set_presentation_ready()
                    except IOError as e:
                        print "Unable to mirror %s" % original_url
                        representation.mirror_url = None

        return imported, messages, next_links


class UnglueItOPDSImporter(ContentOPDSImporter):
    """Importer for unglue.it OPDS feed, which has acquisition links from multiple sources for some entries."""

    def extract_metadata(self, feed):
        metadata = []

        entry_metadata_objs, status_messages, next_links = super(UnglueItOPDSImporter, self).extract_metadata(feed)

        for metadata_obj in entry_metadata_objs:
            book_links = []
            other_links = []
            
            for link in metadata_obj.links:
                if link.media_type in Representation.SUPPORTED_BOOK_MEDIA_TYPES:
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
        
