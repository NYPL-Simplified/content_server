import json
import os
import subprocess
import tempfile
import urllib
from nose.tools import set_trace
from config import Configuration
from core.coverage import (
    CoverageProvider,
    CoverageFailure,
)
from core.model import (
    get_one,
    DataSource,
    DeliveryMechanism,
    Edition,
    Hyperlink,
    Identifier,
    LicensePool,
    Representation,
    Resource,
)
from core.s3 import S3Uploader

class GutenbergEPUBCoverageProvider(CoverageProvider):
    """Upload a text's epub to S3.

    Eventually this will generate the epub from scratch before
    uploading it.
    """

    def __init__(self, _db, workset_size=5, mirror_uploader=S3Uploader):
        data_directory = Configuration.data_directory()

        if data_directory:
            self.gutenberg_mirror = os.path.join(
                data_directory, "Gutenberg", "gutenberg-mirror") + "/"
            self.epub_mirror = os.path.join(
                data_directory, "Gutenberg", "gutenberg-epub") + "/"
        else:
            self.gutenberg_mirror = None
            self.epub_mirror = None

        input_source = DataSource.lookup(_db, DataSource.GUTENBERG)
        self.output_source = DataSource.lookup(
            _db, DataSource.GUTENBERG_EPUB_GENERATOR)        
        if callable(mirror_uploader):
            mirror_uploader = mirror_uploader()
        self.uploader = mirror_uploader

        super(GutenbergEPUBCoverageProvider, self).__init__(
            self.output_source.name, input_source, self.output_source,
            workset_size=workset_size)

    def process_item(self, identifier):
        edition = self.edition(identifier)
        if isinstance(edition, CoverageFailure):
            return edition
        if edition.medium in (Edition.AUDIO_MEDIUM, Edition.VIDEO_MEDIUM):
            # There is no epub to mirror.
            return CoverageFailure(
                self, identifier, 
                'Medium "%s" does not support EPUB' % edition.medium,
                transient=False,
            )
        epub_path = self.epub_path_for(identifier)
        if isinstance(epub_path, CoverageFailure):
            return epub_path
        license_pool = edition.license_pool
        if not edition.license_pool:
            return CoverageFailure(
                self, identifier,
                'No license pool for %r', edition,
                transient=True,
            )

        url = self.uploader.book_url(identifier, 'epub')
        link, new = license_pool.add_link(
            Hyperlink.OPEN_ACCESS_DOWNLOAD, url, self.output_source,
            Representation.EPUB_MEDIA_TYPE, None, epub_path
        )
        representation = link.resource.representation
        representation.mirror_url = url
        self.uploader.mirror_one(representation)

        license_pool.set_delivery_mechanism(
            Representation.EPUB_MEDIA_TYPE, DeliveryMechanism.NO_DRM, 
            link.resource
        )
        return identifier

    def epub_path_for(self, identifier):
        """Find the path to the best EPUB for the given identifier."""
        if identifier.type != Identifier.GUTENBERG_ID:
            return CoverageFailure(
                self, identifier, "Not a Gutenberg book.", transient=False
            )
        epub_directory = os.path.join(
            self.epub_mirror, identifier.identifier
        )
        if not os.path.exists(epub_directory):
            return CoverageFailure(
                self, identifier,
                "Expected EPUB directory %s does not exist!" % epub_directory,
                transient=True,
            )

        files = os.listdir(epub_directory)
        epub_filename = self.best_epub_in(files)
        if not epub_filename:
            return CoverageFailure(
                self, identifier,
                "Could not find a good EPUB in %s!", epub_directory,
                transient=True
            )
        return os.path.join(epub_directory, epub_filename)

    @classmethod
    def best_epub_in(cls, files):
        """Find the best EPUB in the given file list."""
        without_images = None
        with_images = None
        for i in files:
            if not i.endswith('.epub'):
                continue
            if i.endswith('-images.epub'):
                with_images = i
                break
            elif not without_images:
                without_images = i
        return with_images or without_images
