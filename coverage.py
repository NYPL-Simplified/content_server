import json
import os
import re
import requests
import subprocess
import tempfile
import urllib
import urlparse
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

class MetadataWranglerCoverageProvider(CoverageProvider):
    """Ask the metadata wrangler about the primary identifier
    for every book identified by ISBN.
    """

class UnglueItMirror(CoverageProvider):
    """
    Mirror the EPUB and cover image for every unglue.it book, or
    create a permanent failure explaining why that's impossible.
    """

    PROCESS_RELS = set([Hyperlink.IMAGE, Hyperlink.OPEN_ACCESS_DOWNLOAD])
    GENERIC_FILENAME = "cover"
    GOOGLE_BOOKS_BOOK_ID_RE = re.compile("id=([^&]+)")

    def __init__(self, _db, mirror_uploader = S3Uploader):
        data_source = DataSource.lookup(_db, DataSource.UNGLUE_IT)
        super(UnglueItMirror, self).__init__(
            "Unglue.it resource mirror", Identifier.URI,
            data_source, workset_size=1
        )
        if callable(mirror_uploader):
            mirror_uploader = mirror_uploader()
        self.uploader = mirror_uploader

    def process_item(self, identifier):
        hostname = urlparse.urlparse(identifier.identifier).netloc
        if hostname != 'unglue.it':
            self.log.debug("%s is not an Unglue.it URL, ignoring.")
            return identifier

        links = identifier.links

        # For coverage to succeed we must successfully mirror an open-access
        # download. It's nice if we also mirror a cover, but not required.
        image_success = False
        book_success = False
        try:
            for link in links:
                if link.rel == Hyperlink.IMAGE:
                    image_success = (
                        image_success or self.download_cover_image(
                            identifier, link)
                    )
                elif link.rel == Hyperlink.OPEN_ACCESS_DOWNLOAD:
                    book_success = book_success or self.download_book(
                        identifier, link
                    )
        except Exception, e:
            return CoverageFailure(self, identifier, e, transient=True)

        if not book_success:
            return CoverageFailure(
                self, identifier, "Could not find a readable book.", 
                transient=False
            )

    def image_filename(self, original_url, purported_media_type):
        """Determine a good filename for our mirror of the given URL."""
        extension = Representation.FILE_EXTENSIONS.get(
            purported_media_type or Representation.PNG_MEDIA_TYPE
        )

        parsed = urlparse.urlparse(original_url)
        m = self.GOOGLE_BOOKS_BOOK_ID_RE.search(parsed.query)
        if m:
            filename = m.groups()[0] + "." + extension
        else:
            path = parsed.path
            filename = os.path.split(path)[-1]
        if not '.' in filename:
            filename = self.GENERIC_FILENAME + "." + extension
        return filename

    def download_cover_image(self, identifier, link):
        representation = link.resource.representation
        original_url = link.resource.url
        filename = self.image_filename(original_url, representation.media_type)
        mirror_url = self.uploader.cover_image_url(
            self.output_source, identifier, filename
        )
        success = self.mirror_if_existing(
            representation, original_url, mirror_url, 
            Representation.IMAGE_MEDIA_TYPES,
        )
        if not success:
            # There is no such image to be found.
            return False

        # Scale the image.
        DEFAULT_WIDTH = 200
        DEFAULT_HEIGHT = 300
        destination_url = self.uploader.cover_image_url(
            self.output_source, identifier, "cover.jpg", DEFAULT_HEIGHT
        )
        thumbnail, is_new = representation.scale(
            DEFAULT_HEIGHT, DEFAULT_WIDTH,
            destination_url, "image/jpeg", force=True
        )
        self.uploader.mirror_one(thumbnail)
        return True

    def download_book(self, identifier, link):
        representation = link.resource.representation
        original_url = link.resource.url
        purported_media_type = representation.media_type
        extension = Representation.FILE_EXTENSIONS.get(
            purported_media_type or Representation.EPUB_MEDIA_TYPE
        )
        edition = identifier.primarily_identifies[0]
        mirror_url = self.uploader.book_url(
            identifier, extension=extension, title=edition.title
        )
        return self.mirror_if_existing(
            representation, original_url, mirror_url,
            Representation.BOOK_MEDIA_TYPES
        )

    def mirror_if_existing(
            self, representation, original_url, mirror_url, valid_types
    ):
        """Retrieve `original_url`. If it looks good (we get one of the
        expected media types), mirror the content to `mirror_url`.
        """
        headers = {
            "User-Agent": Representation.BROWSER_USER_AGENT
        }

        response = requests.get(original_url, headers=headers)

        if response.status_code != 200:
            representation.mirror_exception = (
                "Bad status code: %s" % response.status_code
            )
            return False

        content_type = response.headers.get('content-type')
        if content_type in valid_types:
            representation.content = response.content
            representation.mirror_url = mirror_url
            self.uploader.mirror_one(representation)
            return True
        else:
            representation.mirror_exception = (
                "Unapproved media type: %s" % content_type
            )
            return False

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

        self.output_source = DataSource.lookup(
            _db, DataSource.GUTENBERG_EPUB_GENERATOR)        
        if callable(mirror_uploader):
            mirror_uploader = mirror_uploader()
        self.uploader = mirror_uploader

        super(GutenbergEPUBCoverageProvider, self).__init__(
            self.output_source.name, Identifier.GUTENBERG_ID, 
            self.output_source,
            workset_size=workset_size
        )

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
                'No license pool for %r' % edition,
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
