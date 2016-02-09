from nose.tools import (
    set_trace,
    eq_,
)
import datetime
import tempfile
import urllib
from ..core.testing import DatabaseTest
from ..config import temp_config
from ..coverage import GutenbergEPUBCoverageProvider
from ..core.s3 import DummyS3Uploader
from ..core.coverage import CoverageFailure
from ..core.model import (
    get_one_or_create,
    DeliveryMechanism,
    Edition,
    Hyperlink,
    Identifier,
    LicensePool,
    Representation,
    Resource,
)

class DummyEPUBCoverageProvider(GutenbergEPUBCoverageProvider):

    def epub_path_for(self, identifier):
        if identifier.identifier.startswith('fail'):
            return CoverageFailure(self, identifier, "failure!", True)
        return "/oh/you/want/%s.epub" % identifier.identifier


class TestGutenbergEPUBCoverageProvider(DatabaseTest):

    def setup(self):
        super(TestGutenbergEPUBCoverageProvider, self).setup()
        self.provider = DummyEPUBCoverageProvider(
            self._db, mirror_uploader=DummyS3Uploader)


    def test_process_item_success(self):
        edition, pool = self._edition(with_license_pool=True)
        # Set aside the default delivery mechanism that got associated with
        # the license pool.
        [lpm1] = pool.delivery_mechanisms

        now = datetime.datetime.now()
        identifier = edition.primary_identifier

        # The coverage provider returned success.
        eq_(identifier, self.provider.process_item(identifier))

        # Something was 'uploaded' to S3.
        [representation] = self.provider.uploader.uploaded
        eq_('http://s3.amazonaws.com/test.content.bucket/Gutenberg%%20ID/%s.epub' % identifier.identifier, representation.mirror_url)
        assert representation.mirrored_at > now

        # The edition has now been linked to a resource.
        [link] = edition.primary_identifier.links
        eq_(identifier, link.identifier)
        eq_(edition.license_pool, link.license_pool)
        eq_(Hyperlink.OPEN_ACCESS_DOWNLOAD, link.rel)
        eq_(self.provider.output_source, link.data_source)

        # A new distribution mechanism has been added to the pool
        [lpm] = [x for x in link.license_pool.delivery_mechanisms if x != lpm1]
        mech = lpm.delivery_mechanism
        eq_(mech.content_type, Representation.EPUB_MEDIA_TYPE)
        eq_(mech.drm_scheme, DeliveryMechanism.NO_DRM)
        eq_(lpm.resource, link.resource)

        resource = link.resource
        representation = resource.representation
        eq_(Representation.EPUB_MEDIA_TYPE, representation.media_type)
        eq_(self.provider.epub_path_for(identifier), 
            representation.local_content_path)

    def test_epub_path_for_wrong_identifier_type(self):
        identifier = self._identifier(Identifier.OVERDRIVE_ID)
        real_provider = GutenbergEPUBCoverageProvider(
            self._db, mirror_uploader=DummyS3Uploader
        )
        failure = real_provider.epub_path_for(identifier)
        assert isinstance(failure, CoverageFailure)
        eq_('Not a Gutenberg book.', failure.exception)

    def test_epub_path_for_empty_directory(self):
        identifier = self._identifier(Identifier.GUTENBERG_ID)

        failure = None
        with temp_config() as config:
            config['data_directory'] = tempfile.gettempdir()
            real_provider = GutenbergEPUBCoverageProvider(
                self._db, mirror_uploader=DummyS3Uploader
            )
            failure = real_provider.epub_path_for(identifier)
        assert isinstance(failure, CoverageFailure)
        assert failure.exception.startswith('Expected EPUB directory')
        assert failure.exception.endswith('does not exist!')

    def test_process_item_failure_wrong_medium(self):
        edition, pool = self._edition(with_license_pool=True)
        edition.medium = Edition.VIDEO_MEDIUM
        failure = self.provider.process_item(edition.primary_identifier)
        eq_('Medium "Video" does not support EPUB', failure.exception)

    def test_process_item_failure(self):
        edition, pool = self._edition(with_license_pool=True)
        edition.primary_identifier.identifier = "fail1"
        failure = self.provider.process_item(edition.primary_identifier)
        eq_("failure!", failure.exception)

        # No resource has been created.
        eq_([], self._db.query(Hyperlink).all())
        eq_([], self._db.query(Resource).all())
        eq_([], self._db.query(Representation).all())

        # Nothing was uploaded to S3.
        eq_([], self.provider.uploader.uploaded)

    def test_best_epub_in(self):
        f = self.provider.best_epub_in
        eq_(None, f([]))
        eq_(None, f(["foo.txt", "bar.html"]))
        eq_("bar.epub", f(["foo.txt", "bar.epub"]))
        eq_("bar-noimages.epub", f(["foo.txt", "bar-noimages.epub"]))
        eq_("bar-images.epub", f(["foo-noimages.epub", "bar-images.epub"]))
