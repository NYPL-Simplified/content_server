from nose.tools import (
    set_trace,
    eq_,
)
import datetime
import urllib
from ..core.testing import DatabaseTest

from ..coverage import GutenbergEPUBCoverageProvider
from ..core.s3 import DummyS3Uploader
from ..core.model import (
    DeliveryMechanism,
    Hyperlink,
    Representation,
    Resource,
)

class DummyEPUBCoverageProvider(GutenbergEPUBCoverageProvider):

    def epub_path_for(self, identifier):
        if identifier.identifier.startswith('fail'):
            return None
        return "/oh/you/want/%s.epub" % identifier.identifier


class TestGutenbergEPUBCoverageProvider(DatabaseTest):

    def setup(self):
        super(TestGutenbergEPUBCoverageProvider, self).setup()
        self.provider = DummyEPUBCoverageProvider(
            self._db, mirror_uploader=DummyS3Uploader)

    def test_process_edition_success(self):
        edition, pool = self._edition(with_license_pool=True)

        now = datetime.datetime.now()

        eq_(True, self.provider.process_edition(edition))

        # Something was 'uploaded' to S3.
        [representation] = self.provider.uploader.uploaded
        identifier = edition.primary_identifier
        eq_('http://s3.amazonaws.com/test.content.bucket/Gutenberg%%20ID/%s.epub' % identifier.identifier, representation.mirror_url)
        assert representation.mirrored_at > now

        # The edition has now been linked to a resource.
        [link] = edition.primary_identifier.links
        eq_(identifier, link.identifier)
        eq_(pool, link.license_pool)
        eq_(Hyperlink.OPEN_ACCESS_DOWNLOAD, link.rel)
        eq_(self.provider.output_source, link.data_source)

        # The license pool has a distribution mechanism.
        [lpm] = link.license_pool.delivery_mechanisms
        mech = lpm.delivery_mechanism
        eq_(mech.content_type, Representation.EPUB_MEDIA_TYPE)
        eq_(mech.drm_scheme, DeliveryMechanism.NO_DRM)
        eq_(lpm.resource, link.resource)

        resource = link.resource
        representation = resource.representation
        eq_(Representation.EPUB_MEDIA_TYPE, representation.media_type)
        eq_(self.provider.epub_path_for(identifier), 
            representation.local_content_path)

    def test_process_edition_failure(self):
        edition, pool = self._edition(with_license_pool=True)
        edition.primary_identifier.identifier = "fail1"
        eq_(False, self.provider.process_edition(edition))

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
