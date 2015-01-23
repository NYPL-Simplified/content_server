from nose.tools import (
    set_trace,
    eq_,
)
import urllib
from ..core.testing import DatabaseTest

from ..coverage import GutenbergEPUBCoverageProvider
from ..core.s3 import DummyS3Uploader
from ..core.model import (
    Resource
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
            self._db, s3_uploader=DummyS3Uploader)

    def test_process_edition_success(self):
        edition, pool = self._edition(with_license_pool=True)
        eq_(True, self.provider.process_edition(edition))

        # Something was 'uploaded' to S3.
        [[local_path, url]] = self.provider.uploader.uploaded
        identifier = edition.primary_identifier
        eq_(self.provider.epub_path_for(identifier), local_path)
        expected_ending = urllib.quote("%s/%s.epub" % (
            identifier.type, identifier.identifier))
        assert url.endswith(expected_ending)

        # A resource was created for this edition.
        [resource] = self._db.query(Resource).all()
        eq_(identifier, resource.identifier)
        eq_(pool, resource.license_pool)
        eq_(Resource.OPEN_ACCESS_DOWNLOAD, resource.rel)
        eq_(Resource.EPUB_MEDIA_TYPE, resource.media_type)
        eq_(self.provider.output_source, resource.data_source)
        eq_(True, resource.mirrored)

        expected_mirrored_path = "%%(open_access_books)s/%s/%s.epub" % (
            urllib.quote(identifier.type).replace("%", "%%"),
            identifier.identifier)

        eq_(expected_mirrored_path, resource.mirrored_path)
        assert resource.final_url.endswith(expected_ending)

    def test_process_edition_failure(self):
        edition, pool = self._edition(with_license_pool=True)
        edition.primary_identifier.identifier = "fail1"
        eq_(False, self.provider.process_edition(edition))

        # No resource has been created.
        eq_([], self._db.query(Resource).all())

        # Nothing was uploaded to S3.
        eq_([], self.provider.uploader.uploaded)

    def test_best_epub_in(self):
        f = self.provider.best_epub_in
        eq_(None, f([]))
        eq_(None, f(["foo.txt", "bar.html"]))
        eq_("bar.epub", f(["foo.txt", "bar.epub"]))
        eq_("bar-noimages.epub", f(["foo.txt", "bar-noimages.epub"]))
        eq_("bar-images.epub", f(["foo-noimages.epub", "bar-images.epub"]))
