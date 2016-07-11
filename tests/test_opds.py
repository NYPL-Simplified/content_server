from nose.tools import (
    assert_raises,
    set_trace,
)
from . import DatabaseTest
from ..opds import ContentServerAnnotator
from ..core.opds import UnfulfillableWork

class TestAnnotator(DatabaseTest):

    def test_unfulfillable_work_raises_exception(self):
        work = self._work(with_license_pool=True)
        # This work has a LicensePool but no licenses and no
        # open-access downloads. The ContentServerAnnotator will raise
        # UnfulfillableWork when asked to annotate a feed for this
        # work.
        assert_raises(
            UnfulfillableWork, 
            ContentServerAnnotator.annotate_work_entry,
            work, work.license_pools[0], work.presentation_edition,
            work.presentation_edition.primary_identifier,
            None, None
        )
