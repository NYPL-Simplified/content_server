from nose.tools import (
    assert_raises,
    eq_,
    set_trace,
)
from . import DatabaseTest
from ..opds import (
    ContentServerAnnotator,
    StaticFeedAnnotator,
)
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

class TestStaticFeedAnnotator(object):

    def test_slugify_feed_title(self):
        annotator = StaticFeedAnnotator
        eq_('hey-im-a-feed', annotator.slugify_feed_title("Hey! I'm a feed!!"))
        eq_('you-and-me-n-every_feed', annotator.slugify_feed_title("You & Me n Every_Feed"))
        eq_('money-honey', annotator.slugify_feed_title("Money $$$       Honey"))
