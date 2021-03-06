import feedparser

from nose.tools import (
    assert_raises,
    eq_,
    set_trace,
)
from . import DatabaseTest
from ..opds import (
    ContentServerAnnotator,
    StaticFeedAnnotator,
    StaticCOPPANavigationFeed,
)
from ..core.opds import UnfulfillableWork

class MockStaticLane(object):

    """Empty, unobtrusive Lane class that gives any
    StaticFeedAnnotator a name to work with."""

    def __init__(self, name):
        self.parent = None
        self.name = name or 'Fake Lane'


class TestAnnotator(DatabaseTest):

    def test_unfulfillable_work_raises_exception(self):
        work = self._work(with_license_pool=True)
        [lp] = work.license_pools
        lp.open_access = True

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


class TestStaticFeedAnnotator(DatabaseTest):

    def test_prefix(self):

        annotator = StaticFeedAnnotator('test.org', prefix='demo/')
        eq_('test.org/demo/index.xml', annotator.default_lane_url())

        lane = MockStaticLane('Parliament Funkadelic')
        eq_('demo/parliament-funkadelic', annotator.lane_filename(lane))
        eq_('test.org/demo/parliament-funkadelic.xml', annotator.groups_url(lane))

        # The prefix doesn't impact the search URL. (Mainly because we
        # use AWS lambda for Instant Classics search at the moment, and
        # there hasn't been an example of another case yet.)
        eq_('test.org/search', annotator.search_url())

    def test_sort_works_for_groups_feed(self):
        spanish = MockStaticLane('Spanish')
        nonfic = MockStaticLane('Nonfiction')
        shorts = MockStaticLane('Short Stories')
        whatever = MockStaticLane('Whatever')

        annotator = StaticFeedAnnotator('test.org')

        works = list()
        for lane in [spanish, nonfic, shorts, whatever]:
            w = self._work()
            works.append(w)
            annotator.lanes_by_work[w] = [dict(lane=lane)]
        w1, w2, w3, w4 = works

        result = annotator.sort_works_for_groups_feed(works)
        # Works are sorted by the priority of their lane names.
        eq_([w3, w2, w1, w4], result)

        # If a work is featured by multiple lanes, it is sorted
        # accoridng to the highest-priority lane.
        fic = MockStaticLane('Fiction')
        annotator.lanes_by_work[w4].append(dict(lane=fic))
        result = annotator.sort_works_for_groups_feed(works)
        eq_([w3, w4, w2, w1], result)


class TestStaticCOPPANavigationFeed(object):

    def test_feed(self):
        youth_lane = MockStaticLane('For Kids')
        adult_lane = MockStaticLane('For Adults')

        feed = StaticCOPPANavigationFeed(
            'Books', 'books.org', youth_lane, adult_lane
        )

        parsed = feedparser.parse(unicode(feed))

        # The feed has the right basic information.
        eq_('books.org/index.xml', parsed.feed.id)
        [link] = parsed.feed.links
        eq_('books.org/index.xml', link.href)
        eq_('Books', parsed.feed.title)
        assert parsed.feed.updated

        entries = parsed.entries
        eq_(2, len(entries))

        # The children's feed entry has accurate details
        kid_url = 'books.org/for-kids.xml'
        [children] = [e for e in entries if e.id==kid_url]
        assert "under 13" in children.title.lower()
        assert children.updated

        [content] = children.content
        assert "children's books" in content.value.lower()
        [link] = children.links
        eq_(feed.ACQUISITION_FEED_TYPE, link.type)
        eq_(kid_url, link.href)
        eq_('subsection', link.rel)

        # The adult feed entry has accurate details
        adult_url = 'books.org/for-adults.xml'
        [adult] = [e for e in entries if e.id==adult_url]
        assert "13 or older" in adult.title.lower()
        assert adult.updated

        [content] = adult.content
        assert "full collection" in content.value.lower()
        [link] = adult.links
        eq_(feed.ACQUISITION_FEED_TYPE, link.type)
        eq_(adult_url, link.href)
        eq_('subsection', link.rel)
