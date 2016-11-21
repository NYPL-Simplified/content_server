import feedparser
import os
from nose.tools import (
    assert_raises,
    eq_,
    set_trace,
)
from os import path

from . import DatabaseTest

from ..core.lane import (
    Facets,
    Lane,
    Pagination,
)
from ..core.model import (
    CachedFeed,
    Edition,
    Identifier,
    LicensePool,
    Representation,
    Work,
)
from ..core.s3 import DummyS3Uploader

from ..lanes import IdentifiersLane
from ..opds import StaticFeedAnnotator
from ..scripts import StaticFeedGenerationScript

class TestStaticFeedGenerationScript(DatabaseTest):

    def setup(self):
        super(TestStaticFeedGenerationScript, self).setup()
        self.uploader = DummyS3Uploader()
        self.script = StaticFeedGenerationScript(_db=self._db)

    def test_run_with_urns(self):
        not_requested = self._work(with_open_access_download=True)
        requested = self._work(with_open_access_download=True)

        # Works with suppressed LicensePools aren't added to the feed.
        suppressed = self._work(with_open_access_download=True)
        suppressed.license_pools[0].suppressed = True

        # Identifiers without LicensePools are ignored.
        no_pool = self._identifier().urn
        urn1 = requested.license_pools[0].identifier.urn
        urn2 = suppressed.license_pools[0].identifier.urn

        cmd_args = ['fake.csv', '-d', 'mta.librarysimplified.org',
                    '-u', '--urns', no_pool, urn1, urn2]
        self.script.run(uploader=self.uploader, cmd_args=cmd_args)

        # Feeds are created and uploaded for the main feed and its facets.
        eq_(2, len(self.uploader.content))
        for feed in self.uploader.content:
            parsed = feedparser.parse(feed)
            eq_(u'mta.librarysimplified.org', parsed.feed.id)
            eq_(StaticFeedAnnotator.TOP_LEVEL_LANE_NAME, parsed.feed.title)

            # There are links for the different facets.
            links = parsed.feed.links
            eq_(2, len([l for l in links if l.get('facetgroup')]))

            # Only the non-suppressed, license_pooled works we requested
            # are in the entry feed.
            [entry] = parsed.entries
            eq_(requested.title, entry.title)

        # There should also be a Representation saved to the database
        # for each feed.
        representations = self._db.query(Representation).all()

        # Representations with "Dummy content" are created in _license_pool()
        # for each working. We'll ignore these.
        representations = [r for r in representations if r.content != 'Dummy content']
        eq_(2, len(representations))

    def test_run_multipage_filenames(self):
        """Confirm files are uploaded to S3 as expected"""

        w1 = self._work(with_open_access_download=True)
        w2 = self._work(with_open_access_download=True)
        urns = [work.license_pools[0].identifier.urn for work in [w1, w2]]

        cmd_args = ['fake.csv', '-d', 'http://ls.org', '--page-size', '1',
                    '-u', '--urns', urns[0], urns[1]]
        self.script.run(uploader=self.uploader, cmd_args=cmd_args)

        eq_(4, len(self.uploader.uploaded))

        expected_filenames = [
            'index.opds', 'index_2.opds', 'index_author.opds',
            'index_author_2.opds'
        ]
        expected = [self.uploader.content_root()+f for f in expected_filenames]
        result = [rep.mirror_url for rep in self.uploader.uploaded]
        eq_(sorted(expected), sorted(result))

    def test_run_with_csv(self):
        # An incorrect CSV document raises a ValueError when there are
        # also no URNs present.
        cmd_args = ['fake.csv', '-d', 'mta.librarysimplified.org', '-u']
        assert_raises(
            ValueError, self.script.run, uploader=self.uploader,
            cmd_args=cmd_args
        )

        works = [self._work(with_open_access_download=True) for _ in range(4)]
        [w1, w2, w3, w4] = works
        csv_isbns = ['9781682280065', '9781682280027', '9781682280010', '9781682280058']
        for idx, work in enumerate(works):
            identifier = work.license_pools[0].identifier
            identifier.type = Identifier.ISBN
            identifier.identifier = csv_isbns[idx]
        self._db.commit()

        # With a proper CSV file, we get results.
        url = 'https://ls.org'
        cmd_args = ['tests/files/scripts/mini.csv', '-d', url, '-u']
        self.script.run(uploader=self.uploader, cmd_args=cmd_args)

        # Nine feeds are created with the filenames we'd expect.
        eq_(9, len(self.uploader.content))

        expected_filenames = [
            'index.opds', 'nonfiction.opds', 'nonfiction_author.opds',
            'fiction.opds', 'fiction_horror.opds', 'fiction_horror_author.opds',
            'fiction_poetry.opds', 'fiction_poetry_sonnets.opds',
            'fiction_poetry_sonnets_author.opds'
        ]

        created = self._db.query(Representation.mirror_url).\
            filter(Representation.mirror_url.like(self.uploader.content_root()+'%')).\
            all()
        created_filenames = [os.path.split(f[0])[1] for f in created]
        eq_(sorted(expected_filenames), sorted(created_filenames))

        def get_feed(filename):
            like_str = self.uploader.content_root() + filename + '.opds'
            representation = self._db.query(Representation).\
                filter(Representation.mirror_url.like(like_str)).one()
            if not representation:
                return None
            return feedparser.parse(representation.content)

        def collection_links(feed):
            entry_links = list()
            [entry_links.extend(e.links) for e in feed.entries]
            collections = [l.href for l in entry_links if l.rel=='collection']
            return set(collections)

        # All four works are shown in the index.
        index = get_feed('index')
        eq_(4, len(index.entries))

        # It's a grouped feed with proper collection links.
        expected = [url+'/'+f+'.opds' for f in ['fiction', 'nonfiction']]
        eq_(sorted(set(expected)), sorted(collection_links(index)))

        # Other intermediate lanes also have collection_links.
        assert collection_links(get_feed('fiction'))
        assert collection_links(get_feed('fiction_poetry'))

        def has_facet_links(feed):
            facet_links = [l for l in feed.feed.links if l.get('facetgroup')]
            if facet_links:
                return True
            return False

        # Nonfiction has no sublanes, so its a faceted feed with the
        # proper Work.
        nonfiction = get_feed('nonfiction')
        [entry] = nonfiction.entries
        eq_(entry.title, w2.title)
        eq_(entry.simplified_sort_name, w2.author)
        assert has_facet_links(nonfiction)

        # There are two works in the Horror lane feed.
        horror = get_feed('fiction_horror')
        eq_(2, len(horror.entries))
        eq_(sorted([w1.title, w4.title]), sorted([e.title for e in horror.entries]))
        eq_(sorted([w1.author, w4.author]), sorted([e.simplified_sort_name for e in horror.entries]))
        assert has_facet_links(horror)

        # And one in the Sonnet lane.
        sonnets = get_feed('fiction_poetry_sonnets')
        [entry] = sonnets.entries
        eq_(w3.title, entry.title)
        eq_(w3.author, entry.simplified_sort_name)
        assert has_facet_links(sonnets)

    def test_make_lanes_from_csv(self):
        csv_filename = os.path.abspath('tests/files/scripts/sample.csv')
        top_level = self.script.make_lanes_from_csv(csv_filename)[0]

        def sublane_names(parent):
            return sorted([s.name for s in parent.sublanes])

        def sublane_by_name(parent, name):
            """Confirms that there is only one lane with the given name
            the sublanes of the parent

            :return: sublane
            """
            [lane] = filter(lambda s: s.name==name, parent.sublanes)
            return lane

        expected = sorted(['Fiction', 'Short Stories', 'Nonfiction'])
        eq_(expected, sublane_names(top_level))

        # Nonfiction has no subcategories, so it is an IdentifiersLane
        # with works.
        nonfiction = sublane_by_name(top_level, 'Nonfiction')
        eq_(True, isinstance(nonfiction, IdentifiersLane))
        eq_(1, len(nonfiction.identifiers))

        # Short Stories are an intermediate Lane object, with an
        # appropriate IdentifiersLane sublane.
        shorts = sublane_by_name(top_level, 'Short Stories')
        eq_(True, isinstance(shorts, Lane))
        [general_fiction] = shorts.sublanes.lanes
        eq_(True, isinstance(general_fiction, IdentifiersLane))
        eq_('General Fiction', general_fiction.name)
        eq_(1, len(general_fiction.identifiers))

        # Fiction has 3 sublanes, as configured via CSV.
        fiction = sublane_by_name(top_level, 'Fiction')
        expected = sorted(['Horror', 'General Fiction', 'Science Fiction'])
        eq_(expected, sublane_names(fiction))

        for lane in fiction.sublanes:
            # They all have identifiers.
            eq_(True, isinstance(lane, IdentifiersLane))
        # Even more than 1!
        horror = sublane_by_name(fiction, 'Horror')
        eq_(3, len(horror.identifiers))
        # And despite the fact that 'Fiction > Horror > Paranormal' is a
        # category included in the example CSV file, it's not included
        # because it has no identifiers marked.
        eq_(0, len(horror.sublanes))

        # If it did have works marked, it would raise an error.
        csv_filename = os.path.abspath('tests/files/scripts/error.csv')
        assert_raises(ValueError, self.script.make_lanes_from_csv, csv_filename)

        # If the CSV doesn't include columns for any lanes, a single
        # IdentifiersLane with works is returned.
        csv_filename = os.path.abspath('tests/files/scripts/laneless.csv')
        result = self.script.make_lanes_from_csv(csv_filename)[0]
        eq_(True, isinstance(result, IdentifiersLane))
        eq_(3, len(result.identifiers))

    def test_create_feeds(self):
        omega = self._work(title='Omega', authors='Iota', with_open_access_download=True)
        alpha = self._work(title='Alpha', authors='Theta', with_open_access_download=True)
        zeta = self._work(title='Zeta', authors='Phi', with_open_access_download=True)

        identifiers = list()
        for work in [omega, alpha, zeta]:
            identifier = work.license_pools[0].identifier
            identifiers.append(identifier)
        lane = IdentifiersLane(
            self._db, identifiers, StaticFeedAnnotator.TOP_LEVEL_LANE_NAME
        )

        results = list(self.script.create_feeds(
            [lane], 'https://mta.librarysimplified.org', 50
        ))

        eq_(2, len(results))
        eq_(['index', 'index_author'], sorted([r[0] for r in results]))
        for filename, [feed] in results:
            eq_(True, isinstance(feed, CachedFeed))
            parsed = feedparser.parse(feed.content)
            [active_facet_link] = [l for l in parsed.feed.links if l.get('activefacet')]
            if filename == 'index':
                # The entries are sorted by title, by default.
                eq_('Title', active_facet_link.get('title'))
                titles = [e.title for e in parsed.entries]
                eq_(['Alpha', 'Omega', 'Zeta'], titles)

            if filename == 'index_author':
                # The entries can also be sorted by author.
                eq_('Author', active_facet_link.get('title'))
                authors = [e.simplified_sort_name for e in parsed.entries]
                eq_(['Iota', 'Phi', 'Theta'], authors)

    def test_create_feed_pages(self):
        w1 = self._work(with_open_access_download=True)
        w2 = self._work(with_open_access_download=True)

        identifiers = [w.license_pools[0].identifier for w in [w1, w2]]

        pagination = Pagination(size=1)
        lane = IdentifiersLane(
            self._db, identifiers, StaticFeedAnnotator.TOP_LEVEL_LANE_NAME
        )
        facet = Facets('main', 'always', 'title')
        annotator = StaticFeedAnnotator('https://ls.org', lane)

        result = self.script.create_feed_pages(
            lane, pagination, 'https://ls.org', annotator, facet
        )

        # Two feeds are returned with the proper links.
        [first, second] = result
        def links_by_rel(parsed_feed, rel):
            return [l for l in parsed_feed.feed.links if l['rel']==rel]

        parsed = feedparser.parse(first.content)
        [entry] = parsed.entries
        eq_(w1.title, entry.title)
        eq_(w1.author, entry.simplified_sort_name)

        [next_link] = links_by_rel(parsed, 'next')
        eq_(next_link.href, 'https://ls.org/index_title_2.opds')
        eq_([], links_by_rel(parsed, 'previous'))
        eq_([], links_by_rel(parsed, 'first'))

        parsed = feedparser.parse(second.content)
        [entry] = parsed.entries
        eq_(w2.title, entry.title)
        eq_(w2.author, entry.simplified_sort_name)

        [previous_link] = links_by_rel(parsed, 'previous')
        [first_link] = links_by_rel(parsed, 'first')
        first = 'https://ls.org/index_title.opds'
        eq_(previous_link.href, first)
        eq_(first_link.href, first)
        eq_([], links_by_rel(parsed, 'next'))
