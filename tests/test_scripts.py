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
    Edition,
    Identifier,
    LicensePool,
    Representation,
    Work,
)

from ..lanes import StaticFeedBaseLane
from ..opds import StaticFeedAnnotator
from ..s3 import DummyS3Uploader
from ..scripts import (
    StaticFeedCSVExportScript,
    StaticFeedGenerationScript,
)


class TestStaticFeedCSVExportScript(DatabaseTest):

    def setup(self):
        super(TestStaticFeedCSVExportScript, self).setup()
        self.script = StaticFeedCSVExportScript(_db=self._db)

    def test_create_row_data(self):
        # Create a buncha works with genres correlated to categories in
        # tests/files/scripts/sample.csv.
        romance = self._work(with_open_access_download=True, genre='Romance')
        paranormal = self._work(with_open_access_download=True, genre='Paranormal Mystery')
        scifi = self._work(with_open_access_download=True, genre='Science Fiction')
        short = self._work(with_open_access_download=True, genre='Short Stories')
        history = self._work(with_open_access_download=True, genre='History', fiction=False)

        # When there are no categories, only basic work information is
        # listed for all of the works.
        row_data = list(self.script.create_row_data(
            self.script.base_works_query, 'tests/files/scripts/laneless.csv'
        ))
        eq_(5, len(row_data))

        # Only the basic work information headers are included.
        expected = self.script.NONLANE_HEADERS[:]
        expected.remove('featured')
        expected.remove('youth')
        all_headers = list()
        [all_headers.extend(r.keys()) for r in row_data]
        eq_(sorted(expected), sorted(set(all_headers)))

        # When there are categories, the rows get sorted into their places.
        row_data = list(self.script.create_row_data(
            self.script.base_works_query, 'tests/files/scripts/sample.csv'
        ))
        eq_(5, len(row_data))

        def work_has_category_path(work, category_path):
            urn = work.license_pools[0].identifier.urn
            [row_datum] = [r for r in row_data if r['urn']==urn]
            assert row_datum.get(category_path)

        work_has_category_path(romance, 'Fiction>General Fiction')
        work_has_category_path(paranormal, 'Fiction>Horror>Paranormal Mystery')
        work_has_category_path(scifi, 'Fiction>Science Fiction')
        work_has_category_path(short, 'Short Stories>General Fiction')
        work_has_category_path(history, 'Nonfiction')

    def test_apply_node(self):
        ignored_work = self._work(with_open_access_download=True)
        base_query = self._db.query(Work)

        # Nodes can be applied if they are a language.
        node = self.script.CategoryNode('Spanish')
        espanol = self._work(with_open_access_download=True, language='spa')
        result = self.script.apply_node(node, base_query)
        eq_([espanol], result.all())

        # Nodes can be applied if they are 'Fiction' or 'Nonfiction'.
        node = self.script.CategoryNode('Nonfiction')
        nonfiction = self._work(with_open_access_download=True, fiction=False)
        result = self.script.apply_node(node, base_query)
        eq_([nonfiction], result.all())

        # Nodes can be applied if they are a genre.
        node = self.script.CategoryNode('Science Fiction')
        scifi = self._work(with_open_access_download=True, genre='Science Fiction')
        result = self.script.apply_node(node, base_query)
        eq_([scifi], result.all())

    def test_get_base_categories(self):
        nodes = self.script.get_base_categories('tests/files/scripts/categories.yml')
        eq_(19, len(nodes))

        nodes = self.script.get_base_categories('tests/files/scripts/mini.csv')
        eq_(3, len(nodes))

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
            eq_(u'mta.librarysimplified.org/index.xml', parsed.feed.id)
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
            'index.xml', 'index_2.xml', 'index_author.xml',
            'index_author_2.xml'
        ]
        expected = [self.uploader.static_feed_root()+f for f in expected_filenames]
        result = [rep.mirror_url for rep in self.uploader.uploaded]
        eq_(sorted(expected), sorted(result))

    def run_mini_csv(self, *args):
        """Performs prepatory work for testing by running the smaller mini.csv
        file with appropriately identified works.

        :return: 4 Works that can be used for testing purposes.
        """
        works = [self._work(with_open_access_download=True) for _ in range(4)]
        [w1, w2, w3, w4] = works
        csv_isbns = ['9781682280065', '9781682280027', '9781682280010', '9781682280058']
        for idx, work in enumerate(works):
            identifier = work.license_pools[0].identifier
            identifier.type = Identifier.ISBN
            identifier.identifier = csv_isbns[idx]
        self._db.commit()

        url = 'https://ls.org'
        cmd_args = ['tests/files/scripts/mini.csv', '-d', url, '-u']
        cmd_args += args
        self.script.run(uploader=self.uploader, cmd_args=cmd_args)
        return w1, w2, w3, w4

    def test_run_with_csv(self):
        # An incorrect CSV document raises a ValueError when there are
        # also no URNs present.
        cmd_args = ['fake.csv', '-d', 'mta.librarysimplified.org', '-u']
        assert_raises(
            ValueError, self.script.run, uploader=self.uploader,
            cmd_args=cmd_args
        )

        # With a proper CSV file, we get results.
        w1, w2, w3, w4 = self.run_mini_csv()

        # Nine feeds are created with the filenames we'd expect.
        eq_(9, len(self.uploader.content))

        expected_filenames = [
            'index.xml', 'nonfiction.xml', 'nonfiction_author.xml',
            'fiction.xml', 'fiction_horror.xml', 'fiction_horror_author.xml',
            'fiction_poetry.xml', 'fiction_poetry_sonnets.xml',
            'fiction_poetry_sonnets_author.xml'
        ]

        created = self._db.query(Representation.mirror_url).\
            filter(Representation.mirror_url.like(self.uploader.static_feed_root()+'%')).\
            all()
        created_filenames = [os.path.split(f[0])[1] for f in created]
        eq_(sorted(expected_filenames), sorted(created_filenames))

        def get_feed(filename):
            like_str = self.uploader.static_feed_root() + filename + '.xml'
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
        expected = ['https://ls.org/'+f+'.xml' for f in ['fiction', 'nonfiction']]
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

    def test_run_with_prefix(self):
        prefix_args = ['--prefix', 'testing/']
        self.run_mini_csv(*prefix_args)
        representations = self._db.query(Representation).filter(
            Representation.mirror_url.like(self.uploader.static_feed_root()+'%')
        ).all()

        eq_(9, len(representations))
        for r in representations:
            # The prefix has been inserted into the mirror url.
            r.mirror_url.startswith('https://ls.org/testing/')

    def test_make_lanes_from_csv(self):
        csv_filename = os.path.abspath('tests/files/scripts/sample.csv')
        top_level, _query, youth_lane = self.script.make_lanes_from_csv(csv_filename)

        def sublane_names(parent):
            return sorted([s.name for s in parent.sublanes])

        def sublane_by_name(parent, name):
            """Confirms that there is only one lane with the given name
            the sublanes of the parent

            :return: sublane
            """
            [lane] = filter(lambda s: s.name==name, parent.sublanes)
            return lane

        # None of the books have been marked for a youth lane, so
        # there is no youth lane.
        eq_(None, youth_lane)

        expected = sorted(['Fiction', 'Short Stories', 'Nonfiction'])
        eq_(expected, sublane_names(top_level))

        # Nonfiction has no subcategories, so it is an StaticFeedBaseLane
        # with works.
        nonfiction = sublane_by_name(top_level, 'Nonfiction')
        eq_(True, isinstance(nonfiction, StaticFeedBaseLane))
        eq_(1, len(nonfiction.identifiers))

        # Short Stories are an intermediate Lane object, with an
        # appropriate StaticFeedBaseLane sublane.
        shorts = sublane_by_name(top_level, 'Short Stories')
        eq_(True, isinstance(shorts, Lane))
        [general_fiction] = shorts.sublanes.lanes
        eq_(True, isinstance(general_fiction, StaticFeedBaseLane))
        eq_('General Fiction', general_fiction.name)
        eq_(1, len(general_fiction.identifiers))

        # Fiction has 3 sublanes, as configured via CSV.
        fiction = sublane_by_name(top_level, 'Fiction')
        expected = sorted(['Horror', 'General Fiction', 'Science Fiction'])
        eq_(expected, sublane_names(fiction))

        for lane in fiction.sublanes:
            # They all have identifiers.
            eq_(True, isinstance(lane, StaticFeedBaseLane))
        # Even more than 1!
        horror = sublane_by_name(fiction, 'Horror')
        eq_(3, len(horror.identifiers))
        # And despite the fact that 'Fiction > Horror > Paranormal Mystery'
        # is a category included in the example CSV file, it's not included
        # because it has no identifiers marked.
        eq_(0, len(horror.sublanes))

        # If it did have works marked, it would raise an error.
        csv_filename = os.path.abspath('tests/files/scripts/error.csv')
        assert_raises(ValueError, self.script.make_lanes_from_csv, csv_filename)

        # If the CSV doesn't include columns for any lanes, a single
        # StaticFeedBaseLane with works is returned.
        csv_filename = os.path.abspath('tests/files/scripts/laneless.csv')
        result = self.script.make_lanes_from_csv(csv_filename)[0]
        eq_(True, isinstance(result, StaticFeedBaseLane))
        eq_(3, len(result.identifiers))

        # Works marked for a youth are returned in a youth lane.
        csv_filename = os.path.abspath('tests/files/scripts/youth.csv')
        result = self.script.make_lanes_from_csv(csv_filename)[2]
        eq_(True, isinstance(result, StaticFeedBaseLane))
        eq_(2, len(result.identifiers))

    def test_create_feeds(self):
        omega = self._work(title='Omega', authors='Iota', with_open_access_download=True)
        alpha = self._work(title='Alpha', authors='Theta', with_open_access_download=True)
        zeta = self._work(title='Zeta', authors='Phi', with_open_access_download=True)

        identifiers = list()
        for work in [omega, alpha, zeta]:
            identifier = work.license_pools[0].identifier
            identifiers.append(identifier)
        lane = StaticFeedBaseLane(
            self._db, identifiers, StaticFeedAnnotator.TOP_LEVEL_LANE_NAME
        )
        annotator = StaticFeedAnnotator('https://mta.librarysimplified.org')

        results = list(self.script.create_feeds([lane], 50, annotator))

        eq_(2, len(results))
        eq_(['index', 'index_author'], sorted([r[0] for r in results]))
        for filename, [feed] in results:
            eq_(True, isinstance(feed, unicode))
            parsed = feedparser.parse(feed)
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
        lane = StaticFeedBaseLane(
            self._db, identifiers, StaticFeedAnnotator.TOP_LEVEL_LANE_NAME
        )
        facet = Facets('main', 'always', 'author')
        annotator = StaticFeedAnnotator('https://ls.org', lane)

        result = self.script.create_feed_pages(
            lane, pagination, 'https://ls.org', annotator, facet
        )

        # Two feeds are returned with the proper links.
        [first, second] = result
        def links_by_rel(parsed_feed, rel):
            return [l for l in parsed_feed.feed.links if l['rel']==rel]

        parsed = feedparser.parse(first)
        [entry] = parsed.entries
        eq_(w1.title, entry.title)
        eq_(w1.author, entry.simplified_sort_name)

        [next_link] = links_by_rel(parsed, 'next')
        eq_(next_link.href, 'https://ls.org/index_author_2.xml')
        eq_([], links_by_rel(parsed, 'previous'))
        eq_([], links_by_rel(parsed, 'first'))

        parsed = feedparser.parse(second)
        [entry] = parsed.entries
        eq_(w2.title, entry.title)
        eq_(w2.author, entry.simplified_sort_name)

        [previous_link] = links_by_rel(parsed, 'previous')
        [first_link] = links_by_rel(parsed, 'first')
        first = 'https://ls.org/index_author.xml'
        eq_(previous_link.href, first)
        eq_(first_link.href, first)
        eq_([], links_by_rel(parsed, 'next'))
