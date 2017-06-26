import contextlib
import feedparser
import json
import os
import tempfile
from nose.tools import (
    assert_raises,
    eq_,
    set_trace,
)
from os import path

from sqlalchemy.orm.exc import NoResultFound

from . import DatabaseTest

from ..core.external_search import DummyExternalSearchIndex
from ..core.lane import (
    Facets,
    Lane,
    Pagination,
)
from ..core.model import (
    ConfigurationSetting,
    CustomList,
    DataSource,
    Edition,
    ExternalIntegration,
    Hyperlink,
    Identifier,
    LicensePool,
    Representation,
    Work,
)

from ..config import (
    Configuration,
    temp_config,
)
from ..lanes import StaticFeedBaseLane
from ..opds import StaticFeedAnnotator
from ..s3 import DummyS3Uploader
from ..scripts import (
    CustomListUploadScript,
    CustomListFeedGenerationScript,
    StaticFeedGenerationScript,
    StaticFeedCSVExportScript,
    CSVFeedGenerationScript,
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

        # Put them all in a Collection with an appropriate DataSource
        # so we can find them.
        feedbooks = self._collection(
            protocol=ExternalIntegration.OPDS_IMPORT,
            data_source_name=DataSource.FEEDBOOKS
        )
        for work in [romance, paranormal, scifi, short, history]:
            work.license_pools[0].collection = feedbooks

        # When there are no categories, only basic work information is
        # listed for all of the works.
        row_data = list(self.script.create_row_data(
            self.script.base_works_query, 'tests/files/scripts/laneless.csv'
        ))
        eq_(5, len(row_data))

        # Only the basic work information headers are included.
        expected = self.script.BASIC_HEADERS
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


class TestCustomListUploadScript(DatabaseTest):

    def setup(self):
        super(TestCustomListUploadScript, self).setup()
        self.script = CustomListUploadScript(_db=self._db)

    def _create_works_from_csv(self, csv_filename):
        local_filename = os.path.join('tests', 'files', 'scripts', csv_filename)
        csv_filename = os.path.abspath(local_filename)

        # Create all of the identifiers in the csv.
        identifiers = list()
        with open(csv_filename) as f:
            text = f.read()
            rows = text.split('\n')
            for row in rows:
                urn = unicode(row.split(',')[0].strip())
                if urn and urn != u'urn':
                    identifier = Identifier.parse_urn(self._db, urn)[0]
                    identifiers.append(identifier)

        # Get any Works that already exist.
        works_by_urn = dict()
        works = Work.from_identifiers(self._db, identifiers).all()
        for work in works:
            identifier = work.license_pools[0].identifier
            identifiers.remove(identifier)
            works_by_urn[identifier.urn] = work

        # Prepare additional works expected by the csv.
        for identifier in identifiers:
            edition, lp = self._edition(
                with_open_access_download=True,
                identifier_type=identifier.type,
                identifier_id=identifier.identifier)
            work = self._work(presentation_edition=edition)

            works.append(work)
            works_by_urn[identifier.urn] = work
        self._db.commit()

        # Return everything you might need for a test.
        return works, works_by_urn, local_filename

    def test_fetch_editable_list(self):
        # Trying to edit a list that doesn't exist raises an error.
        for option in self.script.EDIT_OPTIONS:
            assert_raises(NoResultFound, self.script.fetch_editable_list,
                u'A List', u'my-list', option)

        # Creating a new list without name overlap does not raise an
        # error and does not return a CustomList.
        result = self.script.fetch_editable_list(u'A List', u'my-list', 'new')
        eq_(None, result)

        # Let's make a CustomList!
        custom_list = self._customlist(
            foreign_identifier=u'a-list', name=u'My List', num_entries=0,
            data_source_name=DataSource.LIBRARY_STAFF
        )[0]

        # Trying to create a new list that has the same foreign identifier
        # raises an error.
        assert_raises(self.script.CustomListAlreadyExists,
            self.script.fetch_editable_list, u'A List', u'a-list', 'new')

        # Trying to edit the list in any way returns the list, though.
        for option in self.script.EDIT_OPTIONS:
            result = self.script.fetch_editable_list(u'A List', u'a-list', option)
            eq_(custom_list, result)

    def test_works_from_source(self):
        works, works_by_urn, filename = self._create_works_from_csv('mini.csv')
        # Create an extra work to confirm that it's ignored.
        ignored_work = self._work(with_open_access_download=True)

        # A basic CSV returns what we expect.
        works_qu, youth_qu, featured = self.script.works_from_source(filename)

        # The four works we wanted.
        eq_(4, works_qu.count())
        eq_(sorted(works), sorted(works_qu.all()))

        # The work we didn't ask for isn't included.
        assert ignored_work not in works_qu

        # And when the CSV has no youth entries, no youth query is returned.
        eq_(None, youth_qu)

        # A CSV with youth entries returns those works, too.
        works, works_by_urn, filename = self._create_works_from_csv('youth.csv')
        works_qu, youth_qu, featured = self.script.works_from_source(filename)

        eq_(2, youth_qu.count())
        assert works_by_urn['urn:isbn:9781682280010'] in youth_qu
        assert works_by_urn['urn:isbn:9781682280027'] in youth_qu

        # Plus all the works, as you do.
        eq_(sorted(works), sorted(works_qu.all()))

        # But the work we didn't ask for still isn't included.
        eq_([False, False], [ignored_work in qu for qu in [works_qu, youth_qu]])

        # If the CSV indicates that a cover should be removed, it's gone.
        works, works_by_urn, filename = self._create_works_from_csv('hidden_cover.csv')
        for work in works:
            work.presentation_edition.cover_full_url = 'cover.png'
            work.presentation_edition.cover_thumbnail_url = 'thumbnail.jpg'
            work.calculate_opds_entries()

            # Ensure the covers are listed.
            entries = work.simple_opds_entry+work.verbose_opds_entry
            eq_(2, entries.count('cover.png'))
            eq_(2, entries.count('thumbnail.jpg'))
        self._db.commit()

        # Set a higher minimum quality.
        ConfigurationSetting.for_library(
            Configuration.MINIMUM_FEATURED_QUALITY, self._default_library
        ).value = unicode(0.90)
        works_qu, youth_qu, featured = self.script.works_from_source(filename)

        eq_(2, works_qu.count())
        hidden_work = works_by_urn['urn:isbn:9781682280027']
        hidden_work_entries = hidden_work.simple_opds_entry+hidden_work.verbose_opds_entry
        assert 'cover.png' not in hidden_work_entries
        assert 'thumbnail.jpg' not in hidden_work_entries

        # If the CSV indicates that a work should be featured, its
        # Identifier is returned in the featured list.
        featured_work = works_by_urn['urn:isbn:9781682280065']
        featured_identifier = featured_work.license_pools[0].identifier
        eq_([featured_identifier], featured)

    def test_edit_list(self):
        custom_list = self._customlist(num_entries=0)[0]
        mini_works, works_by_urn, _f = self._create_works_from_csv('mini.csv')

        # You can create a new list.
        works_qu = self._db.query(Work).filter(Work.id.in_([w.id for w in mini_works]))
        self.script.edit_list(custom_list, works_qu, 'new', [])
        eq_(4, len(custom_list.entries))

        # You can append to an existing list.
        sample_works, works_by_urn, _f = self._create_works_from_csv('sample.csv')
        works_qu = self._db.query(Work).filter(Work.id.in_([w.id for w in sample_works]))
        self.script.edit_list(custom_list, works_qu, 'append', [])
        eq_(7, len(custom_list.entries))

        # You can replace an existing list.
        other_work = self._work(with_license_pool=True)
        other_identifier = other_work.license_pools[0].identifier
        works = mini_works + [other_work]
        works_qu = self._db.query(Work).filter(Work.id.in_([w.id for w in works]))
        self.script.edit_list(custom_list, works_qu, 'replace', [other_identifier])
        eq_(5, len(custom_list.entries))
        [entry] = list(custom_list.entries_for_work(other_work))
        # Also, works that are requested to be featured, are featured.
        eq_(True, entry.featured)

        # You can remove from an existing list.
        works_qu = self._db.query(Work).filter(Work.id.in_([w.id for w in sample_works]))
        self.script.edit_list(custom_list, works_qu, 'remove', [])
        eq_(1, len(custom_list.entries))
        assert list(custom_list.entries_for_work(other_work))

    def test_run(self):
        works, works_by_urn, _f = self._create_works_from_csv('youth.csv')
        youth_urns = ['urn:isbn:9781682280010', 'urn:isbn:9781682280027']

        cmd_args = ['tests/files/scripts/youth.csv', 'Test Lane', '-n']
        self.script.do_run(cmd_args=cmd_args)

        # Two CustomLists were created.
        lists = self._db.query(CustomList).all()
        eq_(2, len(lists))
        [youth_list, full_list] = sorted(lists, key=lambda l: len(l.entries))

        # Only the youth works are in the youth list.
        eq_(2, len(youth_list.entries))
        for urn in youth_urns:
            eq_(1, len(list(youth_list.entries_for_work(works_by_urn[urn]))))

        # All of the works are in the full list.
        eq_(7, len(full_list.entries))
        for urn, work in works_by_urn.items():
            eq_(1, len(list(full_list.entries_for_work(work))))


@contextlib.contextmanager
def lower_lane_sample_size():
    setattr(Lane, 'MINIMUM_SAMPLE_SIZE', 1)
    try:
        yield
    finally:
        delattr(Lane, 'MINIMUM_SAMPLE_SIZE')


class TestStaticFeedGenerationScript(DatabaseTest):

    def setup(self):
        super(TestStaticFeedGenerationScript, self).setup()
        self.library = self._default_library
        self.uploader = DummyS3Uploader()
        self.script = StaticFeedGenerationScript(_db=self._db)

    def test_create_feeds(self):
        omega = self._work(title='Omega', authors='Iota', with_open_access_download=True)
        alpha = self._work(title='Alpha', authors='Theta', with_open_access_download=True)
        zeta = self._work(title='Zeta', authors='Phi', with_open_access_download=True)

        identifiers = list()
        for work in [omega, alpha, zeta]:
            identifier = work.license_pools[0].identifier
            identifiers.append(identifier)
        lane = StaticFeedBaseLane(
            self.library, identifiers, StaticFeedAnnotator.TOP_LEVEL_LANE_NAME
        )
        annotator = StaticFeedAnnotator('https://mta.librarysimplified.org')

        results = list(self.script.create_feeds([lane], annotator))

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
            self.library, identifiers, StaticFeedAnnotator.TOP_LEVEL_LANE_NAME
        )
        facet = Facets(
            None, 'main', 'always', 'author',
            enabled_facets=self.script.DEFAULT_ENABLED_FACETS
        )
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


class TestCustomListFeedGenerationScript(DatabaseTest):

    def setup(self):
        super(TestCustomListFeedGenerationScript, self).setup()

        self.custom_list, self.entries = self._customlist(
            foreign_identifier=u'a-list', name=u'A List', num_entries=0,
            data_source_name=DataSource.LIBRARY_STAFF)
        self.uploader = DummyS3Uploader()
        self.script = CustomListFeedGenerationScript(_db=self._db)

    def test_extract_feed_configuration(self):

        # If the feed configuration file is empty, an error is raised.
        test_file = 'tests/files/scripts/empty.json'
        cmd_args = ['a-list', test_file, 'https://ls.org', '--storage-bucket', 'test.feed.bucket']
        parser = self.script.arg_parser().parse_args(cmd_args)
        feed_config = self.script.get_json_config(test_file)
        assert_raises(
            ValueError, self.script.extract_feed_configuration,
            feed_config, parser
        )

        # If the parser has a license_link, it isn't overwritten.
        test_file = 'tests/files/scripts/sample_config.json'
        cmd_args = [
            'a-list', test_file, 'https://ls.org',
            '--license', 'http://samplelicense.net'
        ]
        parser = self.script.arg_parser().parse_args(cmd_args)
        feed_config = self.script.get_json_config(test_file)
        returned_license_link = self.script.extract_feed_configuration(
            feed_config, parser
        )[1]
        eq_('http://samplelicense.net', returned_license_link)

        # Neither is an included uploader.
        test_file = 'tests/files/scripts/sample_config.json'
        cmd_args = ['a-list', test_file, 'https://ls.org']
        parser = self.script.arg_parser().parse_args(cmd_args)
        feed_config = self.script.get_json_config(test_file)
        uploader = DummyS3Uploader()

        returned_uploader = self.script.extract_feed_configuration(
            feed_config, parser, uploader=uploader)[3]
        eq_(uploader, returned_uploader)

        def assert_incomplete_error_raised(config_filename, args=None):
            test_file = 'tests/files/scripts/%s.json' % config_filename
            cmd_args = ['a-list', test_file, 'https://ls.org',
                        '--storage-bucket', 'test.feed.bucket']
            if args:
                cmd_args += args
            parser = self.script.arg_parser().parse_args(cmd_args)
            feed_config = self.script.get_json_config(test_file)
            assert_raises(
                self.script.IncompleteFeedConfigurationError,
                self.script.extract_feed_configuration, feed_config, parser
            )

        # An error is raised if there's no lanes policy.
        assert_incomplete_error_raised('no_lane_policy')

        # Or if the S3 integration is included but incomplete.
        assert_incomplete_error_raised('no_s3_secret', ['-u'])

        # Or if the Elasticsearch integration is included but incomplete.
        assert_incomplete_error_raised('no_elasticsearch_url')

        # When the configuration is complete, we get back everything we expect.
        test_file = 'tests/files/scripts/sample_config.json'
        cmd_args = ['a-list', test_file, 'https://ls.org', '-u']
        parser = self.script.arg_parser().parse_args(cmd_args)
        feed_config = self.script.get_json_config(test_file)
        results = self.script.extract_feed_configuration(feed_config, parser)

        eq_(5, len(results))
        (lanes_policy, license_link, enabled_facets, uploader, search_client) = results

        eq_(feed_config['policies']['lanes'], lanes_policy)
        eq_('https://ls.org/license.html', license_link)
        eq_(feed_config['policies']['facets'], enabled_facets)
        eq_('abc', uploader.pool.auth.access_key)
        # Can't create an Elasticsearch client without referring to a
        # running Elasticsearch instance, which is frustrating here.
        eq_(None, search_client)

        # Enabled facets aren't required.
        test_file = 'tests/files/scripts/no_enabled_facets.json'
        cmd_args = ['a-list', test_file, 'https://ls.org']
        parser = self.script.arg_parser().parse_args(cmd_args)
        feed_config = self.script.get_json_config(test_file)
        results = self.script.extract_feed_configuration(feed_config, parser)
        eq_(5, len(results))
        eq_(dict(), enabled_facets)

    def test_run(self):
        # If there's an uploader but no static_feed_bucket, raise an error.
        incomplete_cmd_args = [
            'a-list', 'tests/files/scripts/sample_config.json',
            'http://ls.org', '-u'
        ]
        assert_raises(
            ValueError, self.script.do_run, uploader=self.uploader,
            cmd_args=incomplete_cmd_args
        )

        romance = self._work(title="A", with_open_access_download=True, genre='Romance')
        gothic = self._work(title="B", with_open_access_download=True, genre='Gothic Romance')
        mystery = self._work(with_open_access_download=True, genre='Hard-Boiled Mystery')
        paranormal = self._work(with_open_access_download=True, genre='Paranormal Mystery')
        scifi = self._work(with_open_access_download=True, genre='Science Fiction')
        short = self._work(with_open_access_download=True, genre='Short Stories')
        history = self._work(with_open_access_download=True, genre='History', fiction=False)
        ignored = self._work(with_open_access_download=True)

        works = [romance, gothic, mystery, paranormal, scifi, short, history]
        for work in works:
            self.custom_list.add_entry(work.presentation_edition)

        cmd_args = incomplete_cmd_args + ['--storage-bucket', 'test.feed.bucket']

        with lower_lane_sample_size():
            self.script.do_run(uploader=self.uploader, cmd_args=cmd_args)

        # All of the expected files have been uploaded.
        eq_(15, len(self.uploader.content))
        expected_filenames = [
            'index', 'fiction', 'fiction_romance',
            'fiction_romance_author', 'fiction_mystery',
            'fiction_mystery_hard-boiled-mystery',
            'fiction_mystery_hard-boiled-mystery_author',
            'fiction_mystery_paranormal-mystery',
            'fiction_mystery_paranormal-mystery_author',
            'fiction_science-fiction', 'fiction_science-fiction_author',
            'fiction_general-fiction', 'fiction_general-fiction_author',
            'nonfiction', 'nonfiction_author'
        ]

        feeds_by_filename = dict()
        for rep in self.uploader.uploaded:
            static_feed_root = self.uploader.url('test.feed.bucket', '/')
            filename = rep.mirror_url.replace(static_feed_root, '')
            filename = filename.replace('.xml', '')
            feeds_by_filename[filename] = feedparser.parse(rep.content)

        # All of the filename that we thought would be uploaded have been.
        eq_(sorted(expected_filenames), sorted(feeds_by_filename))

        # And the books are where they should be.
        def assert_match(entry, work):
            entry.title = work.title
            entry.author = work.author

        romance_feed = feeds_by_filename['fiction_romance']
        eq_(2, len(romance_feed.entries))
        assert_match(romance_feed.entries[0], romance)
        assert_match(romance_feed.entries[1], gothic)

        hard_boiled_feed = feeds_by_filename['fiction_mystery_hard-boiled-mystery']
        [entry] = hard_boiled_feed.entries
        assert_match(entry, mystery)

        paranormal_feed = feeds_by_filename['fiction_mystery_paranormal-mystery']
        [entry] = paranormal_feed.entries
        assert_match(entry, paranormal)

        scifi_feed = feeds_by_filename['fiction_science-fiction']
        [entry] = scifi_feed.entries
        assert_match(entry, scifi)

        general_feed = feeds_by_filename['fiction_general-fiction']
        eq_(0, len(general_feed.entries))

        nonfiction_feed = feeds_by_filename['nonfiction']
        [entry] = nonfiction_feed.entries
        assert_match(entry, history)

        # While books in excluded genres or outside of the CustomList
        # are nowhere to be seen.
        all_entries = list()
        [all_entries.extend(feed.entries) for feed in feeds_by_filename.values()]

        for feed in feeds_by_filename.values():
            for entry in feed.entries:
                assert entry.title not in [short.title, ignored.title]
                assert entry.author not in [short.author, ignored.author]


class TestCSVFeedGenerationScript(DatabaseTest):

    def setup(self):
        super(TestCSVFeedGenerationScript, self).setup()

        # Instantiate a library.
        self._default_library

        self.uploader = DummyS3Uploader()
        self.script = CSVFeedGenerationScript(_db=self._db)

    @property
    def static_feed_root(self):
        return self.uploader.url('test.feed.bucket', '/')

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

        cmd_args = ['fake.csv', 'mta.librarysimplified.org',
                    '-u', '--urns', no_pool, urn1, urn2,
                    '--storage-bucket', 'test.feed.bucket']
        self.script.do_run(uploader=self.uploader, cmd_args=cmd_args)

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

        cmd_args = ['fake.csv', 'http://ls.org', '--page-size', '1',
                    '-u', '--urns', urns[0], urns[1],
                    '--storage-bucket', 'test.feed.bucket']
        self.script.do_run(uploader=self.uploader, cmd_args=cmd_args)

        eq_(4, len(self.uploader.uploaded))

        expected_filenames = [
            'index.xml', 'index_2.xml', 'index_author.xml',
            'index_author_2.xml'
        ]
        expected = [self.static_feed_root+f for f in expected_filenames]
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
        cmd_args = ['tests/files/scripts/mini.csv', url, '-u',
                    '--storage-bucket', 'test.feed.bucket']
        cmd_args += args

        with temp_config() as config:
            config[Configuration.POLICIES] = {
                Configuration.FEATURED_LANE_SIZE : 4
            }
            self.script.do_run(uploader=self.uploader, cmd_args=cmd_args)
        return w1, w2, w3, w4

    def test_run_with_csv(self):
        # An incorrect CSV document raises a ValueError when there are
        # also no URNs present.
        cmd_args = ['fake.csv', 'mta.librarysimplified.org', '-u',
                    '--storage-bucket', 'test.feed.bucket']
        assert_raises(
            ValueError, self.script.do_run, uploader=self.uploader,
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
            filter(Representation.mirror_url.like(self.static_feed_root+'%')).\
            all()
        created_filenames = [os.path.split(f[0])[1] for f in created]
        eq_(sorted(expected_filenames), sorted(created_filenames))

        def get_feed(filename):
            like_str = self.static_feed_root + filename + '.xml'
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
            Representation.mirror_url.like(self.static_feed_root+'%')
        ).all()

        eq_(9, len(representations))
        for r in representations:
            # The prefix has been inserted into the mirror url.
            r.mirror_url.startswith('https://ls.org/testing/')

    def test_run_with_license_link(self):
        """Confirms that licensing information added to the commandline
        is included in the feeds.
        """

        # Add a license url to the command line arguments.
        license_url = 'https://ls.org/license.html'
        license_args = ['--license', license_url]
        self.run_mini_csv(*license_args)

        for content in self.uploader.content:
            # All of the uploaded feeds have a license link with the
            # expected URL.
            feed = feedparser.parse(content)
            [license_link] = [l for l in feed.feed.links if l.rel == 'license']
            eq_(license_url, license_link.href)
            eq_('text/html', license_link.type)

    def test_make_lanes_from_csv(self):
        csv_filename = os.path.abspath('tests/files/scripts/sample.csv')
        result = self.script.make_lanes_from_csv(csv_filename)

        # It returns all the results we expect (even though they're
        # not all being tested in this method).
        eq_(4, len(result))
        top_level = result[0]
        youth_lane = result[2]

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

        # If the CSV includes non-English lanes, those lanes are given the
        # proper languages.
        csv_filename = os.path.abspath('tests/files/scripts/languages.csv')
        top_level = self.script.make_lanes_from_csv(csv_filename)[0]
        eq_(None, top_level.languages)

        expected = dict(Fiction=None, German=['ger'], Spanish=['spa'], French=['fre'])

        for lane in top_level.sublanes:
            eq_(expected[lane.name], lane.languages)
            for sublane in lane.sublanes:
                eq_(expected[lane.name], sublane.languages)
