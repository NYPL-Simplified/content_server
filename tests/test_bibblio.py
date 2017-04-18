import json
import re
from datetime import datetime
from nose.tools import eq_, set_trace
from random import choice
from urllib import quote

from . import (
    DatabaseTest,
    sample_data,
)

from ..core.coverage import (
    CoverageFailure,
    WorkCoverageRecord,
)
from ..core.model import (
    DataSource,
    DeliveryMechanism,
    Hyperlink,
    Identifier,
    Representation,
    RightsStatus,
)
from ..core.util.epub import EpubAccessor
from ..core.util.http import BadResponseException

from ..config import (
    Configuration,
    temp_config,
)

from ..bibblio import (
    BibblioAPI,
    BibblioCoverageProvider,
)


class MockBibblioAPI(object):

    def __init__(self, error=None):
        self.error = None

    def create_content_item(self, identifier):
        if self.error:
            raise self.error

        return json.loads("""{
          "catalogueId": "9e904824-5f98-4281-99be-931a8d68854e",
          "contentItemId": "510b1ee0-bede-4e24-a379-6a387f2dbb64",
          "name": "Air Pollution",
          "url": "http://example.com/path/to/content-item",
          "text": "Air pollution consists of chemicals or particles in the air that can harm the health of humans, animals, and plants. It also damages buildings. Pollutants in the air take many forms. They can be gases, solid particles, or liquid droplets. Sources of Air Pollution Pollution enters the Earth's atmosphere in many different ways. Most air pollution is created by people, taking the form of emissions from factories, cars, planes, or aerosol cans. [...]",
          "headline": "One of the most rapidly developing nations in the world also has one of the highest rates of air pollution.",
          "description": "Air pollution consists of chemicals or particles in the air that can harm the health of humans, animals, and plants. It also damages buildings.",
          "keywords": ["environment", "atmosphere"],
          "learningResourceType": "encyclopedia article",
          "thumbnail": {
            "contentUrl": "http://example.com/path/to/thumbnail"
          },
          "image": {
            "contentUrl": "http://example.com/full-size/air-pollution.jpg"
          },
          "moduleImage": {
            "contentUrl": "http://example.com/thumbnails/air-pollution-300x300.jpg"
          },
          "video": {
            "embedUrl": "http://example.com/embed/video-id"
          },
          "dateCreated": "2011-04-04T11:42:17.082Z",
          "dateModified": "2011-04-04T11:42:17.082Z",
          "datePublished": "2011-04-04T11:42:17.082Z",
          "provider": { "name": "Distribution Inc." },
          "publisher": { "name": "Publication Inc." }
        }""")


class TestBibblioAPI(DatabaseTest):

    def test_from_config(self):
        # When nothing has been configured, nothing is returned.
        with temp_config() as config:
            config['integrations'][Configuration.BIBBLIO_INTEGRATION] = {}
            result = BibblioAPI.from_config(self._db)
            eq_(None, result)

        # When there's only a partial configuration, None is returned.
        with temp_config() as config:
            config['integrations'][Configuration.BIBBLIO_INTEGRATION] = {
                Configuration.BIBBLIO_ID : 'id'
            }
            result = BibblioAPI.from_config(self._db)
            eq_(None, result)

        with temp_config() as config:
            config['integrations'][Configuration.BIBBLIO_INTEGRATION] = {
                Configuration.BIBBLIO_ID : 'id',
                Configuration.BIBBLIO_SECRET : 'secret'
            }
            result = BibblioAPI.from_config(self._db)
            eq_(True, isinstance(result, BibblioAPI))
            eq_('id', result.client_id)
            eq_('secret', result.client_secret)

    def test_timestamp(self):
        item = {'name' : 'banana'}
        expected_format = re.compile('\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?Z')
        now = datetime.utcnow()

        # Adds a 'dateModified' timestamp to a dictionary / JSON object
        result = BibblioAPI.set_timestamp(item)
        assert 'dateModified' in result
        # It's a string.
        assert isinstance(result['dateModified'], basestring)
        assert result['dateModified'] > (now.isoformat() + 'Z')

        assert 'dateCreated' not in result

        # Adds 'dateModified' and 'dateCreated' timestamps when specified
        result = BibblioAPI.set_timestamp(item, create=True)
        assert 'dateCreated' in result
        assert 'dateModified' in result
        eq_(result['dateCreated'], result['dateModified'])
        assert isinstance(result['dateCreated'], basestring)
        assert result['dateCreated'] > (now.isoformat() + 'Z')


class TestBibblioCoverageProvider(DatabaseTest):

    def setup(self):
        super(TestBibblioCoverageProvider, self).setup()

        self.edition, _lp = self._edition(
            with_open_access_download=True,
            data_source_name=BibblioCoverageProvider.INSTANT_CLASSICS_SOURCES[0]
        )
        self.identifier = self.edition.primary_identifier
        # Create a work for fiction/nonfiction status.
        self.work = self._work(presentation_edition=self.edition, fiction=False)

        self.custom_list, _ = self._customlist(
            foreign_identifier=u'fake-list', name=u'Fake List',
            data_source_name=DataSource.LIBRARY_STAFF, num_entries=0)
        # Add the edition to the list.
        self.custom_list.add_entry(self.edition)

        self.provider = BibblioCoverageProvider(
            self._db, self.custom_list.foreign_identifier, api=MockBibblioAPI()
        )

    def sample_file(self, filename):
        return sample_data(filename, 'bibblio')

    def add_representation(self, source_name, media_type, content,
                           identifier=None):
        """Utility method to add representations to the local identifier"""
        identifier = identifier or self.identifier
        pool = identifier.licensed_through
        url = self._url + '/' + media_type
        source = DataSource.lookup(self._db, source_name)

        link, _ = identifier.add_link(
            Hyperlink.OPEN_ACCESS_DOWNLOAD, url, source, pool
        )

        representation, _ = self._representation(
            url, media_type, content, mirrored=True
        )

        link.resource.data_source = source
        link.resource.representation = representation
        pool.set_delivery_mechanism(
            media_type, DeliveryMechanism.NO_DRM,
            RightsStatus.GENERIC_OPEN_ACCESS,
            link.resource,
        )
        return representation

    def test_items_that_need_coverage(self):
        # Use a random acceptable DataSource type.
        end = len(self.provider.INSTANT_CLASSICS_SOURCES)
        source_name = self.provider.INSTANT_CLASSICS_SOURCES[choice(range(0,end))]
        source = DataSource.lookup(self._db, source_name)

        # Create a nonfiction work that is not in the CustomList.
        listless = self._work(with_open_access_download=True, fiction=False)

        # Create a nonfiction work that's in the CustomList, even though
        # its Work isn't directly connected. (A CustomListEntry is not
        # always linked with a Work, though it always has an edition.)
        edition_listed = self._work(with_open_access_download=True, fiction=False)
        entry, _is_new = self.custom_list.add_entry(edition_listed.presentation_edition)
        entry.work = None

        # Create a work that already has coverage.
        covered = self._work(with_open_access_download=True, fiction=False)
        WorkCoverageRecord.add_for(covered, operation=self.provider.operation)

        # Create a fiction work without coverage.
        fiction = self._work(with_open_access_download=True)

        # And here's a nonfiction work without coverage.
        nonfiction = self.work

        for work in [covered, fiction, nonfiction]:
            self.custom_list.add_entry(work.presentation_edition)

        # Give all the works a target DataSource.
        works = [listless, edition_listed, covered, fiction]
        for work in works:
            work.presentation_edition.data_source = source
            [setattr(lp, 'data_source', source) for lp in work.license_pools]

        result = self.provider.items_that_need_coverage()
        # A Work that's not in the list is not included in the result.
        assert listless not in result
        # Unless its presentation_edition is in the list!
        assert edition_listed in result
        # A Work that's already covered is not included.
        assert covered not in result
        # By default, fiction is not included in the result.
        assert fiction not in result
        # A nonfiction Work that's not covered is included in the result.
        assert nonfiction in result

        # When fiction is being included, the fiction edition is included.
        self.provider.fiction = True
        result = self.provider.items_that_need_coverage()
        assert fiction in result
        assert nonfiction in result
        assert edition_listed in result
        # But other ignored editions are still ignored.
        assert listless not in result
        assert covered not in result

    def test_process_item(self):
        representation = self.identifier.links[0].resource.representation
        representation.content = self.sample_file('180.epub')

        def process_item(item):
            with temp_config() as config:
                config[Configuration.INTEGRATIONS][Configuration.CONTENT_SERVER_INTEGRATION] = {
                    Configuration.URL : 'https://www.testing.code'
                }
                return self.provider.process_item(item)

        result = process_item(self.work)
        eq_(self.work, result)

        # An equivalent identifier has been created for the original identifier.
        [equivalency] = self.identifier.equivalencies
        eq_(1.0, equivalency.strength)

        bibblio_id = equivalency.output
        eq_(Identifier.BIBBLIO_CONTENT_ITEM_ID, bibblio_id.type)
        eq_('510b1ee0-bede-4e24-a379-6a387f2dbb64', bibblio_id.identifier)

        def assert_is_coverage_failure_for(result, item, data_source, transient=True):
            eq_(True, isinstance(result, CoverageFailure))
            eq_(item, result.obj)
            eq_(data_source, result.data_source)
            eq_(transient, result.transient)

        # When the API raises an error, a CoverageFailure is returned.
        self.provider.api.error = BadResponseException(
            'fake-bibblio.org', 'Got a bad 401')
        result = process_item(self.work)

        assert_is_coverage_failure_for(
            result, self.work, self.provider.output_source
        )
        assert 'fake-bibblio.org' in result.exception
        assert '401' in result.exception

        # In fact, when any error is raised, an appropriate
        # CoverageFailure is returned.
        self.provider.api.error = ValueError("B A N A N A S")
        result = process_item(self.work)
        assert_is_coverage_failure_for(
            result, self.work, self.provider.output_source
        )
        eq_("B A N A N A S", result.exception)

    def test_content_item_from_work(self):
        self.add_representation(
            DataSource.PLYMPTON, Representation.EPUB_MEDIA_TYPE,
            self.sample_file('180.epub')
        )

        with temp_config() as config:
            config[Configuration.INTEGRATIONS][Configuration.CONTENT_SERVER_INTEGRATION] = {
                Configuration.URL : 'https://www.testing.code'
            }
            result = self.provider.content_item_from_work(self.work)

        eq_(['name', 'provider', 'text', 'url'], sorted(result.keys()))
        eq_('%s by %s' % (self.edition.title, self.edition.author), result.get('name'))
        eq_({ 'name' : DataSource.PLYMPTON }, result['provider'])

        url_urn = quote(self.identifier.urn).replace('/', '%2F')
        expected_url = u'https://www.testing.code/lookup?urn=' + url_urn
        eq_(expected_url, result['url'])

    def test_edition_permalink(self):
        urn = quote(self.identifier.urn).replace('/', '%2F')
        with temp_config() as config:
            config[Configuration.INTEGRATIONS][Configuration.CONTENT_SERVER_INTEGRATION] = {
                Configuration.URL : 'https://www.testing.code'
            }

            result = BibblioCoverageProvider.edition_permalink(self.edition)

        expected = 'https://www.testing.code/lookup?urn=%s' % urn
        eq_(expected, result)

    def test_get_full_text_uses_easiest_representation(self):
        epub_content = self.sample_file('180.epub')
        epub_rep = self.add_representation(
            DataSource.FEEDBOOKS, Representation.EPUB_MEDIA_TYPE, epub_content
        )

        text_rep = self.add_representation(
            DataSource.GUTENBERG, Representation.TEXT_PLAIN, 'blah'
        )

        text, data_source = self.provider.get_full_text(self.work)
        # The plaintext is used instead of the EPUB.
        eq_('blah', text)
        eq_(DataSource.GUTENBERG, data_source.name)

        # If a text representation doesn't have content, a different
        # text representation is selected.
        text_rep.content = None
        html_rep = self.add_representation(
            DataSource.UNGLUE_IT, Representation.TEXT_HTML_MEDIA_TYPE,
            '<p>BLEH</p>'
        )
        # In this case, the html representation is selected. Its HTML
        # tags have been removed.
        text, data_source = self.provider.get_full_text(self.work)
        eq_('BLEH', text)
        eq_(DataSource.UNGLUE_IT, data_source.name)

        # When text isn't readily available, content comes from the EPUB.
        html_rep.resource.representation = None
        text, data_source = self.provider.get_full_text(self.work)
        eq_(True, 'Dostoyevsky' in text)
        eq_(DataSource.FEEDBOOKS, data_source.name)

    def test_extract_plaintext_from_epub(self):
        epub = self.sample_file('180.epub')
        result = None

        with EpubAccessor.open_epub('677.epub', content=epub) as (zip_file, package_path):
            result = BibblioCoverageProvider.extract_plaintext_from_epub(zip_file, package_path)

        # We get back a string.
        eq_(True, isinstance(result, str))

        # To save space, it doesn't have any doubled whitespace.
        double_space = re.compile('\s\s')
        eq_(None, double_space.match(result))

        # Also, in accordance with the Bibblio API, it's not
        # over 200KB. Even though the book (The Brothers Karamazov)
        # is definitely longer.
        eq_(
            BibblioCoverageProvider.BIBBLIO_TEXT_LIMIT,
            len(result)
        )
