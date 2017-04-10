import json
import re
from datetime import datetime
from nose.tools import eq_, set_trace
from random import choice

from . import (
    DatabaseTest,
    sample_data,
)

from ..core.coverage import CoverageRecord
from ..core.model import (
    DataSource,
    Hyperlink,
    Identifier,
    Representation,
)
from ..core.util.epub import EpubAccessor

from ..config import (
    Configuration,
    temp_config,
)

from ..bibblio import (
    BibblioAPI,
    BibblioCoverageProvider,
)


class MockBibblioAPI(object):

    def __init__(self, error_class=None):
        self.error_class = None

    def create_content_item(self, identifier):
        if self.error_class:
            raise self.error_class()

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

        work = self._work(with_open_access_download=True, fiction=False)
        self.edition, _lp = self._edition(
            with_open_access_download=True,
            data_source_name=BibblioCoverageProvider.INSTANT_CLASSICS_SOURCES[0]
        )
        self.identifier = self.edition.primary_identifier

        # Create a work for fiction/nonfiction status.
        work = self._work(presentation_edition=self.edition, fiction=False)

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

        url = self._url + '/' + media_type
        source = DataSource.lookup(self._db, source_name)

        link, _ = identifier.add_link(
            Hyperlink.OPEN_ACCESS_DOWNLOAD, url, source,
            self.identifier.licensed_through
        )

        representation, _ = self._representation(
            url, media_type, content, mirrored=True
        )

        link.resource.data_source = source
        link.resource.representation = representation
        return representation

    def test_items_that_need_coverage(self):
        # Use a random acceptable DataSource type.
        num_data_sources = len(BibblioCoverageProvider.INSTANT_CLASSICS_SOURCES)
        source = BibblioCoverageProvider.INSTANT_CLASSICS_SOURCES[choice(range(0,num_data_sources))]

        # Create a nonfiction edition is not in the CustomList.
        listless_edition, _lp = self._edition(
            with_open_access_download=True, data_source_name=source)
        work = self._work(presentation_edition=listless_edition, fiction=False)

        # Create a nonfiction edition that already has coverage.
        covered_edition, _lp = self._edition(
            with_open_access_download=True, data_source_name=source)

        CoverageRecord.add_for(covered_edition, self.provider.output_source,
            operation=self.provider.operation)
        # A work is required to set the fiction status.
        work = self._work(presentation_edition=covered_edition, fiction=False)

        # Create a fiction edition without coverage.
        fiction_edition, _lp = self._edition(
            with_open_access_download=True, data_source_name=source)
        work = self._work(presentation_edition=fiction_edition)

        workless_edition, _lp = self._edition(
            with_open_access_download=True, data_source_name=source)

        # Add the listed editions to the targeted CustomList.
        self.custom_list.add_entry(covered_edition)
        self.custom_list.add_entry(fiction_edition)
        self.custom_list.add_entry(workless_edition)

        result = self.provider.items_that_need_coverage()

        # An edition that's not in the list is not included in the result.
        assert listless_edition.primary_identifier not in result
        # An edition without a Work isn't included either.
        assert workless_edition.primary_identifier not in result
        # An edition that's already covered is not in the list.
        assert covered_edition.primary_identifier not in result
        # By default, fiction is not included in the result.
        assert fiction_edition.primary_identifier not in result
        # An edition that's not covered is returned.
        assert self.edition.primary_identifier in result

        # When fiction is being included, the fiction edition is included.
        self.provider.fiction = True
        result = self.provider.items_that_need_coverage()
        assert fiction_edition.primary_identifier in result
        assert self.edition.primary_identifier in result
        # But other ignored editions are still ignored.
        assert listless_edition.primary_identifier not in result
        assert workless_edition.primary_identifier not in result
        assert covered_edition.primary_identifier not in result

    def test_process_item(self):
        representation = self.identifier.links[0].resource.representation
        representation.content = self.sample_file('180.epub')
        result = self.provider.process_item(self.identifier)

        eq_(self.identifier, result)

        # An equivalent identifier has been created.
        [equivalency] = self.identifier.equivalencies
        eq_(1.0, equivalency.strength)

        bibblio_id = equivalency.output
        eq_(Identifier.BIBBLIO_CONTENT_ITEM_ID, bibblio_id.type)
        eq_('510b1ee0-bede-4e24-a379-6a387f2dbb64', bibblio_id.identifier)

    def test_add_coverage_record_for(self):
        source = DataSource.lookup(self._db, DataSource.OVERDRIVE)
        other_identifier = self._identifier(identifier_type=Identifier.OVERDRIVE_ID)
        self.identifier.equivalent_to(source, other_identifier, 1)

        self.provider.add_coverage_record_for(self.identifier)

        # The item itself has coverage.
        [coverage_record] = [cr for cr in self.identifier.coverage_records
                             if cr.data_source == self.provider.output_source]
        eq_(CoverageRecord.SYNC_OPERATION, coverage_record.operation)

        # And so does its equivalent item!
        [other_record] = [cr for cr in other_identifier.coverage_records
                             if cr.data_source == self.provider.output_source]
        eq_(CoverageRecord.SYNC_OPERATION, other_record.operation)

    def test_content_item_from_identifier(self):
        self.add_representation(
            DataSource.PLYMPTON, Representation.EPUB_MEDIA_TYPE,
            self.sample_file('180.epub')
        )

        result = self.provider.content_item_from_identifier(self.identifier)
        eq_(['name', 'provider', 'text', 'url'], sorted(result.keys()))

        eq_('%s by %s' % (self.edition.title, self.edition.author), result.get('name'))
        eq_({ 'name' : DataSource.PLYMPTON }, result['provider'])
        eq_(self.provider.edition_permalink(self.edition), result['url'])

    def test_edition_permalink(self):
        urn = self.identifier.urn
        with temp_config() as config:
            config[Configuration.INTEGRATIONS][Configuration.CONTENT_SERVER_INTEGRATION] = {
                Configuration.URL : 'https://www.testing.code'
            }

            result = BibblioCoverageProvider.edition_permalink(self.edition)

        expected = 'https://www.testing.code/lookup?urn=%s' % urn
        eq_(expected, result)

    def test_get_full_text_uses_easiest_representation(self):

        html_rep = self.add_representation(
            DataSource.UNGLUE_IT, Representation.TEXT_HTML_MEDIA_TYPE,
            "<p>bleh bleh bleh</p>"
        )

        text_rep = self.add_representation(
            DataSource.GUTENBERG, Representation.TEXT_PLAIN, "blah blah blah"
        )

        epub_content = self.sample_file('180.epub')
        epub_rep = self.add_representation(
            DataSource.FEEDBOOKS, Representation.EPUB_MEDIA_TYPE, epub_content
        )

        text, data_source = self.provider.get_full_text(self.edition)
        # Despite the plethora of options, the plaintext is used.
        eq_("blah blah blah", text)
        eq_(DataSource.GUTENBERG, data_source.name)

        # If a text representation doesn't have content, a different
        # representation is selected.
        text_rep.content = None
        text, data_source = self.provider.get_full_text(self.identifier)
        # In this case, the html representation is selected. Its HTML
        # tags have been removed.
        eq_("bleh bleh bleh", text)
        eq_(DataSource.UNGLUE_IT, data_source.name)

        # When text isn't readily available, content comes from the EPUB.
        html_rep.resource.representation = None
        text, data_source = self.provider.get_full_text(self.edition)
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
