import re
from datetime import datetime
from nose.tools import eq_, set_trace

from . import (
    DatabaseTest,
    sample_data,
)

from ..core.model import(
    DataSource,
    Hyperlink,
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

    @classmethod
    def from_config(cls, *args, **kwargs):
        return cls()


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

        self.edition = self._edition()
        self.identifier = self.edition.primary_identifier

        api = MockBibblioAPI.from_config()
        self.provider = BibblioCoverageProvider(self._db, api)

    def sample_file(self, filename):
        return sample_data(filename, 'bibblio')

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

        def add_representation(source_name, media_type, content):
            url = self._url + '/' + media_type
            source = DataSource.lookup(self._db, source_name)

            link, _ = self.identifier.add_link(
                Hyperlink.OPEN_ACCESS_DOWNLOAD, url, source,
                self.identifier.licensed_through
            )

            representation, _ = self._representation(
                url, media_type, content, mirrored=True
            )

            link.resource.data_source = source
            link.resource.representation = representation
            return representation

        html_rep = add_representation(
            DataSource.UNGLUE_IT, Representation.TEXT_HTML_MEDIA_TYPE,
            "<p>bleh bleh bleh</p>"
        )

        text_rep = add_representation(
            DataSource.GUTENBERG, Representation.TEXT_PLAIN, "blah blah blah"
        )

        epub_content = self.sample_file('180.epub')
        epub_rep = add_representation(
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
