from datetime import datetime, timedelta
from nose.tools import set_trace

from config import Configuration

from core.model import (
    Credential,
    DataSource,
    Hyperlink,
    Identifier,
    Representation,
    Resource,
)
from core.util.epub import EpubAccessor
from core.util.http import HTTP


class BibblioAPI(object):

    API_ENDPOINT = u"https://api.bibblio.org/v1/"
    
    TOKEN_CONTENT_TYPE = u"application/x-www-form-urlencoded"
    TOKEN_TYPE = u"Bearer "

    @classmethod
    def from_config(cls, _db):
        config = Configuration.integration(Configuration.BIBBLIO_INTEGRATION)
        if not (config and len(config.values())==2):
            return None

        client_id = config.get(Configuration.BIBBLIO_ID)
        client_secret = config.get(Configuration.BIBBLIO_SECRET)
        return cls(_db, client_id, client_secret)

    def __init__(self, _db, client_id, client_secret):
        self._db = _db
        self.client_id = client_id
        self.client_secret = client_secret
        self._credential = None

    @property
    def source(self):
        return DataSource.lookup(self._db, DataSource.BIBBLIO)

    @property
    def token(self):
        if (self._credential and
            self._credential.expires <= datetime.utcnow()):
            return self._credential.credential

        credential = Credential.lookup(
            self._db, self.source, None, None, self.refresh_credential
        )
        return credential.credential

    def refresh_credential(self, credential):
        url = self.API_ENDPOINT + 'token'
        headers = {'Content-Type' : self.TOKEN_CONTENT_TYPE}
        payload = dict(client_id=self.client_id, client_secret=self.client_secret)

        response = HTTP.post_with_timeout(url, payload, headers=headers)
        data = response.json()

        credential.credential = data.get('access_token')
        expires_in = data.get('expires_in')
        credential.expires = datetime.utcnow() + timedelta(0, expires_in * 0.9)
        self._credential = credential


class BibblioContentExtractor(object):

    EXTRACTABLE_MEDIA_TYPES = [
        Representation.TEXT_PLAIN,
        Representation.TEXT_HTML_MEDIA_TYPE,
        Representation.EPUB_MEDIA_TYPE,
    ]

    def __init__(self, _db):
        self._db = _db

    def content_item_from_edition(self, edition):
        pass

    def get_plaintext(self, edition_or_identifier):
        identifier = edition_or_identifier
        if not isinstance(edition_or_identifier, Identifier):
            identifier = edition_or_identifier.primary_identifier

        epub_url = identifier.licensed_through.open_access_download_url
        epub_representations = self._db.query(Representation)\
            .join(Representation.resource).join(Resource.hyperlink)\
            .join(Hyperlink.identifier).filter(
                Identifier.id==identifier.id,
                Hyperlink.rel==Hyperlink.OPEN_ACCESS_DOWNLOAD,
                Representation.media_type.in_(self.EXTRACTABLE_MEDIA_TYPES),
                Representation.content != None).all()

        # Get the easiest Representation to extract from.
        epub_representations.sort(
            key=lambda r: ACCEPTABLE_MEDIA_TYPES.index(r.media_type)
        )
        epub_representation = epub_representations[0]
        if epub_representation.media_type == Representation.TEXT_PLAIN:
            return epub_representation.content

        if epub_representation.media_type == Representation.TEXT_HTML_MEDIA_TYPE:
            return self._html_to_text(epub_representation.content)

        epub_representations = filter(lambda r: r.url==epub_url, epub_representations)
        content = epub_representations[0].content
        with EpubAccessor.open_epub(epub_url, content=content) as (zip_file, package_path)
            return self.extract_plaintext_from_epub(zip_file, package_path)

    def extract_plaintext_from_epub(zip_file, package_document_path):
        spine, manifest = EpubAccessor.get_elements_from_package(
            zip_file, package_document_path, ['spine', 'manifest']
        )

        text_basefiles = list()
        for child in spine:
            if child.tag == "{%s}itemref" % EpubAccessor.IDPF_NAMESPACE:
                print child.get('idref')
                text_basefiles.append(child.get('idref'))

        epub_item_elements = list()
        for child in manifest:
            if (child.tag == "{%s}item" % EpubAccessor.IDPF_NAMESPACE
                and child.get('id') in text_basefiles):
                epub_item_elements.append(child)

        # Sort the items by their order in the spine.
        epub_item_elements.sort(key=lambda el: text_basefiles.index(el.get('id')))

        # Get the full EPUB filename for each text document.
        text_filenames = [el.get('href') for el in epub_item_elements]
        full_path = os.path.split(package_document_path)[0]
        text_filenames = [os.path.join(full_path, f) for f in text_filenames]

        accumulated_text = ""
        for filename in text_filenames:
            with zip_file.open(filename) as text_file:
                raw_text = self._html_to_text(text_file.read())
                accumulated_text += raw_text

        return accumulated_text

    def _html_to_text(self, html_content):
        return BeautifulSoup(html_content, "lxml").get_text()
