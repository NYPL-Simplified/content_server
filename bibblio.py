import json
import logging
import os
import re
from datetime import datetime, timedelta
from nose.tools import set_trace
from urlparse import urlparse

from bs4 import BeautifulSoup
from sqlalchemy.orm import eagerload

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

from config import Configuration


class BibblioAPI(object):

    API_ENDPOINT = u'https://api.bibblio.org/v1/'
    CATALOGUES_ENDPOINT = API_ENDPOINT + u'catalogues/'
    CONTENT_ITEMS_ENDPOINT = API_ENDPOINT + u'content-items/'

    TOKEN_CONTENT_TYPE = u'application/x-www-form-urlencoded'

    log = logging.getLogger(__name__)

    @classmethod
    def from_config(cls, _db):
        config = Configuration.integration(Configuration.BIBBLIO_INTEGRATION)
        if not (config and len(config.values())==2):
            return None

        client_id = config.get(Configuration.BIBBLIO_ID)
        client_secret = config.get(Configuration.BIBBLIO_SECRET)
        return cls(_db, client_id, client_secret)

    @classmethod
    def set_timestamp(cls, resource, create=False):
        """Adds a timestamp to a resource (catalogue or content item)"""

        now = datetime.utcnow().isoformat() + 'Z'
        resource['dateModified'] = now
        if create:
            resource['dateCreated'] = now

        return resource

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

    @property
    def default_headers(self):
        return {
            'Authorization': 'Bearer '+self.token,
            'Content-Type': 'application/json'
        }

    def refresh_credential(self, credential):
        url = self.API_ENDPOINT + 'token'
        headers = {'Content-Type': self.TOKEN_CONTENT_TYPE}
        client_details = dict(client_id=self.client_id, client_secret=self.client_secret)

        response = HTTP.post_with_timeout(url, client_details, headers=headers)
        data = response.json()

        credential.credential = data.get('access_token')
        expires_in = data.get('expires_in')
        credential.expires = datetime.utcnow() + timedelta(0, expires_in * 0.9)
        self._credential = credential

    def create_catalogue(self, name, description=None):
        catalogue = dict(name=name)
        if description:
            catalogue['description'] = description

        catalogue = self.set_timestamp(catalogue, create=True)
        catalogue = json.dumps(catalogue)

        response = HTTP.post_with_timeout(
            self.CATALOGUES_ENDPOINT, catalogue, headers=self.default_headers
        )
        if response.status_code == 201:
            catalogue = response.json()
            name = catalogue.get('name')
            catalogue_id = catalogue.get('catalogueId')

            self.log.info(
                "New catalogue '%s' created with ID: %s",
                name, catalogue_id
            )
            return catalogue

    def get_catalogue(self, name):
        response = HTTP.get_with_timeout(
            self.CATALOGUES_ENDPOINT, headers=self.default_headers
        )

        if response.status_code == 200:
            catalogues = response.json().get('results')
            catalogue = filter(lambda c: c.get('name') == name, catalogues)
            if catalogue:
                return catalogue[0]
            else:
                return None

    def create_content_item(self, content_item):
        content_item = self.set_timestamp(content_item, create=True)
        content_item = json.dumps(content_item)
        response = HTTP.post_with_timeout(
            self.CONTENT_ITEMS_ENDPOINT, content_item,
            headers=self.default_headers
        )

        if response.status_code == 201:
            content_item = response.json()
            name = content_item.get('name')
            content_item_id = content_item.get('contentItemId')

            self.log.info(
                "New content item created for '%s': '%s'",
                name, content_item_id
            )
            return content_item


class BibblioCoverageProvider(object):

    BIBBLIO_TEXT_LIMITATION = 200000
    TEXT_MEDIA_TYPES = [
        Representation.TEXT_PLAIN,
        Representation.TEXT_HTML_MEDIA_TYPE,
    ]

    def __init__(self, _db, api=None, catalogue_id=None):
        self._db = _db
        self.api = api or BibblioAPI.from_config(self._db)
        self.catalogue_id = catalogue_id

    def process_item(self, item, force=False):
        pass

    def content_item_from_edition(self, edition):
        name = edition.title + ' by ' + edition.author
        url = self.edition_permalink(edition)
        text, data_source = self.get_full_text(edition)
        provider = dict(name=data_source.name)

        content_item = dict(
            name=name, url=url, text=text, provider=provider
        )
        if self.catalogue_id:
            content_item['catalogueId'] = self.catalogue_id

        return content_item

    def edition_permalink(self, edition):
        base_url = Configuration.integration_url(
            Configuration.CONTENT_SERVER_INTEGRATION, required=True
        )
        scheme, host = urlparse(base_url)[0:2]
        base_url = '://'.join([scheme, host])

        urn = edition.primary_identifier.urn
        permalink = '%s/lookup?urn=%s' % (base_url, urn)
        return permalink

    def get_full_text(self, edition_or_identifier):
        identifier = edition_or_identifier
        if not isinstance(edition_or_identifier, Identifier):
            identifier = edition_or_identifier.primary_identifier

        representations = self._db.query(Representation)\
            .join(Representation.resource).join(Resource.links)\
            .join(Hyperlink.identifier).filter(
                Identifier.id==identifier.id,
                Hyperlink.rel==Hyperlink.OPEN_ACCESS_DOWNLOAD)\
            .options(eagerload(Representation.resource))

        text_representation = representations.filter(
            Representation.media_type.in_(self.TEXT_MEDIA_TYPES),
            Representation.content.isnot(None))\
            .limit(1).all()

        if text_representation:
            # Get the full text if it's readily available.
            [representation] = text_representation
            full_text = self._html_to_text(representation.content)
            full_text = self._shrink_text(full_text)
            return full_text, representation.resource.data_source

        # If it's gotta be an EPUB, make sure it matches the download url.
        epub_representation = representations.filter(
            Representation.media_type==Representation.EPUB_MEDIA_TYPE)\
            .limit(1).all()

        if not epub_representation:
            # Access to the full text isn't available.
            return None, None

        [representation] = epub_representation
        url = representation.url
        content = representation.content
        with EpubAccessor.open_epub(url, content=content) as (zip_file, package_path):
            return (
                self.extract_plaintext_from_epub(zip_file, package_path),
                representation.resource.data_source
            )

    def extract_plaintext_from_epub(self, zip_file, package_document_path):
        spine, manifest = EpubAccessor.get_elements_from_package(
            zip_file, package_document_path, ['spine', 'manifest']
        )

        text_basefiles = list()
        for child in spine:
            if child.tag == '{%s}itemref' % EpubAccessor.IDPF_NAMESPACE:
                text_basefiles.append(child.get('idref'))

        epub_item_elements = list()
        for child in manifest:
            if (child.tag == '{%s}item' % EpubAccessor.IDPF_NAMESPACE
                and child.get('id') in text_basefiles):
                epub_item_elements.append(child)

        # Sort the items by their order in the spine.
        epub_item_elements.sort(key=lambda el: text_basefiles.index(el.get('id')))

        # Get the full EPUB filename for each text document.
        text_filenames = [el.get('href') for el in epub_item_elements]
        full_path = os.path.split(package_document_path)[0]
        text_filenames = [os.path.join(full_path, f) for f in text_filenames]

        accumulated_text = u''
        for filename in text_filenames:
            with zip_file.open(filename) as text_file:
                raw_text = self._html_to_text(text_file.read())
                accumulated_text += raw_text

        return self._shrink_text(accumulated_text)

    def _shrink_text(self, text):
        """Removes excessive whitespace and shortens text according to
        the API requirements
        """
        text = re.sub(r'(\s?\n\s+|\s+\n\s?)+', '\n', text)
        text = re.sub(r'\t{2,}', '\t', text)
        text = re.sub(r' {2,}', ' ', text)

        return text.encode('utf-8')[0:self.BIBBLIO_TEXT_LIMITATION]

    def _html_to_text(self, html_content):
        """Returns raw text from HTML"""
        return BeautifulSoup(html_content, 'lxml').get_text()
