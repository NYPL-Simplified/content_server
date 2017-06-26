#!/usr/bin/env python
"""Create Collections for each license-offering DataSource on the
Content Server.
"""
import os
import sys
import json
import logging
from nose.tools import set_trace

bin_dir = os.path.split(__file__)[0]
package_dir = os.path.join(bin_dir, "..")
sys.path.append(os.path.abspath(package_dir))

from core.model import (
    Collection,
    DataSource,
    ExternalIntegration,
    Library,
    get_one_or_create,
    production_session,
)
from core.util import LanguageCodes


_db = production_session()

def create_collection(data_source_name, name, protocol, url):
    name = name or data_source_name
    protocol = protocol or ExternalIntegration.OPDS_IMPORT

    collection, is_new = Collection.by_name_and_protocol(
        _db, name, protocol
    )

    if not collection.data_source:
        collection.external_integration.set_setting(
            Collection.DATA_SOURCE_NAME_SETTING, data_source_name
        )

    if protocol==ExternalIntegration.OPDS_IMPORT and url:
        collection.external_account_id = url

    if is_new:
        logging.info('Created collection for %s: %r' % (
            data_source_name, collection
        ))

try:
    collection_data = [
        (DataSource.GUTENBERG, None, ExternalIntegration.GUTENBERG, None),
        (DataSource.PLYMPTON, None, ExternalIntegration.DIRECTORY_IMPORT, None),
        (DataSource.UNGLUE_IT, None, None, u'https://unglue.it/api/opds/epub/'),
        (DataSource.STANDARD_EBOOKS, None, None, u'https://standardebooks.org/opds/all'),
    ]

    # Create a Collection for each language OPDS feed from FeedBooks
    FEEDBOOKS_BASE_URL = u'http://www.feedbooks.com/books/recent.atom?lang='
    FEEDBOOKS_BASE_NAME = unicode(DataSource.FEEDBOOKS + ' - ')
    for lang in ['en', 'es', 'fr', 'it', 'de']:
        opds_url = FEEDBOOKS_BASE_URL + lang

        language_name = LanguageCodes.english_names.get(lang)[0]
        full_name = FEEDBOOKS_BASE_NAME + language_name

        collection_args = [DataSource.FEEDBOOKS, full_name, None, opds_url]
        collection_data.append(tuple(collection_args))

    # Create each Collection
    for collection_args in collection_data:
        create_collection(*collection_args)
    _db.commit()
except Exception as e:
    _db.close()
