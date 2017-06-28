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
    Edition,
    ExternalIntegration,
    Library,
    LicensePool,
    get_one_or_create,
    production_session,
)
from core.util import (
    fast_query_count,
    LanguageCodes,
)

from scripts import (
    DirectoryImportScript,
    OPDSImportScript,
)
from feedbooks import FeedbooksOPDSImporter
from unglueit import UnglueItImporter

_db = production_session()

try:
    # Create Collections generated by directory import.
    directory_import = DirectoryImportScript(_db=_db)
    for data_source_name in [DataSource.PLYMPTON, DataSource.ELIB]:
        directory_import.create_collection(data_source_name)

    # Create Collections generated by OPDS import with importer classes.
    opds_importers = {
        FeedbooksOPDSImporter : DataSource.FEEDBOOKS,
        UnglueItImporter : DataSource.UNGLUE_IT,
    }
    for importer_class, data_source_name in opds_importers.items():
        OPDSImportScript(importer_class, data_source_name, _db=_db)

    # Create a StandardEbooks Collection.
    OPDSImportScript(object(), DataSource.STANDARD_EBOOKS, _db=_db,
        collection_data=dict(url=u'https://standardebooks.org/opds/all'))

    # Create a Gutenberg Collection.
    gutenberg, is_new = Collection.by_name_and_protocol(
        _db, DataSource.GUTENBERG, ExternalIntegration.GUTENBERG
    )
    if not gutenberg.data_source:
        gutenberg.external_integration.set_setting(
            Collection.DATA_SOURCE_NAME_SETTING, DataSource.GUTENBERG
        )
    if is_new:
        library = Library.default(_db)
        gutenberg.libraries.append(library)
        logging.info('CREATED Collection for %s: %r' % (
            DataSource.GUTENBERG, gutenberg))

    _db.commit()

    # Alright, all the Collections have been created. Let's update the
    # LicensePools now.
    base_query = _db.query(LicensePool).filter(LicensePool.collection_id==None)

    single_collection_sources = [
        DataSource.PLYMPTON, DataSource.ELIB, DataSource.UNGLUE_IT,
        DataSource.STANDARD_EBOOKS, DataSource.GUTENBERG
    ]
    for data_source_name in single_collection_sources:
        # Get the Collection.
        collection = Collection.by_datasource(_db, data_source_name).one()

        # Find LicensePools with the matching DataSource.
        source = DataSource.lookup(_db, data_source_name)
        qu = base_query.filter(LicensePool.data_source==source)
        qu.update({LicensePool.collection_id : collection.id})

        logging.info('UPDATED: %d LicensePools given Collection %r' % (
            int(fast_query_count(qu)), collection))
        _db.commit()

    # Now update the FeedBooks LicensePools, which have to take language
    # into account.
    feedbooks = DataSource.lookup(_db, DataSource.FEEDBOOKS)
    base_query = _db.query(LicensePool.id)\
        .filter(LicensePool.data_source==feedbooks)\
        .join(LicensePool.presentation_edition)

    for lang in ['en', 'es', 'fr', 'it', 'de']:
        # Get the Collection for each language.
        language = LanguageCodes.english_names.get(lang)[0]
        name = FeedbooksOPDSImporter.BASE_COLLECTION_NAME + language
        collection, ignore = Collection.by_name_and_protocol(
            _db, name, ExternalIntegration.OPDS_IMPORT
        )

        # Find LicensePools with an Edition in that language.
        edition_lang = LanguageCodes.two_to_three[lang]
        lang_query = base_query.filter(Edition.language==edition_lang)
        lang_query = lang_query.subquery()

        # Give them the proper Collection.
        qu = _db.query(LicensePool).filter(LicensePool.id.in_(lang_query))
        qu.update(
            {LicensePool.collection_id : collection.id},
            synchronize_session='fetch'
        )

        logging.info('UPDATED: %d LicensePools given Collection %r' % (
            int(fast_query_count(qu)), collection))
        _db.commit()

except Exception as e:
    _db.close()
    logging.error(
        "Fatal exception while running script: %s", e,
        exc_info=e
    )
