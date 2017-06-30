#!/usr/bin/env python
"""Move integration details from the Configuration file into the
database as ExternalIntegrations
"""
import os
import sys
import json
import logging
import uuid
from nose.tools import set_trace

bin_dir = os.path.split(__file__)[0]
package_dir = os.path.join(bin_dir, "..")
sys.path.append(os.path.abspath(package_dir))

from config import Configuration
from core.model import (
    ConfigurationSetting,
    ExternalIntegration as EI,
    Library,
    create,
    production_session,
)

log = logging.getLogger(name="Content Server configuration import")

def log_import(integration_or_setting):
    log.info("CREATED: %r" % integration_or_setting)


_db = production_session()
try:
    Configuration.load()
    library = Library.default(_db)
    if not library:
        library, ignore = create(
            _db, Library, name=u'default', short_name=u'default',
            uuid=unicode(uuid.uuid4())
        )
        library.is_default = True

    # Create the Bibblio integration.
    bibblio_conf = Configuration.integration('Bibblio')
    if bibblio_conf:
        bibblio = EI(
            name=EI.BIBBLIO,
            protocol=EI.BIBBLIO,
            goal=EI.METADATA_GOAL
        )
        _db.add(bibblio)
        bibblio.username = bibblio_conf.get('client_id')
        bibblio.password = bibblio_conf.get('client_secret')
        log_import(bibblio)

    # Create the Metadata Wrangler configuration.
    metadata_wrangler_conf = Configuration.integration('Metadata Wrangler')
    if metadata_wrangler_conf:
        wrangler = EI(
            name=EI.METADATA_WRANGLER,
            protocol=EI.METADATA_WRANGLER,
            goal=EI.METADATA_GOAL
        )
        _db.add(wrangler)
        wrangler.url = metadata_wrangler_conf.get('url')
        wrangler.username = metadata_wrangler_conf.get('client_id')
        wrangler.password = metadata_wrangler_conf.get('client_secret')
        log_import(wrangler)

    # Get the base url.
    content_server_conf = Configuration.integration('Content Server')
    if content_server_conf:
        url = content_server_conf.get('url')
        setting = ConfigurationSetting.sitewide(_db, Configuration.BASE_URL_KEY)
        setting.value = url
        log_import(setting)

    # Copy facet configuration to the library.
    facet_policy = Configuration.policy("facets", default={})

    default_enabled = Configuration.DEFAULT_ENABLED_FACETS
    enabled = facet_policy.get("enabled", default_enabled)
    for k, v in enabled.items():
        library.enabled_facets_setting(unicode(k)).value = unicode(json.dumps(v))

    default_facets = Configuration.DEFAULT_FACET
    default = facet_policy.get("default", default_facets)
    for k, v in default.items():
        library.default_facet_setting(unicode(k)).value = unicode(v)

    log.info('Default facets imported')

except Exception as e:
    # Catch any error and roll back the database so the full
    # migration can be run again without raising integrity exceptions
    # for duplicate integrations.
    _db.rollback()
    raise e

finally:
    _db.commit()
    _db.close()
