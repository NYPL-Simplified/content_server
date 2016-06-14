import datetime
import os
import json
import logging
import random
import re
import random
import requests
import time
import shutil
import tarfile
from urlparse import urljoin, urlparse
from StringIO import StringIO

from sqlalchemy.orm import aliased

from nose.tools import set_trace

import rdflib
from rdflib import Namespace

from core.coverage import CoverageProvider
from core.model import (
    get_one_or_create,
    CirculationEvent,
    Contributor,
    Edition,
    DataSource,
    Hyperlink,
    Measurement,
    Representation,
    Resource,
    RightsStatus,
    Identifier,
    LicensePool,
    Subject,
    DeliveryMechanism,
)

from core.metadata_layer import (
    ContributorData,
    Metadata,
    LinkData,
    IdentifierData,
    FormatData,
    MeasurementData,
    SubjectData,
    CirculationData,
    ReplacementPolicy,
)

from core.monitor import Monitor
from core.util import LanguageCodes

class GutenbergAPI(object):

    """An 'API' to Project Gutenberg's RDF catalog.

    A bit different from the other APIs since the data comes over the
    web all at once in one big BZ2 file.
    """

    ID_IN_FILENAME = re.compile("pg([0-9]+).rdf")

    EVENT_SOURCE = "Gutenberg"
    FILENAME = "rdf-files.tar.bz2"

    ONE_DAY = 60 * 60 * 24


    MIRRORS = [
        # "http://www.gutenberg.org/cache/epub/feeds/rdf-files.tar.bz2",
        "http://gutenberg.readingroo.ms/cache/generated/feeds/rdf-files.tar.bz2",
        "http://gutenberg.pglaf.org/cache/generated/feeds/rdf-files.tar.bz2",
    ] 

    # This will be passed in to Representation.get when downloading
    # the mirror.
    def http_get_from_random_mirror(self, url, headers):
        actual_url = random.choice(MIRRORS)
        return Representations.simple_http_get(actual_url, headers)

    GUTENBERG_ORIGINAL_MIRROR = "%(gutenberg_original_mirror)s"
    GUTENBERG_EBOOK_MIRROR = "%(gutenberg_ebook_mirror)s"
    EPUB_ID = re.compile("/([0-9]+)")

    def __init__(self, _db, data_directory):
        self._db = _db
        self.source = DataSource.lookup(self._db, DataSource.GUTENBERG)
        self.data_directory = data_directory
        self.catalog_path = os.path.join(self.data_directory, self.FILENAME)
        self.log = logging.getLogger("Gutenberg API")

    def update_catalog(self):
        """Download the most recent Project Gutenberg catalog
        from a randomly selected mirror.

        We don't use Representation (for now) because the file is huge
        and the tarfile module only supports reading from a file on
        disk.
        """
        url = random.choice(self.MIRRORS)
        self.log.info("Refreshing %s", url)
        response = requests.get(url)
        if response.status_code == '403':
            self.log.error("Request blocked by Gutenberg, not updating.")
            return
        tmp_path = self.catalog_path + ".tmp"
        open(tmp_path, "wb").write(response.content)
        shutil.move(tmp_path, self.catalog_path)

    def needs_refresh(self):
        """Is it time to download a new version of the catalog?"""
        if os.path.exists(self.catalog_path):
            modification_time = os.stat(self.catalog_path).st_mtime
            return (time.time() - modification_time) >= self.ONE_DAY
        return True

    def all_books(self):
        """Yields raw data for every book in the PG catalog."""
        if self.needs_refresh():
            self.update_catalog()
        archive = tarfile.open(self.catalog_path)
        next_item = archive.next()
        a = 0
        while next_item:
            if next_item.isfile() and next_item.name.endswith(".rdf"):

                pg_id = self.ID_IN_FILENAME.search(next_item.name).groups()[0]
                yield pg_id, archive, next_item
            next_item = archive.next()

    def create_missing_books(self, subset=None):
        """Finds books present in the PG catalog but missing from Edition.

        Yields (Edition, LicensePool) 2-tuples.
        """
        books = list(self.all_books())
        for pg_id, archive, archive_item in books:
            if subset is not None and not subset(pg_id, archive, archive_item):
                continue
            self.log.info("Considering %s" % pg_id)
            # Find an existing Edition for the book.
            book = Edition.for_foreign_id(
                self._db, self.source, Identifier.GUTENBERG_ID, pg_id,
                create_if_not_exists=False)

            if not book:
                # Create a new Edition object with bibliographic
                # information from the Project Gutenberg RDF file.
                fh = archive.extractfile(archive_item)
                data = fh.read()
                fake_fh = StringIO(data)
                book, license, new = GutenbergRDFExtractor.book_in(
                    self._db, pg_id, fake_fh)

                if book and license:
                    yield (book, license)


class GutenbergRDFExtractor(object):

    """Transform a Project Gutenberg RDF description of a title into a
    Edition object and an open-access LicensePool object.
    """

    dcterms = Namespace("http://purl.org/dc/terms/")
    dcam = Namespace("http://purl.org/dc/dcam/")
    rdf = Namespace(u'http://www.w3.org/1999/02/22-rdf-syntax-ns#')
    gutenberg = Namespace("http://www.gutenberg.org/2009/pgterms/")

    ID_IN_URI = re.compile("/([0-9]+)$")

    FORMAT = "format"

    DATE_FORMAT = "%Y-%m-%d"

    @classmethod
    def _values(cls, graph, query):
        """Return just the values of subject-predicate-value triples."""
        return [x[2] for x in graph.triples(query)]

    @classmethod
    def _value(cls, graph, query):
        """Return just one value for a subject-predicate-value triple."""
        v = cls._values(graph, query)
        if v:
            return v[0]
        return None

    @classmethod
    def book_in(cls, _db, pg_id, fh):

        """Yield a Edition object for the book described by the given
        filehandle, creating it (but not committing it) if necessary.

        This assumes that there is at most one book per
        filehandle--the one identified by ``pg_id``. However, a file
        may turn out to describe no books at all (such as pg_id=1984,
        reserved for George Orwell's "1984"). In that case,
        ``book_in()`` will return None.
        """
        g = rdflib.Graph()
        g.load(fh)
        data = dict()

        # Determine the 'about' URI.
        title_triples = list(g.triples((None, cls.dcterms['title'], None)))

        new = False
        if title_triples:
            if len(title_triples) > 1:
                uris = set([x[0] for x in title_triples])
                if len(uris) > 1:
                    # Each filehandle is associated with one Project
                    # Gutenberg ID and should thus describe at most
                    # one title.
                    raise ValueError(
                        "More than one book in file for Project Gutenberg ID %s" % pg_id)
                else:
                    self.log.warn("WEIRD MULTI-TITLE: %s", pg_id)

            # TODO: Some titles such as 44244 have titles in multiple
            # languages. Not sure what to do about that.
            uri, ignore, title = title_triples[0]
            logging.info("Parsing book %s", title)
            book, license, new = cls.parse_book(_db, g, uri, title)

        else:
            book = None
            license = None
            new = False

        return book, license, new

    @classmethod
    def parse_book(cls, _db, g, uri, title):
        """Turn an RDF graph into a Edition for the given `uri` and
        `title`.
        """
        source_id = unicode(cls.ID_IN_URI.search(uri).groups()[0])
        primary_identifier = IdentifierData(
            Identifier.GUTENBERG_ID, source_id
        )

        # Split a subtitle out from the main title.
        title = unicode(title)
        subtitle = None
        for separator in "\r\n", "\n":
            if separator in title:
                parts = title.split(separator)
                title = parts[0]
                subtitle = "\n".join(parts[1:])
                break

        issued = cls._value(g, (uri, cls.dcterms.issued, None))
        issued = datetime.datetime.strptime(issued, cls.DATE_FORMAT).date()

        rights = cls._value(g, (uri, cls.dcterms.rights, None))
        if rights:
            rights = str(rights)
        else:
            rights = ''
        rights_uri = RightsStatus.rights_uri_from_string(rights)

        # As far as I can tell, Gutenberg descriptions are 100%
        # useless for our purposes. They should not be used, even if
        # no other description is available.

        publisher = cls._value(g, (uri, cls.dcterms.publisher, None))

        languages = []
        for ignore, ignore, language_uri in g.triples(
                (uri, cls.dcterms.language, None)):
            code = str(cls._value(g, (language_uri, cls.rdf.value, None)))
            code = LanguageCodes.two_to_three[code]
            if code:
                languages.append(code)

        if 'eng' in languages:
            language = 'eng'
        elif languages:
            language = languages[0]
        else:
            language = None

        contributors = []
        for ignore, ignore, author_uri in g.triples((uri, cls.dcterms.creator, None)):
            name = cls._value(g, (author_uri, cls.gutenberg.name, None))
            aliases = cls._values(g, (author_uri, cls.gutenberg.alias, None))
            contributors.append(ContributorData(
                sort_name=name,
                aliases=aliases,
                roles=[Contributor.AUTHOR_ROLE],
            ))

        subjects = []
        subject_links = cls._values(g, (uri, cls.dcterms.subject, None))
        for subject in subject_links:
            value = cls._value(g, (subject, cls.rdf.value, None))
            vocabulary = cls._value(g, (subject, cls.dcam.memberOf, None))
            vocabulary = Subject.by_uri[str(vocabulary)]
            subjects.append(SubjectData(vocabulary, value))

        medium = Edition.BOOK_MEDIUM

        # Turn the Gutenberg download links into Hyperlinks associated 
        # with the new Edition. They will serve either as open access
        # downloads or cover images.
        download_links = cls._values(g, (uri, cls.dcterms.hasFormat, None))
        links = [LinkData(
            rel=Hyperlink.CANONICAL,
            href=str(uri),
        )]

        # Gutenberg won't allow us to use any of the download or image
        # links--we have to make our own from an rsynced mirror--but
        # we can look through the links to determine which medium to
        # assign to this book.
        formats = []
        for href in download_links:
            for format_uri in cls._values(
                    g, (href, cls.dcterms['format'], None)):
                media_type = unicode(
                    cls._value(g, (format_uri, cls.rdf.value, None)))
                if media_type.startswith('audio/'):
                    medium = Edition.AUDIO_MEDIUM
                    formats.append(FormatData(
                        content_type=Representation.MP3_MEDIA_TYPE,
                        drm_scheme=DeliveryMechanism.NO_DRM,
                    ))
                elif media_type.startswith('video/'):
                    medium = Edition.VIDEO_MEDIUM
                else:
                    formats.append(FormatData(
                        content_type=Representation.EPUB_MEDIA_TYPE,
                        drm_scheme=DeliveryMechanism.NO_DRM,
                        rights_uri=rights_uri,
                    ))

        metadata = Metadata(
            data_source=DataSource.GUTENBERG,
            title=title,
            subtitle=subtitle,
            language=language,
            publisher=publisher,
            issued=issued,
            medium=medium,
            primary_identifier=primary_identifier,
            subjects=subjects,
            contributors=contributors,
            links=links,
        )
        edition, new = metadata.edition(_db)
        metadata.apply(edition)

        # Ensure that an open-access LicensePool exists for this book.
        circulation_data = CirculationData(
            data_source=DataSource.GUTENBERG,
            primary_identifier=primary_identifier,
            formats=formats,
            default_rights_uri=rights_uri,
            links=links,
        )

        license_pool, new_license_pool = circulation_data.license_pool(_db)
        replace = ReplacementPolicy(formats=True)
        circulation_data.apply(license_pool, replace=replace)
        license_pool.calculate_work()
        return edition, license_pool, new


class GutenbergMonitor(Monitor):
    """Maintain license pool and metadata info for Gutenberg titles.
    """

    def __init__(self, _db, data_directory):
        self._db = _db
        path = os.path.join(data_directory, DataSource.GUTENBERG)
        if not os.path.exists(path):
            os.makedirs(path)
        self.source = GutenbergAPI(_db, path)

    def run(self, subset=None):
        added_books = 0
        for edition, license_pool in self.source.create_missing_books(subset):
            # Log a circulation event for this title.
            event = get_one_or_create(
                self._db, CirculationEvent,
                type=CirculationEvent.TITLE_ADD,
                license_pool=license_pool,
                create_method_kwargs=dict(
                    start=license_pool.last_checked
                )
            )

            self._db.commit()

