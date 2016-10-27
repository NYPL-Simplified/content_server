import argparse
import csv
import os
import re
from datetime import datetime
from sqlalchemy.orm import lazyload

from core.scripts import (
    IdentifierInputScript,
    Script,
)
from monitor import GutenbergMonitor
from coverage import (
    GutenbergEPUBCoverageProvider,
)
from core.model import (
    CustomList,
    CustomListEntry,
    DataSource,
    DeliveryMechanism,
    get_one,
    get_one_or_create,
    Hyperlink,
    Identifier,
    LicensePool,
    Representation,
    Resource,
    RightsStatus,
    Work,
)
from core.classifier import Classifier
from core.monitor import PresentationReadyMonitor

from core.metadata_layer import (
    LinkData,
    FormatData,
    CirculationData,
    ReplacementPolicy,
)
from core.opds import AcquisitionFeed
from core.s3 import S3Uploader

from marc import MARCExtractor
from opds import ContentServerAnnotator
from nose.tools import set_trace


class GutenbergMonitorScript(Script):

    """Update the content server with new items from Project Gutenberg."""

    # Some useful methods to pass as the argument of `subset`.
    @classmethod
    def very_small_subset(self, pg_id, archive, archive_item):
        """A minimal set of test data that focuses on the many Gutenberg
        editions of three works: "Moby-Dick", "Alice in Wonderland", and
        "The Adventures of Huckleberry Finn".
        """
        return int(pg_id) in [
            11, 928, 28885, 23716, 19033, # Alice in Wonderland
            12, 23718,                    # Through The Looking Glass

            76, 19640, 9007,              # The Adventures of Huckleberry Finn
            32325,                        # "The Adventures of Huckleberry Finn,
            #  Tom Sawyer's Comrade"

            # This is the best example for two books that have different titles
            # but are the same work.
            15, 9147,                     # Moby Dick
            2701, 2489, 28794,            # "Moby Dick, or, the Whale"

            # This is the best example for two books that have similar titles
            # but are different works.
            91, 9036,                     # Tom Sawyer Abroad
            93, 9037,                     # Tom Sawyer, Detective

            # These aren't really that useful except for verifying that
            # books semi-similar enough to other books don't get
            # consolidated.
            19778,                        # AiW in German
            28371,                        # AiW in French
            17482,                        # AiW in Esperanto
            114,                          # Tenniel illustrations only
            19002,                        # Alice's Adventures Under Ground
            10643,                        # World's Greatest Books, includes AiW
            36308,                        # AiW songs only
            19551,                        # AiW in words of one syllable
            35688,                        # "Alice in Wonderland" but by a
            #  different author.
            35990,                        # "The Story of Lewis Carroll"
            
            7100, 7101, 7102, 7103,       # Huckleberry Finn in 5-chapter
            7104, 7105, 7106, 7107,       #  chunks
            
            74, 26203, 9038,              # The Adventures of Tom Sawyer
            30165,                        # Tom Sawyer in German
            30890,                        # Tom Sawyer in French
            45333,                        # Tom Sawyer in Finnish
            7193, 7194, 7198, 7196,       # Tom Sawyer in chunks
            7197, 7198, 7199, 7200,
        ]


    @classmethod
    def secret_garden_subset(self, pg_id, archive, archive_item):
        return int(pg_id) in [113, 8812, 17396, 21585,   # The Secret Garden
                              146, 19514, 23711, 37332,  # A Little Princess
                              479, 23710,                # Little Lord Fauntleroy
                              
                              # # Some pretty obscure books.
                              # 2300,
                              # 2400,
                              # 2500,
                              # 2600,
                          ]

    @classmethod
    def first_half_subset(cls, pg_id, archive, archive_item):
        """A large data set containing all the well-known public domain works,
        but not the entirety of Project Gutenberg."""
        return int(pg_id) < 20000

    @classmethod
    def middle_of_the_road_subset(cls, pg_id, archive, archive_item):
        """A relatively small data set containing relatively well-known
        works."""
        return int(pg_id) > 1000 and int(pg_id) < 1200

    subset = None

    def run(self):
        GutenbergMonitor(
            self._db, self.data_directory).run(self.subset)


class MakePresentationReadyScript(Script):

    def run(self):
        epub = GutenbergEPUBCoverageProvider(self._db)

        providers = [epub]
        PresentationReadyMonitor(
            self._db, providers, calculate_work_even_if_no_author=True).run()

class DirectoryImportScript(Script):

    def run(self, directory, data_source_name):
        # Look for a MARC metadata file in the directory
        metadata_file = None
        for root, dirs, files in os.walk(directory):
            for file in files:
                if file.endswith(".mrc") or file.endswith(".marc"):
                    metadata_file = os.path.join(root, file)
                    break
            if metadata_file:
                break

        if not metadata_file:
            raise Exception("No metadata file found")

        # Extract metadata for each book
        replacement_policy = ReplacementPolicy(rights=True, links=True, formats=True)
        with open(metadata_file) as f:
            metadata_records = MARCExtractor().parse(f, data_source_name)
            for metadata in metadata_records:
                primary_identifier = metadata.primary_identifier
                isbn = primary_identifier.identifier

                uploader = S3Uploader()
                paths = dict()

                url = uploader.book_url(primary_identifier, "epub")
                epub_path = os.path.join(directory, isbn + ".epub")
                paths[url] = epub_path

                epub_link = LinkData(
                    rel=Hyperlink.OPEN_ACCESS_DOWNLOAD,
                    href=url,
                    media_type=Representation.EPUB_MEDIA_TYPE,
                )

                formats = [FormatData(
                    content_type=Representation.EPUB_MEDIA_TYPE,
                    drm_scheme=DeliveryMechanism.NO_DRM,
                    link=epub_link,
                )]
                circulation_data = CirculationData(
                    data_source_name,
                    primary_identifier,
                    links=[epub_link],
                    formats=formats,
                )
                metadata.circulation = circulation_data

                cover_file = isbn + ".jpg"
                cover_url = uploader.cover_image_url(
                    data_source_name, primary_identifier, cover_file)
                cover_path = os.path.join(directory, cover_file)
                paths[cover_url] = cover_path

                metadata.links.append(LinkData(
                    rel=Hyperlink.IMAGE,
                    href=cover_url,
                    media_type=Representation.JPEG_MEDIA_TYPE,
                ))

                edition, new = metadata.edition(self._db)
                metadata.apply(edition, replace=replacement_policy)
                pool = get_one(self._db, LicensePool, identifier=edition.primary_identifier)

                if new:
                    print "created new edition %s" % edition.title
                for link in edition.primary_identifier.links:
                    if link.rel == Hyperlink.IMAGE:
                        representation = link.resource.representation
                        representation.mirror_url = link.resource.url
                        representation.local_content_path = paths[link.resource.url]
                        uploader.mirror_one(representation)   
                        height = 300
                        width = 200
                        cover_file = isbn + ".png"
                        thumbnail_url = uploader.cover_image_url(
                            data_source_name, primary_identifier, cover_file,
                            height
                        )
                        thumbnail, ignore = representation.scale(
                            height, width, thumbnail_url, 
                            Representation.PNG_MEDIA_TYPE
                        )
                        uploader.mirror_one(thumbnail)

                work, ignore = pool.calculate_work()
                work.set_presentation_ready()
                self._db.commit()


class CSVExportScript(Script):

    """Exports the primary identifier, title, author, and download URL
    for all of the works from a particular DataSource or DataSources
    to a CSV file format.
    """

    @classmethod
    def arg_parser(cls):
        parser = argparse.ArgumentParser()
        parser.add_argument(
            'data-source',
            help='A specific source whose Works should be exported.'
        )
        parser.add_argument(
            '-f', '--filename', help='Name for the output CSV file.'
        )
        parser.add_argument(
            '-a', '--append', action='store_true',
            help='Append new results to existing file.'
        )
        return parser

    def do_run(self):
        parser = self.arg_parser().parse_args()

        # Verify the requested DataSource exists.
        source_name = parser.data_source
        source = DataSource.lookup(self._db, source_name)
        if not source:
            raise ValueError('DataSource "%s" could not be found.', source_name)

        # Get all Works from the Source.
        works = self._db.query(Work, Identifier, Resource.url)
        works = works.options(lazyload(Work.license_pools))
        works = works.join(Work.license_pools).join(LicensePool.data_source).\
                join(LicensePool.links).join(LicensePool.identifier).\
                join(Hyperlink.resource).filter(
                    DataSource.name==source_name,
                    Hyperlink.rel==Hyperlink.OPEN_ACCESS_DOWNLOAD,
                    Resource.url.like(u'%.epub')
                ).all()

        self.log.info(
            'Exporting data for %d Works from %s', len(works), source_name
        )

        # Transform the works into very basic CSV data for review.
        rows = list()
        for work, identifier, url in works:
            row_data = dict(
                identifier=identifier.urn.encode('utf-8'),
                title=work.title.encode('utf-8'),
                author=work.author.encode('utf-8'),
                download_url=url.encode('utf-8')
            )
            rows.append(row_data)

        # List the works in alphabetical order by title.
        compare = lambda a,b: cmp(a['title'].lower(), b['title'].lower())
        rows.sort(cmp=compare)

        # Find or create a CSV file in the main app directory.
        filename = parser.filename
        if not filename.lower().endswith('.csv'):
            filename += '.csv'
        filename = os.path.abspath(filename)

        # Determine whether to append new rows or write over an existing file.
        open_method = 'w'
        if parser.append:
            open_method = 'a'

        with open(filename, open_method) as f:
            fieldnames=['identifier', 'title', 'author', 'download_url']
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            if not parser.append:
                writer.writeheader()
            for row in rows:
                writer.writerow(row)


class CustomOPDSFeedGenerationScript(IdentifierInputScript):

    @classmethod
    def arg_parser(cls):
        parser = IdentifierInputScript.arg_parser()
        parser.add_argument('-t', '--title', help='The title of the feed.')
        parser.add_argument(
            '-d', '--domain', help='The domain where the feed will be placed.'
        )
        parser.add_argument(
            '--create-list', action='store_true',
            help='Create a CustomList to save feed entries.'
        )
        parser.add_argument(
            '--append-list', action='store_true',
            help='Save new entries to an existing CustomList.'
        )
        return parser

    @classmethod
    def slugify_feed_title(cls, feed_title):
        slug = re.sub('[.!@#$,]', '', feed_title.lower())
        slug = re.sub('&', ' and ', slug)
        slug = re.sub(' {2,}', ' ', slug)
        return unicode('-'.join(slug.split(' ')))

    def do_run(self):
        parser = self.arg_parser().parse_args()
        feed_title = unicode(parser.title)
        identifier_type = parser.identifier_type
        identifiers = parser.identifier_strings

        if not (feed_title and parser.domain and identifier_type):
            # We can't build an OPDS feed or identify the required
            # Works without this information.
            raise ValueError('Please include all required arguments.')

        # TODO: Works are being selected against LicensePool,
        # ignoring the possibility that a the LicensePool may have been
        # suppressed or superceded.
        works = self._db.query(Work).select_from(LicensePool).\
            join(LicensePool.work).join(LicensePool.identifier).filter(
                Identifier.type==identifier_type,
                Identifier.identifier.in_(identifiers)
            ).options(lazyload(Work.license_pools)).all()
        feed = AcquisitionFeed(
            self._db, feed_title, parser.domain, works,
            annotator=ContentServerAnnotator()
        )

        self.create_custom_list(parser, feed_title, works)

        timestamp = datetime.now().strftime('%Y%m%d%H%M%S-')
        filename = self.slugify_feed_title(feed_title)
        filename = os.path.abspath(timestamp + filename + '.opds')
        with open(filename, 'w') as f:
            f.write(unicode(feed))

    def create_custom_list(self, parser, feed_title, works):
        list = None

        if not (parser.create_list or parser.append_list):
            return

        list_source = DataSource.lookup(self._db, DataSource.LIBRARY_STAFF)
        foreign_identifier = self.slugify_feed_title(feed_title)
        custom_list, is_new_list = get_one_or_create(
            self._db, CustomList, data_source=list_source,
            foreign_identifier=foreign_identifier
        )

        if is_new_list:
            custom_list.name = feed_title

        if parser.create_list and not is_new_list:
            raise ValueError(
                'A custom list with the title %s already exists. Please '\
                'select a different title for this list or use the \'-a\''\
                ' option to append new entries.' % feed_title
            )

        for work in works:
            edition = work.presentation_edition
            custom_list.add_entry(edition)
        self._db.commit()
