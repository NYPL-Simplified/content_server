import argparse
import csv
import os
import re
from datetime import datetime
from sqlalchemy.orm import lazyload
from lxml import etree

from core.scripts import Script
from monitor import GutenbergMonitor
from coverage import (
    GutenbergEPUBCoverageProvider,
)
from core.model import (
    DataSource,
    DeliveryMechanism,
    Edition,
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
from core.util.opds_writer import OPDSFeed
from core.s3 import S3Uploader
from core.external_search import ExternalSearchIndex

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

    def run(self, data_source_name, metadata_records, epub_directory, cover_directory):
        replacement_policy = ReplacementPolicy(rights=True, links=True, formats=True)
        for metadata in metadata_records:
            primary_identifier = metadata.primary_identifier

            uploader = S3Uploader()
            paths = dict()

            url = uploader.book_url(primary_identifier, "epub")
            epub_path = os.path.join(epub_directory, primary_identifier.identifier + ".epub")
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

            cover_file = primary_identifier.identifier + ".jpg"
            cover_url = uploader.cover_image_url(
                data_source_name, primary_identifier, cover_file)
            cover_path = os.path.join(cover_directory, cover_file)
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
                if link.rel == Hyperlink.IMAGE or link.rel == Hyperlink.OPEN_ACCESS_DOWNLOAD:
                    representation = link.resource.representation
                    representation.mirror_url = link.resource.url
                    representation.local_content_path = paths[link.resource.url]
                    try:
                        uploader.mirror_one(representation)
                        if link.rel == Hyperlink.IMAGE:
                            height = 300
                            width = 200
                            cover_file = primary_identifier.identifier + ".png"
                            thumbnail_url = uploader.cover_image_url(
                                data_source_name, primary_identifier, cover_file,
                                height
                            )
                            thumbnail, ignore = representation.scale(
                                height, width, thumbnail_url, 
                                Representation.PNG_MEDIA_TYPE
                            )
                            uploader.mirror_one(thumbnail)
                    except ValueError, e:
                        print "Failed to mirror file %s" % representation.local_content_path

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


class CustomOPDSFeedGenerationScript(Script):

    @classmethod
    def arg_parser(cls):
        parser = argparse.ArgumentParser()
        parser.add_argument('-t', '--title', help='The title of the feed.')
        parser.add_argument(
            '-d', '--domain', help='The domain where the feed will be placed.'
        )
        parser.add_argument(
            '-u', '--upload', action='store_true',
            help='Upload OPDS feed via S3.'
        )
        parser.add_argument(
            '--search-url', help='Upload to this elasticsearch url. elasticsearch-index must also be included'
        )
        parser.add_argument(
            '--search-index', help='Upload to this elasticsearch index. elasticsearch-url must also be included'
        )
        parser.add_argument(
            'urns', help='A specific identifier urn to process.',
            metavar='URN', nargs='*'
        )
        return parser

    @classmethod
    def slugify_feed_title(cls, feed_title):
        slug = re.sub('[.!@#\'$,]', '', feed_title.lower())
        slug = re.sub('&', ' and ', slug)
        slug = re.sub(' {2,}', ' ', slug)
        return unicode('-'.join(slug.split(' ')))

    def run(self, uploader=None, cmd_args=None):
        parsed = self.arg_parser().parse_args(cmd_args)
        feed_title = unicode(parsed.title)
        feed_id = unicode(parsed.domain)

        if not (feed_title and feed_id and parsed.urns):
            # We can't build an OPDS feed or identify the required
            # Works without this information.
            raise ValueError('Please include all required arguments.')

        if (parsed.search_index and not parsed.search_url) or (parsed.search_url and not parsed.search_index):
            raise ValueError("Both --search-url and --search-index arguments must be included to upload to a search index")

        works = list()
        for urn in parsed.urns:
            identifier = Identifier.parse_urn(self._db, unicode(urn))[0]
            license_pool = identifier.licensed_through
            if not license_pool:
                self.log.warn("No LicensePool found for %r", identifier)
                continue
            if license_pool.suppressed:
                self.log.warn(
                    "LicensePool %r has been suppressed and won't be added to "\
                    "the feed.", license_pool
                )
                continue
            work = license_pool.work
            if not work:
                self.log.warn("No Work found for %r", license_pool)
                continue
            works.append(work)

        feed = AcquisitionFeed(
            self._db, feed_title, feed_id, works,
            annotator=ContentServerAnnotator()
        )

        if parsed.search_url and parsed.search_index:
            OPDSFeed.add_link_to_feed(feed=feed.feed,
                                      rel="search",
                                      href=feed_id + "/search",
                                      type="application/opensearchdescription+xml")

        filename = self.slugify_feed_title(feed_title)
        if parsed.upload:
            feed_representation = Representation()
            feed_representation.set_fetched_content(
                unicode(feed), content_path=filename
            )

            uploader = uploader or S3Uploader()
            feed_representation.mirror_url = uploader.feed_url(filename)

            self._db.add(feed_representation)
            self._db.commit()
            uploader.mirror_one(feed_representation)
        else:
            filename = os.path.abspath(filename + '.opds')
            with open(filename, 'w') as f:
                f.write(unicode(feed))
            self.log.info("OPDS feed saved locally at %s", filename)

        if parsed.search_url and parsed.search_index:
            search_client = ExternalSearchIndex(parsed.search_url, parsed.search_index)
            annotator = ContentServerAnnotator()

            search_documents = []

            # It's slow to do these individually, but this won't run very often.
            for work in works:
                doc = work.to_search_document()
                doc["_index"] = search_client.works_index
                doc["_type"] = search_client.work_document_type
                doc["opds_entry"] = etree.tostring(AcquisitionFeed.single_entry(self._db, work, annotator))
                search_documents.append(doc)

            success_count, errors = search_client.bulk(
                search_documents,
                raise_on_error=False,
                raise_on_exception=False,
            )

            if (len(errors) > 0):
                self.log.error("%i errors uploading to search index" % len(errors))
            self.log.info("%i documents uploaded to search index %s on %s" % (success_count, parsed.search_index, parsed.search_url))
