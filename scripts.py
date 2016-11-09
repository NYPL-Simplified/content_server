import argparse
import csv
import os
import re
from collections import defaultdict
from datetime import datetime
from nose.tools import set_trace
from sqlalchemy.orm import lazyload
from lxml import etree

from core.classifier import Classifier
from core.scripts import Script
from core.lane import (
    Facets,
    Pagination,
)
from core.metadata_layer import (
    LinkData,
    FormatData,
    CirculationData,
    ReplacementPolicy,
)
from core.lane import Lane
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
from core.monitor import PresentationReadyMonitor
from core.opds import AcquisitionFeed
from core.s3 import S3Uploader
from core.external_search import ExternalSearchIndex
from core.util import fast_query_count

from coverage import GutenbergEPUBCoverageProvider
from lanes import IdentifiersLane
from marc import MARCExtractor
from monitor import GutenbergMonitor
from opds import StaticFeedAnnotator, ContentServerAnnotator


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

    # Feeds ordered by this facet will be considered the default.
    DEFAULT_ORDER = Facets.ORDER_TITLE

    DEFAULT_ENABLED_FACETS = {
        Facets.ORDER_FACET_GROUP_NAME : [
            Facets.ORDER_TITLE, Facets.ORDER_AUTHOR
        ],
        Facets.AVAILABILITY_FACET_GROUP_NAME : [
            Facets.AVAILABLE_OPEN_ACCESS
        ],
        Facets.COLLECTION_FACET_GROUP_NAME : [
            Facets.COLLECTION_FULL
        ]
    }

    # Static feeds are refreshed each time they're created, so this
    # type is primarily just to distinguish them in the database.
    CACHE_TYPE = 'static'

    # Identifies csv headers that are not considered titles for lanes.
    NONLANE_HEADERS = ['urn', 'title', 'author', 'epub']

    @classmethod
    def arg_parser(cls):
        parser = argparse.ArgumentParser()
        parser.add_argument(
            'source_csv', help='A CSV file to import URNs and Lane categories'
        )
        parser.add_argument('-t', '--title', help='The title of the feed.')
        parser.add_argument(
            '-d', '--domain', help='The domain where the feed will be placed.'
        )
        parser.add_argument(
            '-u', '--upload', action='store_true',
            help='Upload OPDS feed via S3.'
        )
        parser.add_argument(
            '--page-size', type=int, default=Pagination.DEFAULT_SIZE,
            help='The number of entries in each page feed'
        )
        parser.add_argument(
            '--search-url', help='Upload to this elasticsearch url. elasticsearch-index must also be included'
        )
        parser.add_argument(
            '--search-index', help='Upload to this elasticsearch index. elasticsearch-url must also be included'
        )
        parser.add_argument(
            '--urns', metavar='URN', nargs='*',
            help='Specific identifier urns to process, esp. for testing'
        )
        return parser

    def run(self, uploader=None, cmd_args=None):
        parsed = self.arg_parser().parse_args(cmd_args)
        source_csv = os.path.abspath(parsed.source_csv)
        feed_title = unicode(parsed.title)
        feed_id = unicode(parsed.domain)
        page_size = parsed.page_size

        if not (os.path.isfile(source_csv) or
               (feed_title and feed_id and parsed.urns)):
            # We can't build an OPDS feed or identify the required
            # Works without this information.
            raise ValueError('Please include all required arguments.')

        if (parsed.search_index and not parsed.search_url) or (parsed.search_url and not parsed.search_index):
            raise ValueError("Both --search-url and --search-index arguments must be included to upload to a search index")

        identifiers = [Identifier.parse_urn(self._db, unicode(urn))[0]
                       for urn in parsed.urns]

        lane = IdentifiersLane(self._db, identifiers, feed_title)

        if fast_query_count(lane.works()) != len(identifiers):
            identifier_ids = [i.id for i in identifiers]
            self.log_missing_identifiers(identifier_ids, lane.works())

        search_link = None
        if parsed.search_url and parsed.search_index:
            search_link = feed_id + "/search"

        # Create feeds for all enabled facets.
        feeds = self.create_feeds(lane, feed_title, feed_id, page_size, search_link)

        for base_filename, feed_pages in feeds.items():
            for index, page in enumerate(feed_pages):
                filename = base_filename
                if index != 0:
                    filename += '_%i' % (index+1)

                if parsed.upload:
                    feed_representation = Representation()
                    feed_representation.set_fetched_content(
                        page.content, content_path=filename
                    )

                    uploader = uploader or S3Uploader()
                    feed_representation.mirror_url = uploader.feed_url(filename)

                    self._db.add(feed_representation)
                    self._db.commit()
                    uploader.mirror_one(feed_representation)
                else:
                    filename = os.path.abspath(filename + '.opds')
                    with open(filename, 'w') as f:
                        f.write(page.content)
                    self.log.info("OPDS feed saved locally at %s", filename)

        if parsed.search_url and parsed.search_index:
            search_client = ExternalSearchIndex(parsed.search_url, parsed.search_index)
            annotator = ContentServerAnnotator()

            search_documents = []

            # It's slow to do these individually, but this won't run very often.
            for work in lane.works().all():
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

    def log_missing_identifiers(self, requested_ids, works_qu):
        """Logs details about requested identifiers that could not be added
        to the static feeds for whatever reason.
        """
        included_ids_qu = works_qu.statement.with_only_columns([Identifier.id])
        included_ids = self._db.execute(included_ids_qu)
        included_ids = [i[0] for i in included_ids.fetchall()]

        missing_ids = set(requested_ids).difference(included_ids)
        if not missing_ids:
            return

        detail_list = ""
        bullet = "\n    - "
        for id in missing_ids:
            identifier = self._db.query(Identifier).filter(Identifier.id==id).one()
            license_pool = identifier.licensed_through
            if not license_pool:
                detail_list += (bullet + "%r : No LicensePool found." % identifier)
                continue
            if license_pool.suppressed:
                detail_list +=  (bullet + "%r : LicensePool has been suppressed." % license_pool)
                continue
            work = license_pool.work
            if not work:
                detail_list += (bullet + "%r : No Work found." % license_pool)
                continue
            detail_list += (bullet + "%r : Unknown error." % identifier)
        self.log.warn(
            "%i identifiers could not be added to the feed. %s",
            len(missing_ids), detail_list
        )

    def make_lanes_from_csv(self, filename):
        """Parses a CSV file and creates the appropriate lane structure

        :return: a top-level Lane object, complete with sublanes
        """
        lanes = defaultdict(list)

        with open(filename) as f:
            reader = csv.DictReader(f)

            # Initialize all headers that identify a Lane.
            lane_headers = filter(
                lambda h: h.lower() not in self.NONLANE_HEADERS,
                reader.fieldnames
            )
            [lanes[header] for header in lane_headers]

            # Sort identifiers into their intended Lane
            for row in reader:
                identifier = Identifier.parse_urn(self._db, row.get('urn'))[0]
                for header in lane_headers:
                    if row.get(header):
                        lanes[header].append(identifier)

        # Create lanes and sublanes.
        top_level_lane = self.empty_lane()
        for lane_header, identifiers in lanes.items():
            if not identifiers:
                # This lane has no Works and can be ignored.
                continue

            lane_path = [name.strip() for name in lane_header.split('>')]
            base_lane = IdentifiersLane(self._db, identifiers, lane_path[-1])
            self._add_lane_to_lane_path(top_level_lane, base_lane, lane_path)

        return top_level_lane

    def _add_lane_to_lane_path(self, top_level_lane, base_lane, lane_path):
        """Adds a lane with works to the proper place in a tiered lane
        hierarchy
        """
        intermediate_lanes = lane_path[:-1]
        target = top_level_lane
        while intermediate_lanes:
            # Find or create any empty intermediate lanes.
            lane_name = intermediate_lanes.pop(0)

            existing = filter(lambda s: s.name==lane_name, target.sublanes)
            if existing:
                target = existing[0]

                # Make sure it doesn't have any Works in it, or creating
                # the feed files will get dicey.
                if isinstance(target, IdentifiersLane):
                    flawed_lane_path = '>'.join(lane_path[:-1])
                    raise ValueError(
                        "'%s' is configured with both Works AND a sublane"
                        "'%s'. This functionality is not yet supported." %
                        (flawed_lane_path, base_lane.name)
                    )
            else:
                # Create a new lane and set it as the target.
                new_sublane = self.empty_lane(name=lane_name, parent=target)
                target.sublanes.add(new_sublane)
                target = new_sublane

        # We've reached the end of the Lane path. If we like it then
        # we better put some Works in it.
        base_lane.parent = target
        target.sublanes.add(base_lane)

    def empty_lane(self, name=None, parent=None):
        """Creates a Lane without Works, either for the top level or
        somewhere along a Lane tree / path.
        """
        if not parent:
            # Create a top level lane.
            return Lane(
                self._db, 'All Books',
                display_name='All Books',
                include_all=False,
                searchable=True,
                invisible=True
            )
        else:
            # Create a visible intermediate lane.
            return Lane(
                self._db, name,
                display_name=name,
                parent=parent,
                include_all=False,
            )

    def create_feeds(self, lane, feed_title, feed_id, page_size, search_link=None):
        """Creates feeds for facets that may be required

        :return: A dictionary of filenames pointing to a list of CachedFeed
        objects representing pages
        """
        annotator = StaticFeedAnnotator(
            feed_id, feed_title, default_order=self.DEFAULT_ORDER, search_link=search_link
        )
        static_facets = Facets(
            Facets.COLLECTION_FULL, Facets.AVAILABLE_OPEN_ACCESS,
            Facets.ORDER_TITLE, enabled_facets=self.DEFAULT_ENABLED_FACETS
        )

        feeds = dict()
        for facet_group in list(static_facets.facet_groups):
            ordered_by, facet_obj = facet_group[1:3]

            pagination = Pagination(size=page_size)
            feed_pages = list(self.create_feed_pages(
                lane, pagination, feed_title, feed_id, annotator, facet_obj
            ))

            key = annotator.base_filename
            if ordered_by != self.DEFAULT_ORDER:
                key += ('_' + ordered_by)
            feeds[key] = feed_pages

        return feeds

    def create_feed_pages(self, lane, pagination, feed_title, feed_id,
                          annotator, facet):
        """Yields each page of the feed for a particular lane."""

        previous_page = pagination.previous_page
        while not previous_page or previous_page.has_next_page:
            page = AcquisitionFeed.page(
                self._db, feed_title, feed_id, lane,
                annotator=annotator,
                facets=facet,
                pagination=pagination,
                cache_type=self.CACHE_TYPE,
                force_refresh=True,
                use_materialized_works=False
            )
            yield page

            # Reset values to determine if next page should be created.
            previous_page = pagination
            pagination = pagination.next_page
