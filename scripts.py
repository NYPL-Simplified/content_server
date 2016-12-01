import argparse
import csv
import os
import re
import yaml
from collections import defaultdict
from datetime import datetime
from nose.tools import set_trace
from lxml import etree

from sqlalchemy import (
    not_,
    or_,
)

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
from core.model import (
    Classification,
    DataSource,
    DeliveryMechanism,
    Edition,
    Genre,
    get_one,
    get_one_or_create,
    Hyperlink,
    Identifier,
    LicensePool,
    Representation,
    Resource,
    RightsStatus,
    Subject,
    Work,
    WorkGenre,
)
from core.monitor import PresentationReadyMonitor
from core.opds import AcquisitionFeed
from core.s3 import S3Uploader
from core.external_search import ExternalSearchIndex
from core.util import (
    fast_query_count,
    LanguageCodes,
)

from coverage import GutenbergEPUBCoverageProvider
from lanes import (
    StaticFeedBaseLane,
    StaticFeedParentLane,
)
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


class StaticFeedScript(Script):

    # Identifies csv headers that are not considered titles for lanes.
    NONLANE_HEADERS = ['urn', 'title', 'author', 'epub', 'featured']

    @classmethod
    def header_to_path(cls, header):
        return [name.strip() for name in header.split('>')]

    @classmethod
    def category_paths(cls, csv_reader):
        return filter(
            lambda h: h.lower() not in cls.NONLANE_HEADERS,
            csv_reader.fieldnames
        )


class StaticFeedCSVExportScript(StaticFeedScript):

    """Exports the primary identifier, title, author, and download URL
    for all of the works from a particular DataSource or DataSources
    to a CSV file format.

    If requested categories are passed in via an existing CSV, this
    script will add headers to represent those categories, in a proper
    format for StaticFeedGenerationScript.
    """

    class CategoryNode(object):

        @classmethod
        def head(cls):
            return cls('Main')

        def __init__(self, name, parent=None, children=None):
            self.name = unicode(name)
            self.set_parent(parent)
            self._children = children or []

        @property
        def parent(self):
            return self._parent

        @property
        def children(self):
            return self._children

        @property
        def path(self):
            if self.name == 'Main':
                return None

            nodes = list()
            current_node = self
            while current_node.parent:
                nodes.insert(0, current_node)
                current_node = current_node.parent
            return nodes

        @property
        def siblings(self):
            if not self.parent:
                return []
            return filter(lambda c: c.name!=self.name, self.parent.children)

        @property
        def default_sibling(self):
            if self.default:
                return self
            default_sibling = [s for s in self.siblings if s.default]
            if default_sibling:
                return default_sibling[0]

        def __str__(self):
            if self.name == 'Main':
                return ''

            nodes = list()
            current_node = self
            while current_node.parent:
                nodes.insert(0, current_node.name)
                current_node = current_node.parent
            return '>'.join(nodes)

        def set_parent(self, parent):
            self._parent = parent
            if parent:
                parent.add_child(self)
            self.set_default()

        def set_default(self):
            self.default = False
            if (not self.default_sibling and
                ((self.name=='Fiction' and not self.parent.name=='Main')
                or self.name.startswith('General'))):
                self.default = True

        def add_child(self, child):
            existing_children = [c.name for c in self.children]
            if not child.name in existing_children:
                self._children.append(child)

        def add_path(self, branch_list):
            current = self
            while branch_list:
                new_node_name = branch_list.pop(0)

                existing = filter(lambda c: c.name==new_node_name, current.children)
                if existing:
                    current = existing[0]
                else:
                    new = type(self)(new_node_name, parent=current)
                    current = new

    FICTIONALITY = ['fiction', 'nonfiction']

    LANGUAGES = LanguageCodes.english_names_to_three.keys()

    SUBJECTS = ['Short Stories']

    @classmethod
    def arg_parser(cls):
        parser = argparse.ArgumentParser()
        parser.add_argument(
            '-s', '--source-file',
            help='Existing CSV file or YAML list with categories'
        )
        parser.add_argument(
            '-o', '--output-file', default='output.csv',
            help='New or existing filename for the output CSV file.'
        )
        parser.add_argument(
            '-d', '--datasources', nargs='+',
            help='A specific source whose Works should be exported.'
        )
        parser.add_argument(
            '-a', '--append', action='store_true',
            help='Append new results to existing file.'
        )
        return parser

    @property
    def base_works_query(self):
        """The base query for works, used externally for testing."""
        return self._db.query(Work, Identifier, Resource.url).\
            enable_eagerloads(False).join(Work.license_pools).\
            join(LicensePool.data_source).join(LicensePool.links).\
            join(LicensePool.identifier).join(Hyperlink.resource)

    def do_run(self):
        parser = self.arg_parser().parse_args()

        # Verify the requested DataSources exists.
        source_names = parser.datasources
        for name in source_names:
            source = DataSource.lookup(self._db, name)
            if not source:
                raise ValueError('DataSource "%s" could not be found.', name)

        # Get all Works from the DataSources.
        works_qu = self.base_works_query.filter(
            DataSource.name.in_(source_names),
            Hyperlink.rel==Hyperlink.OPEN_ACCESS_DOWNLOAD,
            Resource.url.like(u'%.epub')
        )

        self.log.info(
            'Exporting data for %d Works from DataSources: %s',
            fast_query_count(works_qu), ','.join(source_names)
        )

        # Transform the works into CSV data for review.
        rows = list(self.create_row_data(works_qu, parser.source_file))

        # Find or create a CSV file in the main app directory.
        filename = parser.output_file
        if not filename.lower().endswith('.csv'):
            filename += '.csv'
        filename = os.path.abspath(filename)

        if parser.append and os.path.isfile(filename):
            existing_rows = None
            with open(filename) as f:
                existing_rows = [r for r in csv.DictReader(f)]

            if existing_rows:
                # Works that are already in the file may have been
                # updated by a human since the last run. Prefer existing
                # categorizations over newer server-generated ones.
                existing_urns = set([r['urn'] for r in existing_rows])
                new_urns = set([r['urn'] for r in rows])
                overwritten_urns = existing_urns & new_urns
                rows = filter(
                    lambda r: r['urn'] not in overwritten_urns,
                    rows
                )
                rows.extend(existing_rows)

        # List the works in alphabetical order by title.
        compare = lambda a,b: cmp(a['title'].lower(), b['title'].lower())
        rows.sort(cmp=compare)

        with open(filename, 'w') as f:
            fieldnames = self.NONLANE_HEADERS
            category_fieldnames = self.get_category_fieldnames(rows, fieldnames)
            fieldnames.extend(category_fieldnames)
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)

    def create_row_data(self, base_query, source_file):
        category_nodes = self.get_base_categories(source_file)
        if not category_nodes:
            # There's no desire for categorized works.
            for item in base_query:
                yield self.basic_work_row_data(*item)
            return

        works_by_category_node = self.apply_nodes(category_nodes, base_query)
        for node, works in works_by_category_node.items():
            for work, identifier, url in works:
                row_data = self.basic_work_row_data(work, identifier, url)
                row_data['featured'] = ''.encode('utf-8')
                row_data[str(node)] = 'x'.encode('utf-8')
                yield row_data

    def basic_work_row_data(self, work, identifier, url):
        return dict(
            urn=identifier.urn.encode('utf-8'),
            title=work.title.encode('utf-8'),
            author=work.author.encode('utf-8'),
            epub=url.encode('utf-8')
        )

    def apply_nodes(self, category_nodes, qu):
        """Applies category criteria to the base query for each CategoryNode"""

        def sort_key(node):
            # Lanes should be applied to works according to the
            # specificity of their parent classes.
            parents = self.header_to_path(str(node))[:-1]
            parents = [n.lower() for n in parents]
            genre_parents = filter(
                lambda n: n not in self.LANGUAGES and n not in self.FICTIONALITY,
                parents
            )

            specificity = -(len(genre_parents))
            if node.default:
                # Default nodes are sorted to the end of the list, while
                # otherwise maintaining an order of specificity.
                return specificity + 100
            return specificity

        category_nodes = sorted(category_nodes, key=sort_key)
        works_by_category_node = dict()
        for category_node in category_nodes:
            # Apply category criteria to the base query, going from
            # parent to base node, assuming increasing specificity.
            path = category_node.path
            if category_node.default and category_node.name.lower() not in self.FICTIONALITY:
                # Default nodes are catchall lanes like "Fiction" or
                # "General Fiction". Because they don't necessarily represent
                # specific genres, we shouldn't apply them willy nilly.
                path = path[:-1]
            node_qu = qu

            if path[0].name.lower() not in self.LANGUAGES:
                # The default language is English.
                node_qu = self.apply_language(node_qu, 'English')

            for node in path:
                node_qu = self.apply_node(node, node_qu)

            # Remove any works included in previously-run (and thus more
            # specific) categories.
            placed_works = list()
            if category_node.default:
                placed_works = works_by_category_node.values()
            else:
                placed_works = [results for n, results in works_by_category_node.items()
                                if n.name == category_node.name]

            if placed_works:
                placed_works_ids = set()
                for results in placed_works:
                    for work, _, _ in results:
                        placed_works_ids.add(work.id)
                node_qu = node_qu.filter(not_(Work.id.in_(placed_works_ids)))

            works_results = node_qu.distinct(Work.id).all()
            self.log.info(
                "%i results found for lane %s",
                len(works_results), category_node
            )
            works_by_category_node[category_node] = works_results

        return works_by_category_node

    def apply_node(self, node, qu):
        if node.name == 'Main':
            # This is the head of the CategoryNode tree. Ignore it.
            return qu
        if node.name.lower() in self.LANGUAGES:
            return self.apply_language(qu, node.name)
        if node.name.lower() in self.FICTIONALITY:
            return self.apply_fiction_status(qu, node.name)
        if node.name in self.SUBJECTS:
            return qu.outerjoin(Identifier.classifications).\
                outerjoin(Classification.subject).join(Work.work_genres).\
                join(WorkGenre.genre).filter(
                    or_(Genre.name == node.name, Subject.name == node.name)
                )
        if not node.children:
            # This is a base-level genre and should be applied.
            return qu.join(Work.work_genres, aliased=True, from_joinpoint=True).\
                join(WorkGenre.genre, aliased=True, from_joinpoint=True).\
                filter(Genre.name==node.name)

        # This is the head of the CategoryNode tree or an intermediate
        # genre lane that doesn't give helpful WorkGenre filtering info,
        # like Horror in Fiction > Horror > Paranormal. Ignore it.
        return qu

    def apply_language(self, qu, language):
        code = LanguageCodes.english_names_to_three[language.lower()]
        qu = qu.join(Work.presentation_edition).filter(Edition.language==code)
        return qu

    def apply_fiction_status(self, qu, fiction_status):
        if 'non' in fiction_status.lower():
            return qu.filter(Work.fiction==False)
        else:
            return qu.filter(Work.fiction==True)

    def get_base_categories(self, source_file):
        if not source_file:
            return None

        category_file = os.path.abspath(source_file)
        if not os.path.isfile(category_file):
            raise ValueError("Category file %s not found." % category_file)

        category_tree = self.CategoryNode.head()

        with open(category_file) as f:
            if category_file.endswith('.yml'):
                categories = yaml.load(f)
                category_paths = list()

                def unpack(category, ancestors=None):
                    path = ancestors or list()
                    if isinstance(category, dict):
                        [(parent, children)] = category.items()
                        path.append(parent)
                        for child in children:
                            unpack(child, ancestors=path)
                    else:
                        full_path = path[:]
                        full_path.append(category)
                        category_paths.append(full_path)

                for category in categories:
                    unpack(category)

            if category_file.endswith('.csv'):
                reader = csv.DictReader(f)
                category_paths = self.category_paths(reader)
                category_paths = [self.header_to_path(p) for p in category_paths]

        for path in category_paths:
            category_tree.add_path(path)
        if not category_paths:
            # The source file didn't have any categories.
            return None

        def find_base_nodes(category_node):
            if not category_node.children:
                yield category_node
            else:
                for child in category_node.children:
                    for n in find_base_nodes(child):
                        yield n
        return list(find_base_nodes(category_tree))

    def get_category_fieldnames(self, rows, existing):
        """Returns any fieldnames in the rows that are not included in
        basic work data fieldnames.
        """
        fieldnames = list()
        [fieldnames.extend(r.keys()) for r in rows]
        new = set(fieldnames).difference(existing)
        return sorted(new)


class StaticFeedGenerationScript(StaticFeedScript):

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
    CACHE_TYPE = u'static'

    @classmethod
    def arg_parser(cls):
        parser = argparse.ArgumentParser()
        parser.add_argument(
            'source_csv', help='A CSV file to import URNs and Lane categories'
        )
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
        feed_id = unicode(parsed.domain)
        page_size = parsed.page_size

        if not (feed_id and (os.path.isfile(source_csv) or parsed.urns)):
            # We can't build an OPDS feed or identify the required
            # Works without this information.
            raise ValueError('Please include all required arguments.')

        if (parsed.search_index and not parsed.search_url) or (parsed.search_url and not parsed.search_index):
            raise ValueError("Both --search-url and --search-index arguments must be included to upload to a search index")

        if parsed.urns:
            identifiers = [Identifier.parse_urn(self._db, unicode(urn))[0]
                           for urn in parsed.urns]
            lane = StaticFeedBaseLane(
                self._db, identifiers, StaticFeedAnnotator.TOP_LEVEL_LANE_NAME
            )
            full_query = lane.works()

            self.log_missing_identifiers(lane.identifiers, full_query)
        else:
            lane, full_query = self.make_lanes_from_csv(source_csv)

        search_link = None
        if parsed.search_url and parsed.search_index:
            search_link = feed_id + "/search"

        feeds = list(self.create_feeds([lane], feed_id, page_size, search_link))

        for base_filename, feed_pages in feeds:
            for index, page in enumerate(feed_pages):
                filename = base_filename
                if index != 0:
                    filename += '_%i' % (index+1)

                if parsed.upload:
                    self.upload(filename, page.content, uploader=uploader)
                else:
                    filename = os.path.abspath(filename + '.xml')
                    with open(filename, 'w') as f:
                        f.write(page.content)
                    self.log.info("OPDS feed saved locally at %s", filename)

        if parsed.search_url and parsed.search_index:
            search_client = ExternalSearchIndex(parsed.search_url, parsed.search_index)
            annotator = ContentServerAnnotator()

            search_documents = []

            # It's slow to do these individually, but this won't run very often.
            for work in full_query.all():
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

    def log_missing_identifiers(self, requested_identifiers, works_qu):
        """Logs details about requested identifiers that could not be added
        to the static feeds for whatever reason.
        """
        included_ids_qu = works_qu.with_labels().statement.\
            with_only_columns([Identifier.id])
        included_ids = self._db.execute(included_ids_qu)
        included_ids = [i[0] for i in included_ids.fetchall()]

        requested_ids = [i.id for i in requested_identifiers]
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

        :return: a top-level StaticFeedParentLane, complete with sublanes
        """
        lanes = defaultdict(list)

        with open(filename) as f:
            reader = csv.DictReader(f)

            # Initialize all headers that identify a categorized lane.
            lane_headers = self.category_paths(reader)
            [lanes[unicode(header)] for header in lane_headers]

            # Sort identifiers into their intended lane.
            urns_to_identifiers = dict()
            all_featured = list()
            for row in reader:
                urn = row.get('urn')
                identifier = Identifier.parse_urn(self._db, urn)[0]
                urns_to_identifiers[urn] = identifier
                if row.get('featured'):
                    all_featured.append(identifier)
                for header in lane_headers:
                    if row.get(header):
                        lanes[header].append(identifier)

            if not lanes:
                # There aren't categorical lanes in this csv, so
                # create and return a single StaticFeedBaseLane.
                identifiers = urns_to_identifiers.values()
                single_lane = StaticFeedBaseLane(
                    self._db, identifiers, StaticFeedAnnotator.TOP_LEVEL_LANE_NAME
                )
                return single_lane, single_lane.works()

        # Create lanes and sublanes.
        top_level_lane = self.empty_lane()
        lanes_with_works = list()
        for lane_header, identifiers in lanes.items():
            if not identifiers:
                # This lane has no Works and can be ignored.
                continue
            lane_path = self.header_to_path(lane_header)
            featured = filter(lambda i: i in all_featured, identifiers)
            base_lane = StaticFeedBaseLane(
                self._db, identifiers, lane_path[-1], featured=featured
            )
            lanes_with_works.append(base_lane)

            self._add_lane_to_lane_path(top_level_lane, base_lane, lane_path)

        full_query = top_level_lane.works()
        self.log_missing_identifiers(identifiers, full_query)

        return top_level_lane, full_query

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
                if isinstance(target, StaticFeedBaseLane):
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

        # We've reached the end of the lane path. If we like it then
        # we better put some Works in it.
        base_lane.parent = target
        target.sublanes.add(base_lane)

    def empty_lane(self, name=None, parent=None):
        """Creates a Work-less StaticFeedParentLane, either for the top
        level or somewhere along a Lane tree / path.
        """
        identifiers = []
        if not parent:
            # Create a top level lane.
            return StaticFeedParentLane(
                self._db, StaticFeedAnnotator.TOP_LEVEL_LANE_NAME,
                include_all=False,
                searchable=True,
                invisible=True
            )
        else:
            # Create a visible intermediate lane.
            return StaticFeedParentLane(
                self._db, name,
                parent=parent,
                include_all=False,
            )

    def create_feeds(self, lanes, feed_id, page_size, search_link=None):
        """Creates feeds for facets that may be required

        :return: A dictionary of filenames pointing to a list of CachedFeed
        objects representing pages
        """

        for lane in lanes:
            annotator = StaticFeedAnnotator(
                feed_id, lane, default_order=self.DEFAULT_ORDER, search_link=search_link
            )
            if lane.sublanes:
                # This is an intermediate lane, without its own works.
                # It needs a groups feed.
                filename = annotator.lane_filename(lane)
                feed = AcquisitionFeed.groups(
                    self._db, lane.name, feed_id, lane, annotator,
                    cache_type=self.CACHE_TYPE,
                    force_refresh=True,
                    use_materialized_works=False
                )
                yield filename, [feed]

                # Return filenames and feeds for any sublanes as well.
                for filename, feeds in self.create_feeds(
                    lane.sublanes, feed_id, page_size, search_link=search_link
                ):
                    yield filename, feeds
            else:
                static_facets = Facets(
                    collection=Facets.COLLECTION_FULL,
                    availability=Facets.AVAILABLE_OPEN_ACCESS,
                    order=Facets.ORDER_TITLE,
                    enabled_facets=self.DEFAULT_ENABLED_FACETS
                )

                for facet_group in list(static_facets.facet_groups):
                    ordered_by, facet_obj = facet_group[1:3]

                    pagination = Pagination(size=page_size)
                    feed_pages = self.create_feed_pages(
                        lane, pagination, feed_id, annotator, facet_obj
                    )

                    filename = annotator.lane_filename(lane)
                    if ordered_by != self.DEFAULT_ORDER:
                        filename += ('_' + ordered_by)
                    yield filename, feed_pages

    def create_feed_pages(self, lane, pagination, feed_id, annotator, facet):
        """Yields each page of the feed for a particular lane."""
        pages = list()
        previous_page = pagination.previous_page
        while not previous_page or previous_page.has_next_page:
            page = AcquisitionFeed.page(
                self._db, lane.name, feed_id, lane,
                annotator=annotator,
                facets=facet,
                pagination=pagination,
                cache_type=self.CACHE_TYPE,
                force_refresh=True,
                use_materialized_works=False
            )
            pages.append(page)

            # Reset values to determine if next page should be created.
            previous_page = pagination
            pagination = pagination.next_page
        return pages

    def upload(self, filename, content, uploader=None):
        uploader = uploader or S3Uploader()
        feed_representation = Representation()

        feed_representation.set_fetched_content(content, content_path=filename)
        feed_representation.mirror_url = uploader.feed_url(filename)

        self._db.add(feed_representation)
        self._db.commit()
        uploader.mirror_one(feed_representation)
