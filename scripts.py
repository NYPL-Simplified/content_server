import os
import datetime
datetime.datetime.strptime("2015","%Y")
from pymarc import MARCReader
from core.scripts import Script
from monitor import GutenbergMonitor
from coverage import (
    GutenbergIllustratedCoverageProvider,
    GutenbergEPUBCoverageProvider,
)
from core.model import (
    LicensePool,
    DataSource,
    Edition,
    Identifier,
    Representation,
    DeliveryMechanism,
    Contributor,
    Hyperlink,
    get_one,
)
from core.classifier import Classifier
from core.monitor import PresentationReadyMonitor

from core.metadata_layer import (
    ContributorData,
    Metadata,
    LinkData,
    IdentifierData,
    FormatData,
    MeasurementData,
    SubjectData,
)
from core.s3 import S3Uploader


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
        illustrated = GutenbergIllustratedCoverageProvider(self._db)
        epub = GutenbergEPUBCoverageProvider(self._db)

        providers = [illustrated, epub]
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
        with open(metadata_file) as f:
            reader = MARCReader(f)
            for record in reader:
                title = record.title()
                issued_year = datetime.datetime.strptime(record.pubyear(), "%Y.")
                publisher = record.publisher()
                summary = record.notes()[0]['a']

                isbn = record['020']['a'].split(" ")[0]
                primary_identifier = IdentifierData(
                    Identifier.ISBN, isbn
                )

                subjects = [SubjectData(
                    Classifier.LCSH,
                    subject['a'],
                ) for subject in record.subjects()]

                author = record.author()
                contributors = [ContributorData(
                    sort_name=author,
                    roles=[Contributor.AUTHOR_ROLE],
                )]
                
                uploader = S3Uploader()
                links = []

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
                links.append(epub_link)

                cover_file = isbn + ".jpg"
                cover_url = uploader.cover_image_url(
                    data_source_name, primary_identifier, cover_file)
                cover_path = os.path.join(directory, cover_file)
                paths[cover_url] = cover_path

                links.append(LinkData(
                    rel=Hyperlink.IMAGE,
                    href=cover_url,
                    media_type=Representation.JPEG_MEDIA_TYPE,
                ))

            
                metadata = Metadata(
                    data_source=data_source_name,
                    book_data_source=data_source_name,
                    title=title,
                    language='eng',
                    medium=Edition.BOOK_MEDIUM,
                    publisher=publisher,
                    issued=issued_year,
                    primary_identifier=primary_identifier,
                    subjects=subjects,
                    contributors=contributors,
                    formats=formats,
                    links=links,
                )
                license_pool, new = metadata.license_pool(self._db)
                edition, new = metadata.edition(self._db)
                if new:
                    print "created new edition %s" % edition.title
                
                for link in edition.primary_identifier.links:
                    representation = link.resource.representation
                    representation.mirror_url = link.resource.url
                    representation.local_content_path = paths[link.resource.url]
                    uploader.mirror_one(representation)                    
