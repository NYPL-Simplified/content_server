from pymarc import MARCReader
import datetime
from core.metadata_layer import (
    Metadata,
    IdentifierData,
    SubjectData,
    ContributorData,
)
from core.classifier import Classifier
from core.model import (
    Identifier,
    Contributor,
    Edition,
)

class MARCExtractor(object):

    """Transform a MARC file into a list of Metadata objects."""

    @classmethod
    def parse(cls, file, data_source_name):
        reader = MARCReader(file)
        metadata_records = []
        
        for record in reader:
            title = record.title()
            issued_year = datetime.datetime.strptime(record.pubyear(), "%Y.")
            publisher = record.publisher()
            
            # Summary isn't part of metadata or edition, but we can get it from
            # a MARC file if we want it. 
            #summary = record.notes()[0]['a']
            
            isbn = record['020']['a'].split(" ")[0]
            primary_identifier = IdentifierData(
                Identifier.ISBN, isbn
            )

            subjects = [SubjectData(
                Classifier.FAST,
                subject['a'],
            ) for subject in record.subjects()]

            author = record.author()
            contributors = [ContributorData(
                sort_name=author,
                roles=[Contributor.AUTHOR_ROLE],
            )]

            metadata_records.append(Metadata(
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
            ))
        return metadata_records
