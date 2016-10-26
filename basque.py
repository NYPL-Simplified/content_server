from nose.tools import set_trace
import csv
import datetime
import re

from core.metadata_layer import (
    Metadata,
    IdentifierData,
    ContributorData,
    SubjectData,
)
from core.classifier import Classifier
from core.model import (
    Identifier,
    Contributor,
    Edition,
)
from core.util import LanguageCodes

class BasqueMetadataExtractor(object):

    """Transfrom the Basque metadata spreadsheet into a list of Metadata objects."""

    @classmethod
    def parse(cls, file, data_source_name):
        metadata_records = []
        reader = csv.DictReader(file)

        for row in reader:
            publisher = unicode(row.get('Sello Editorial'), 'utf-8')
            title = unicode(row.get('Title'), 'utf-8')

            # The spreadsheet's identifier column is labeled ISBN, but
            # contains custom eLiburutegia IDs, like "ELIB201600288".
            identifier = row.get('ISBN')
            primary_identifier = IdentifierData(
                Identifier.ELIB_ID, identifier)

            issued_date = datetime.datetime.strptime(row.get('Publication Date'), "%m/%d/%Y")

            author = unicode(row.get('Author'), 'utf-8')
            contributors = [ContributorData(
                sort_name=author,
                roles=[Contributor.AUTHOR_ROLE]
            )]
            
            subjects = []
            bisac = row.get('BISAC')
            if bisac:
                subjects.append(SubjectData(Classifier.BISAC, bisac))

            ibic = row.get('IBIC')
            if ibic:
                # I haven't found any documentation on IBIC, so I am
                # treating it as BIC for now. It's possible that some
                # of the codes won't be valid BIC codes, but they'll
                # just be ignored.
                subjects.append(SubjectData(Classifier.BIC, ibic))

            age = row.get('Age')
            if age:
                age_re = re.compile(".*\(([\d-]+)\)")
                match = age_re.match(age)
                if match:
                    subjects.append(SubjectData(Classifier.AGE_RANGE, match.groups()[0]))            

            language = row.get('Language')
            if language:
                language = LanguageCodes.string_to_alpha_3(language)

            metadata_records.append(Metadata(
                data_source=data_source_name,
                title=title,
                language=language,
                medium=Edition.BOOK_MEDIUM,
                publisher=publisher,
                issued=issued_date,
                primary_identifier=primary_identifier,
                contributors=contributors,
                subjects=subjects,
            ))
        return metadata_records
