# encoding: utf-8
from pymarc import MARCReader
import datetime
import re
from core.metadata_layer import (
    Metadata,
    IdentifierData,
    SubjectData,
    ContributorData,
    LinkData,
)
from core.classifier import Classifier
from core.model import (
    Identifier,
    Contributor,
    Edition,
    Hyperlink,
    Representation,
)

from nose.tools import set_trace

class MARCExtractor(object):

    """Transform a MARC file into a list of Metadata objects."""

    END_OF_AUTHOR_NAME_RES = [
        re.compile(",\s+[0-9]+-"), # Birth year
        re.compile(",\s+active "),
        re.compile(",\s+graf,"),
        re.compile(",\s+author."),
    ]
    
    @classmethod
    def parse(cls, file, data_source_name):
        reader = MARCReader(file)
        metadata_records = []

        for record in reader:
            title = record.title()
            if title.endswith(' /'):
                title = title[:-len(' /')]
            issued_year = datetime.datetime.strptime(record.pubyear(), "%Y.")
            publisher = record.publisher()
            if publisher.endswith(','):
                publisher = publisher[:-1]
            
            links = []
            summary = record.notes()[0]['a']

            if summary and not summary.startswith('"Recovering the Classics is'):
                if summary.endswith("--Provided by publisher."):
                    summary = summary[:-len("--Provided by publisher.")]
                summary_link = LinkData(
                    rel=Hyperlink.DESCRIPTION,
                    media_type=Representation.TEXT_PLAIN,
                    content=summary,
                )
                links.append(summary_link)

            isbn = record['020']['a'].split(" ")[0]
            primary_identifier = IdentifierData(
                Identifier.ISBN, isbn
            )

            subjects = [SubjectData(
                Classifier.FAST,
                subject['a'],
            ) for subject in record.subjects()]

            author = record.author()
            if author:
                old_author = author
                # Turn 'Dante Alighieri,   1265-1321, author.'
                # into 'Dante Alighieri'. The metadata wrangler will
                # take it from there.
                for regex in cls.END_OF_AUTHOR_NAME_RES:
                    match = regex.search(author)
                    if match:
                        old_author = author
                        author = author[:match.start()]
                        break
                if author.startswith(u"Bront\xe8e"):
                    author = author.replace(u"\xe8e", u"ë")
                if author == u'Gogol§, Nikolaæi Vasil§evich':
                    author = "Gogol, Nikolai Vasilievich"
                if author == u'Zola, \xe2Emile':
                    author = u"Zola, Émile"
                if author.startswith('Tolstoy, Leo, '):
                    author = 'Tolstoy, Leo'
                author_names = [author]
            else:
                if title == 'The Federalist Papers':
                    author_names = [
                        'Hamilton, Alexander',
                        'Madison, James',
                        'Jay, John'
                    ]
                else:
                    author_names = ['Anonymous']
            contributors = [
                ContributorData(
                    sort_name=author,
                    roles=[Contributor.AUTHOR_ROLE],
                )
                for author in author_names
            ]
                    
                    
                
            metadata_records.append(Metadata(
                data_source=data_source_name,
                title=title,
                language='eng',
                medium=Edition.BOOK_MEDIUM,
                publisher=publisher,
                issued=issued_year,
                primary_identifier=primary_identifier,
                subjects=subjects,
                contributors=contributors,
                links=links
            ))
        return metadata_records
