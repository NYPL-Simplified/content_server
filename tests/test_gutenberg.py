# encoding: utf-8

import datetime
import os
import StringIO

from nose.tools import set_trace, eq_ 

from ..core.model import (
    Contributor,
    DataSource,
    Hyperlink,
    Resource,
    Subject,
    Identifier,
    Edition,
    RightsStatus,
    get_one_or_create,
)
from ..gutenberg import (
    GutenbergAPI,
    GutenbergRDFExtractor,
)
from . import DatabaseTest

class TestGutenbergMetadataExtractor(DatabaseTest):

    def sample_data(self, filename):
        base_path = os.path.split(__file__)[0]
        resource_path = os.path.join(base_path, "files", "gutenberg")
        path = os.path.join(resource_path, filename)
        return open(path).read()

    def test_rdf_parser(self):
        """Parse RDF into a Edition."""
        fh = StringIO.StringIO(self.sample_data("gutenberg-17.rdf"))
        book, pool, new = GutenbergRDFExtractor.book_in(self._db, "17", fh)

        # Verify that the Edition is hooked up to the correct
        # DataSource and Identifier.
        gutenberg = DataSource.lookup(self._db, DataSource.GUTENBERG)
        identifier, ignore = get_one_or_create(
            self._db, Identifier, type=Identifier.GUTENBERG_ID,
            identifier="17")
        eq_(gutenberg, book.data_source)
        eq_(identifier, book.primary_identifier)

        [canonical] = [x for x in book.primary_identifier.links
                       if x.rel == 'canonical']
        eq_("http://www.gutenberg.org/ebooks/17", canonical.resource.url)

        eq_("The Book of Mormon", book.title)
        eq_("An Account Written by the Hand of Mormon Upon Plates Taken from the Plates of Nephi", book.subtitle)

        eq_("Project Gutenberg", str(book.publisher))
        eq_("eng", book.language)

        eq_(datetime.date(2008, 6, 25), book.issued)

        for x in book.contributions:
            eq_("Author", x.role)

        a1, a2 = sorted(
            [x.contributor for x in book.contributions],
            key = lambda x: x.name)

        eq_("Church of Jesus Christ of Latter-day Saints", a1.name)

        eq_("Smith, Joseph, Jr.", a2.name)
        eq_(["Smith, Joseph"], a2.aliases)

        classifications = book.primary_identifier.classifications
        eq_(3, len(classifications))

        # The book has a LCC classification...
        [lcc] = [x.subject for x in classifications
                 if x.subject.type == Subject.LCC]
        eq_("BX", lcc.identifier)

        # ...and two LCSH classifications
        lcsh = [x.subject for x in classifications
                 if x.subject.type == Subject.LCSH]
        eq_([u'Church of Jesus Christ of Latter-day Saints -- Sacred books',
             u'Mormon Church -- Sacred books'], 
            sorted(x.identifier for x in lcsh))

        eq_(RightsStatus.PUBLIC_DOMAIN_USA, pool.delivery_mechanisms[0].rights_status.uri)
        eq_(True, pool.open_access)

    def test_unicode_characters_in_title(self):
        fh = StringIO.StringIO(self.sample_data("gutenberg-10130.rdf"))
        book, pool, new = GutenbergRDFExtractor.book_in(self._db, "10130", fh)
        eq_(u"The Works of Charles and Mary Lamb — Volume 3", book.title)
        eq_("Books for Children", book.subtitle)

    def test_includes_cover_image(self):
        fh = StringIO.StringIO(self.sample_data("gutenberg-40993.rdf"))
        book, pool, new = GutenbergRDFExtractor.book_in(self._db, "40993", fh)

        identifier = book.primary_identifier

        # The RDF includes a cover image, but we don't pick it up.
        # If we want to use it, we'll find it in the rsynced mirror.
        eq_([], [x for x in identifier.links if x.rel == Hyperlink.IMAGE])

    def test_rdf_file_describing_no_books(self):
        """GutenbergRDFExtractor can handle an RDF document that doesn't
        describe any books."""
        fh = StringIO.StringIO(self.sample_data("gutenberg-0.rdf"))
        book, pool, new = GutenbergRDFExtractor.book_in(self._db, "0", fh)
        eq_(None, book)
        eq_(False, new)

    def test_audio_book(self):
        """An audio book is loaded with its medium set to AUDIO."""
        fh = StringIO.StringIO(self.sample_data("pg28794.rdf"))
        book, pool, new = GutenbergRDFExtractor.book_in(self._db, "28794", fh)
        eq_(Edition.AUDIO_MEDIUM, book.medium)


    def test_non_public_domain_book(self):
        """The Gutenberg 'Peter Pan' has a weird copyright situation and will
        be treated as 'in copyright' rather than as an open access book.
        """
        fh = StringIO.StringIO(self.sample_data("pg16.rdf"))
        book, pool, new = GutenbergRDFExtractor.book_in(self._db, "16", fh)
        eq_(RightsStatus.IN_COPYRIGHT, pool.delivery_mechanisms[0].rights_status.uri)
        eq_(False, pool.open_access)

