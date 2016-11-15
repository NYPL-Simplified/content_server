# encoding: utf-8
import os
from nose.tools import (
    eq_,
    set_trace,
)
from . import DatabaseTest
from ..feedbooks import (
    FeedbooksOPDSImporter,
    RehostingPolicy,
)
from ..core.model import (
    DataSource,
    Hyperlink,
    Representation,
    RightsStatus,
)
from ..core.metadata_layer import (
    Metadata,
    LinkData,
)
from ..core.opds import OPDSFeed
from ..core.s3 import DummyS3Uploader
from ..core.testing import (
    DummyHTTPClient,
    DummyMetadataClient,
)

LIFE_PLUS_70 = "This work is available for countries where copyright is Life+70."

class TestFeedbooksOPDSImporter(DatabaseTest):

    def setup(self):
        super(TestFeedbooksOPDSImporter, self).setup()
        self.http = DummyHTTPClient()
        self.metadata = DummyMetadataClient()
        self.mirror = DummyS3Uploader()
        self.importer = FeedbooksOPDSImporter(
            self._db, http_get = self.http.do_get,
            mirror=self.mirror,
            metadata_client=self.metadata,
        )
        self.data_source = DataSource.lookup(
            self._db, self.importer.DATA_SOURCE_NAME, autocreate=True,
            offers_licenses=True
        )
        
        base_path = os.path.split(__file__)[0]
        self.resource_path = os.path.join(base_path, "files", "feedbooks")

    def sample_file(self, filename):
        path = os.path.join(self.resource_path, filename)
        data = open(path).read()
        return data

    def test_rights_uri_from_feedparser_entry(self):
        entry = dict(rights=LIFE_PLUS_70,
                     source='gutenberg.net.au')
        expect = RehostingPolicy.rights_uri(
            LIFE_PLUS_70, 'gutenberg.net.au', None
        )
        actual = self.importer.rights_uri_from_feedparser_entry(entry) 
        eq_(expect, actual)

    def test_extract_feed_data_improves_descriptions(self):
        feed = self.sample_file("feed.atom")
        self.http.queue_response(200, OPDSFeed.ENTRY_TYPE,
                                 content=self.sample_file("677.atom"))
        metadata, failures = self.importer.extract_feed_data(
            feed, "http://url/"
        )
        [(key, value)] = metadata.items()
        eq_(u'http://www.feedbooks.com/book/677', key)
        eq_("Discourse on the Method", value.title)

        # Instead of the short description from feed.atom, we have the
        # long description from 677.atom.
        [description] = [x for x in value.links if x.rel==Hyperlink.DESCRIPTION]
        eq_(1818, len(description.content))
        
    def test_improve_description(self):
        # Here's a Metadata that has a bad (truncated) description.
        metadata = Metadata(self.data_source)

        bad_description = LinkData(rel=Hyperlink.DESCRIPTION, media_type="text/plain", content=u"The Discourse on the Method is a philosophical and mathematical treatise published by Ren\xe9 Descartes in 1637. Its full name is Discourse on the Method of Rightly Conducting the Reason, and Searching for Truth in the Sciences (French title: Discour...")

        irrelevant_description = LinkData(
            rel=Hyperlink.DESCRIPTION, media_type="text/plain",
            content="Don't look at me; I'm irrelevant!"
        )
        
        # Sending an HTTP request to this URL is going to give a 404 error.
        alternate = LinkData(rel=Hyperlink.ALTERNATE, href="http://foo/",
                             media_type=OPDSFeed.ENTRY_TYPE)

        # We're not even going to try to send an HTTP request to this URL
        # because it doesn't promise an OPDS entry.
        alternate2 = LinkData(rel=Hyperlink.ALTERNATE, href="http://bar/",
                             media_type="text/html")
        
        # But this URL will give us full information about this
        # entry, including a better description.
        alternate3 = LinkData(
            rel=Hyperlink.ALTERNATE, href="http://baz/",
            media_type=OPDSFeed.ENTRY_TYPE
        )

        # This URL will not be requested because the third alternate URL
        # gives us the answer we're looking for.
        alternate4 = LinkData(
            rel=Hyperlink.ALTERNATE, href="http://qux/",
            media_type=OPDSFeed.ENTRY_TYPE
        )
        
        # Two requests will be made. The first will result in a 404
        # error. The second will give us an OPDS entry.
        self.http.queue_response(200, OPDSFeed.ENTRY_TYPE,
                                 content=self.sample_file("677.atom"))
        self.http.queue_response(404, content="Not found")
        
        metadata.links = [bad_description, irrelevant_description,
                          alternate, alternate2, alternate3, alternate4]

        self.importer.improve_description("some ID", metadata)

        # The descriptions have been removed from metatadata.links,
        # because 677.atom included a description we know was better.
        #
        # The incomplete description was removed even though 677.atom
        # also included a copy of it.
        assert bad_description not in metadata.links
        assert irrelevant_description not in metadata.links
        
        # The more complete description from 677.atom has been added.
        [good_description] = [
            x for x in metadata.links if x.rel == Hyperlink.DESCRIPTION
        ]
        
        # The four alternate links have not been touched.
        assert (alternate in metadata.links)
        assert (alternate2 in metadata.links)
        assert (alternate3 in metadata.links)
        assert (alternate4 in metadata.links)

        # Two HTTP requests were made.
        eq_(['http://foo/', 'http://baz/'], self.http.requests)

    def test_generic_acquisition_link_picked_up_as_open_access(self):
        feed = self.sample_file("feed_with_open_access_book.atom")
        imports, errors = self.importer.extract_feed_data(feed)
        [book] = imports.values()
        open_access_links = [x for x in book.circulation.links
                             if x.rel==Hyperlink.OPEN_ACCESS_DOWNLOAD]
        links = sorted(x.href for x in open_access_links)
        eq_(['http://www.feedbooks.com/book/677.epub',
             'http://www.feedbooks.com/book/677.mobi',
             'http://www.feedbooks.com/book/677.pdf'], links)

    def test_open_access_book_mirrored(self):
        feed = self.sample_file("feed_with_open_access_book.atom")
        self.http.queue_response(
            200, OPDSFeed.ACQUISITION_FEED_TYPE,
            content=feed
        )
        self.metadata.lookups = { u"René Descartes" : "Descartes, Rene" }

        # The request to
        # http://covers.feedbooks.net/book/677.jpg?size=large&t=1428398185'
        # will result in a 404 error, and the image will not be
        # mirrored.
        self.http.queue_response(404, media_type="text/plain")

        # The requests to the various copies of the book will succeed,
        # and the books will be mirrored.
        self.http.queue_response(
            200, content='I am 667.pdf',
            media_type=Representation.PDF_MEDIA_TYPE,
        )
        self.http.queue_response(
            200, content="I am 667.mobi",
            media_type="application/x-mobipocket-ebook"
        )
        self.http.queue_response(
            200, content='I am 667.epub',
            media_type=Representation.EPUB_MEDIA_TYPE
        )

        [edition], [pool], [work], failures = self.importer.import_from_feed(
            feed, immediately_presentation_ready=True,
        )

        eq_({}, failures)

        # The work has been created and has metadata.
        eq_("Discourse on the Method", work.title)
        eq_(u'Ren\xe9 Descartes', work.author)

        # Four mock HTTP requests were made.
        eq_(['http://www.feedbooks.com/book/677.epub',
             'http://www.feedbooks.com/book/677.mobi',
             'http://www.feedbooks.com/book/677.pdf',
             'http://covers.feedbooks.net/book/677.jpg?size=large&t=1428398185'],
            self.http.requests
        )

        # Three 'books' were uploaded to the mock S3 service.
        eq_(['http://s3.amazonaws.com/test.content.bucket/FeedBooks/URI/http%3A//www.feedbooks.com/book/677/Discourse%20on%20the%20Method.' + extension
             for extension in 'epub', 'mobi', 'pdf'
        ],
            [x.mirror_url for x in self.mirror.uploaded]
        )
        eq_(
            [u'application/epub+zip', 'application/x-mobipocket-ebook',
             'application/pdf'],
            [x.delivery_mechanism.content_type
             for x in pool.delivery_mechanisms]
        )            

        # From information contained in the OPDS entry we determined
        # all three links to be CC-BY-NC.
        eq_([u'https://creativecommons.org/licenses/by-nc/4.0'] * 3,
            [x.rights_status.uri for x in pool.delivery_mechanisms])
        
    def test_in_copyright_book_not_mirrored(self):

        feed = self.sample_file("feed_with_in_copyright_book.atom")
        self.http.queue_response(
            200, OPDSFeed.ACQUISITION_FEED_TYPE,
            content=feed
        )

        [edition], [pool], [work], failures = self.importer.import_from_feed(
            feed, immediately_presentation_ready=True,
        )
        set_trace()
        pass
        
class TestRehostingPolicy(object):
    
    def test_rights_uri(self):
        # A Feedbooks work based on a text that is in copyright in the
        # US gets a RightsStatus of IN_COPYRIGHT.  We will not be
        # hosting this book and if we should host it by accident we
        # will not redistribute it.
        pd_in_australia_only = RehostingPolicy.rights_uri(
            LIFE_PLUS_70, "gutenberg.net.au", 1930
        )
        eq_(RightsStatus.IN_COPYRIGHT, pd_in_australia_only)

        unknown_australia_publication = RehostingPolicy.rights_uri(
            LIFE_PLUS_70, "gutenberg.net.au", None
        )
        eq_(RightsStatus.IN_COPYRIGHT, unknown_australia_publication)
        
        # A Feedbooks work based on a text that is in the US public
        # domain is relicensed to us as CC-BY-NC.
        pd_in_us = RehostingPolicy.rights_uri(
            LIFE_PLUS_70, "gutenberg.net.au", 1922
        )
        eq_(RightsStatus.CC_BY_NC, pd_in_us)

        # A Feedbooks work based on a text whose CC license is not
        # compatible with CC-BY-NC is relicensed to us under the
        # original license.
        sharealike = RehostingPolicy.rights_uri(
            "Attribution Share Alike (cc by-sa)", "mywebsite.com", 2016
        )
        eq_(RightsStatus.CC_BY_SA, sharealike)

        # A Feedbooks work based on a text whose rights status cannot
        # be determined gets an unknown RightsStatus. We will not be
        # hosting this book, but we might change our minds after
        # investigating.
        unknown = RehostingPolicy.rights_uri(
            RehostingPolicy.RIGHTS_UNKNOWN, "mywebsite.com", 2016
        )
        eq_(RightsStatus.UNKNOWN, unknown)
        
        
    def test_can_rehost_us(self):
        # We will rehost anything published prior to 1923.
        eq_(
            True, RehostingPolicy.can_rehost_us(
                LIFE_PLUS_70, "gutenberg.net.au", 1922
            )
        )

        # We will rehost anything whose rights statement explicitly
        # indicates it can be rehosted in the US, no matter the
        # issuance date.
        for terms in RehostingPolicy.CAN_REHOST_IN_US:
            eq_(
                True, RehostingPolicy.can_rehost_us(
                    terms, "gutenberg.net.au", 2016
                )
            )

        # We will rehost anything that originally derives from a
        # US-based site that specializes in open-access books.
        for site in list(RehostingPolicy.US_SITES) + [
                "WikiSource", "Gutenberg", "http://gutenberg.net/"
        ]:
            eq_(
                True, RehostingPolicy.can_rehost_us(
                    None, site, 2016
                )
            )
            
        # If none of these conditions are met we will not rehost a
        # book.
        eq_(
            False, RehostingPolicy.can_rehost_us(
                LIFE_PLUS_70, "gutenberg.net.au", 1930
            )
        )

        # If a book would require manual work to determine copyright
        # status, we will distinguish slightly between that case and
        # the case where we're pretty sure.
        eq_(
            None, RehostingPolicy.can_rehost_us(
                RehostingPolicy.RIGHTS_UNKNOWN, "Some random website", 2016
            )
        )
