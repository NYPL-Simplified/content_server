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
    RightsStatus
)
from ..core.metadata_layer import (
    Metadata,
    LinkData,
)
from ..core.opds import OPDSFeed
from ..core.s3 import DummyS3Uploader
from ..core.testing import DummyHTTPClient

LIFE_PLUS_70 = "This work is available for countries where copyright is Life+70."

class TestFeedbooksOPDSImporter(DatabaseTest):

    def setup(self):
        super(TestFeedbooksOPDSImporter, self).setup()
        self.http = DummyHTTPClient()
        self.importer = FeedbooksOPDSImporter(
            self._db, http_get = self.http.do_get
        )
        self.data_source = DataSource.lookup(
            self._db, self.importer.DATA_SOURCE_NAME, autocreate=True
        )
        
        base_path = os.path.split(__file__)[0]
        self.resource_path = os.path.join(base_path, "files", "feedbooks")

    def sample_file(self, filename):
        path = os.path.join(self.resource_path, filename)
        data = open(path).read()
        return data

    def test_rights_for_entry(self):
        entry = dict(rights=LIFE_PLUS_70,
                     source='gutenberg.net.au',
                     publication_year="1922")
        rights = self.importer.rights_for_entry(entry) 
        eq_(RightsStatus.CC_BY_NC, rights)

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
