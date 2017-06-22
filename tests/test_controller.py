# encoding=utf8
from nose.tools import (
    eq_,
    set_trace,
)

from flask import url_for

from . import DatabaseTest
from ..opds import ContentServerAnnotator
from ..config import(
    Configuration,
    temp_config,
)
import os
from ..controller import (
    ContentServer,
    ContentServerController,
)

from ..core.app_server import (
    load_facets_from_request,
    load_pagination_from_request,
)

from ..core.model import (
    DataSource,
    SessionManager,
)

from ..core.lane import(
    Facets,
    Pagination,
    Lane,
)

import feedparser


class TestContentServer(ContentServer):
    pass

class ControllerTest(DatabaseTest):
    def setup(self):
        super(ControllerTest, self).setup()
        os.environ['AUTOINITIALIZE'] = "False"
        from ..app import app
        del os.environ['AUTOINITIALIZE']
        self.app = app
        # Create two English books and a French book.
        self.english_1 = self._work(
            "Quite British", "John Bull", language="eng", fiction=True,
            with_open_access_download=True
        )

        self.english_2 = self._work(
            "Totally American", "Uncle Sam", language="eng", fiction=False,
            with_open_access_download=True
        )
        self.french_1 = self._work(
            u"Très Français", "Marianne", language="fre", fiction=False,
            with_open_access_download=True
        )

        self.content_server = TestContentServer(self._db, testing=True)
        self.app.content_server = self.content_server
        self.controller = ContentServerController(self.content_server)
        SessionManager.refresh_materialized_views(self._db)
        
        
class TestFeedController(ControllerTest):

    def test_feed(self):
        with self.app.test_request_context("/"):
            response = self.content_server.opds_feeds.feed()

            assert self.english_1.title in response.data
            assert self.english_2.title in response.data
            assert self.french_1.author in response.data

    def test_verify_default_feed_facets(self):
        with self.app.test_request_context('/?size=2'):
            response = self.content_server.opds_feeds.feed()
            feed = feedparser.parse(response.data)

            links = feed.feed.links
            next_link = [x for x in links if x['rel'] == 'next'][0]['href']

            assert 'order=added' in next_link
            assert 'collection=full' in next_link
            assert 'available=always' in next_link

    def test_lane_feed(self):
        with self.app.test_request_context("/?size=2"):
            # This request finds two of the three test books, since they're
            # all in Gutenberg.
            license_source = DataSource.lookup(self._db, DataSource.GUTENBERG)
            response = self.content_server.opds_feeds.feed(
                license_source_name=license_source.name
            )
            feed = feedparser.parse(response.data)
            entries = feed['entries']
            eq_(2, len(entries))

            [next_url] = [x['href'] for x in feed['feed']['links']
                          if x['rel'] == 'next']

            # Verify that the next link comes from feed_url() and points
            # to the next page.
            annotator = ContentServerAnnotator()
            facets = load_facets_from_request(Configuration)
            pagination = load_pagination_from_request().next_page
            lane = Lane(self._default_library, "test", license_source=license_source)
            expect = annotator.feed_url(lane, facets, pagination)
            eq_(expect, next_url)

            # Verify that the next link goes to the feed_from_license_source
            # controller.
            expect = url_for(
                "feed_from_license_source", 
                license_source_name=license_source.name,
                _external=True
            )
            assert next_url.startswith(expect)

        with self.app.test_request_context('/?after=2&size=2'):
            # Getting the next page finds the remaining book.
            response = self.content_server.opds_feeds.feed(
                license_source_name=DataSource.GUTENBERG
            )
            feed = feedparser.parse(response.data)
            eq_(1, len(feed.entries))

        with self.app.test_request_context('/'):
            # This request for Overdrive finds zero books.
            response = self.content_server.opds_feeds.feed(
                license_source_name=DataSource.OVERDRIVE
            )
            feed = feedparser.parse(response.data)
            entries = feed['entries']
            eq_(0, len(entries))

    def test_multipage_feed(self):
        with self.app.test_request_context("/?size=1&order=title"):
            
            response = self.content_server.opds_feeds.feed()

            assert self.english_1.title in response.data
            assert self.english_2.title not in response.data
            assert self.french_1.author not in response.data

            feed = feedparser.parse(response.data)
            entries = feed['entries']

            eq_(1, len(entries))
            
            links = feed['feed']['links']

            next_link = [x for x in links if x['rel'] == 'next'][0]['href']
            assert 'after=1' in next_link
            assert 'size=1' in next_link
            assert 'order=title' in next_link

    def test_verbose_opds_entry(self):
        engdahl, new_contributor = self._contributor(
            name = u"Sylvia Engdahl",
            family_name = u"Engdahl",
            wikipedia_name = u"Sylvia_Louise_Engdahl"
        )
        self.english_1.presentation_edition.add_contributor(engdahl, "Author")
        self.english_1.calculate_opds_entries(verbose=False)
        SessionManager.refresh_materialized_views(self._db)


        assert "family_name" not in self.english_1.simple_opds_entry
        assert "Louise" not in self.english_1.simple_opds_entry

        with self.app.test_request_context("/"):
            response = self.content_server.opds_feeds.feed()
            assert "family_name" in response.data
            assert "Sylvia_Louise_Engdahl" in response.data
