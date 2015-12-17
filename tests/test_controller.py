# encoding=utf8
from nose.tools import (
    eq_,
    set_trace,
)

from . import DatabaseTest
import os
from ..controller import (
    ContentServer,
    ContentServerController,
)

from ..core.model import (
    SessionManager
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

        
        
class TestFeedController(ControllerTest):

    def test_feed(self):
        SessionManager.refresh_materialized_views(self._db)

        with self.app.test_request_context("/"):
            response = self.content_server.opds_feeds.feed()

            assert self.english_1.title in response.data
            assert self.english_2.title in response.data
            assert self.french_1.author in response.data

    def test_multipage_feed(self):
        SessionManager.refresh_materialized_views(self._db)

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