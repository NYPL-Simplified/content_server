import feedparser
from nose.tools import set_trace, eq_

from . import DatabaseTest

from ..core.s3 import DummyS3Uploader

from ..scripts import CustomOPDSFeedGenerationScript

class TestCustomOPDSFeedGenerationScript(DatabaseTest):

    def test_slugify_feed_title(self):
        script = CustomOPDSFeedGenerationScript
        eq_('hey-im-a-feed', script.slugify_feed_title("Hey! I'm a feed!!"))
        eq_('you-and-me-n-every_feed', script.slugify_feed_title("You & Me n Every_Feed"))
        eq_('money-honey', script.slugify_feed_title("Money $$$       Honey"))

    def test_run(self):
        not_requested = self._work(with_open_access_download=True)
        requested = self._work(with_open_access_download=True)

        # Works with suppressed LicensePools aren't added to the feed.
        suppressed = self._work(with_open_access_download=True)
        suppressed.license_pools[0].suppressed = True

        # Identifiers without LicensePools are ignored.
        no_pool = self._identifier().urn
        urn1 = requested.license_pools[0].identifier.urn
        urn2 = suppressed.license_pools[0].identifier.urn

        script = CustomOPDSFeedGenerationScript(_db=self._db)
        uploader = DummyS3Uploader()
        cmd_args = ['-t', 'Test Feed', '-d', 'mta.librarysimplified.org',
                    '-u', no_pool, urn1, urn2]

        script.run(uploader=uploader, cmd_args=cmd_args)
        parsed = feedparser.parse(uploader.content[0])
        eq_(u'mta.librarysimplified.org', parsed.feed.id)
        eq_(u'Test Feed', parsed.feed.title)

        # Only the non-suppressed, license_pooled work we requested is in the entry feed.
        [entry] = parsed.entries
        eq_(requested.title, entry.title)
