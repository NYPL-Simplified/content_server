import feedparser
from nose.tools import set_trace, eq_

from . import DatabaseTest

from ..core.model import (
    Edition,
    Representation,
    Work,
)
from ..core.opds import AcquisitionFeed
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

        # Feeds are created and uploaded for the main feed and its facets.
        eq_(2, len(uploader.content))
        for feed in uploader.content:
            parsed = feedparser.parse(feed)
            eq_(u'mta.librarysimplified.org', parsed.feed.id)
            eq_(u'Test Feed', parsed.feed.title)

            # There are links for the different facets.
            links = parsed.feed.links
            eq_(2, len([l for l in links if l.get('facetgroup')]))

            # Only the non-suppressed, license_pooled works we requested
            # are in the entry feed.
            [entry] = parsed.entries
            eq_(requested.title, entry.title)

        # There should also be a Representation saved to the database
        # for each feed.
        representations = self._db.query(Representation).all()
        # Representations with "Dummy content" are created in _license_pool()
        # for each working. We'll ignore these.
        representations = [r for r in representations if r.content != 'Dummy content']
        eq_(2, len(representations))

    def test_create_faceted_feeds(self):
        omega = self._work(title='Omega', authors='Iota', with_open_access_download=True)
        alpha = self._work(title='Alpha', authors='Theta', with_open_access_download=True)
        zeta = self._work(title='Zeta', authors='Phi', with_open_access_download=True)

        qu = self._db.query(Work, Edition).join(Work.presentation_edition)
        script = CustomOPDSFeedGenerationScript(_db=self._db)
        result = script.create_faceted_feeds(
            qu, 'Test Feed', 'https://mta.librarysimplified.org'
        )

        eq_(True, isinstance(result, dict))
        eq_(['default', 'ordered-by-author'], sorted(result.keys()))
        for feed in result.values():
            eq_(True, isinstance(feed, AcquisitionFeed))
            parsed = feedparser.parse(unicode(feed))
            [active_facet_link] = [l for l in parsed.feed.links if l.get('activefacet')]

            if active_facet_link.get('title') == 'Title':
                # The entries should be sorted by title.
                titles = [e.title for e in parsed.entries]
                eq_(['Alpha', 'Omega', 'Zeta'], titles)

            if active_facet_link.get('title') == 'Author':
                authors = [e.simplified_sort_name for e in parsed.entries]
                eq_(['Iota', 'Phi', 'Theta'], authors)
