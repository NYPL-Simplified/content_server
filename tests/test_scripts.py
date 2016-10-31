from nose.tools import set_trace, eq_

from . import DatabaseTest
from ..scripts import CustomOPDSFeedGenerationScript

class TestCustomOPDSFeedGenerationScript(DatabaseTest):

    def test_slugify_feed_title(self):
        script = CustomOPDSFeedGenerationScript
        eq_('hey-im-a-feed', script.slugify_feed_title("Hey! I'm a feed!!"))
        eq_('you-and-me-n-every_feed', script.slugify_feed_title("You & Me n Every_Feed"))
        eq_('money-honey', script.slugify_feed_title("Money $$$       Honey"))

    

