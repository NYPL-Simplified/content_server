import contextlib
from nose.tools import (
    set_trace,
    eq_
)

from . import DatabaseTest

from ..config import (
    Configuration,
    temp_config as core_temp_config
)
from ..s3 import S3Uploader

class TestS3URLGeneration(DatabaseTest):

    @contextlib.contextmanager
    def temp_config(self):
        with core_temp_config() as tmp:
            i = tmp['integrations']
            S3 = Configuration.S3_INTEGRATION
            i[S3] = {
                Configuration.S3_STATIC_FEED_BUCKET : 'test-opds-feed-s3-bucket'
            }
            yield tmp

    def test_feed_root(self):
        with self.temp_config():
            eq_(
                "http://s3.amazonaws.com/test-opds-feed-s3-bucket/",
                S3Uploader.static_feed_root()
            )
