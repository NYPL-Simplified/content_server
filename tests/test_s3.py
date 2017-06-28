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

    def test_feed_url(self):
        eq_('http://s3.amazonaws.com/test.feed.bucket/my_file.xml',
            S3Uploader.feed_url('test.feed.bucket', 'my_file'))

        eq_('http://s3.amazonaws.com/test.feed.bucket/my_file.banana',
            S3Uploader.feed_url('test.feed.bucket', 'my_file', extension='banana'))
