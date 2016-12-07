from nose.tools import set_trace

from config import Configuration
from core.s3 import (
    S3Uploader as BaseS3Uploader,
    DummyS3Uploader as BaseDummyS3Uploader,
)


class S3Uploader(BaseS3Uploader):

    @classmethod
    def static_feed_root(cls, open_access=True):
        """The root URL to the S3 location of hosted content of
        the given type.
        """
        bucket = Configuration.s3_bucket(
            Configuration.S3_STATIC_FEED_BUCKET
        )
        return cls._static_feed_root(bucket, open_access)

    @classmethod
    def _static_feed_root(cls, bucket, open_access):
        if not open_access:
            raise NotImplementedError()
        return cls.url(bucket, '/')

    @classmethod
    def feed_url(cls, filename, extension='.xml', open_access=True):
        """The path to the hosted file for an OPDS feed with the given filename"""
        root = cls.static_feed_root(open_access)
        if not extension.startswith('.'):
            extension = '.' + extension
        if not filename.endswith(extension):
            filename += extension
        return root + filename


class DummyS3Uploader(BaseDummyS3Uploader, S3Uploader):

    @classmethod
    def static_feed_root(cls, open_access=True):
        return cls._static_feed_root('test.static_feed.bucket', open_access)
