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

    def delete_batch(self, keys, _db=None):
        """Deletes files identified by their keys (i.e. mirror urls)
        from s3 bucket and--if a database session is provided--their
        saved Representations.

        This method is intended for utility use.
        """
        requests = list()
        if _db:
            from model import Representation, get_one
            for key in keys:
                representation = get_one(_db, Representation, mirror_url=unicode(key))
                if representation:
                    logging.info("DELETED Representation for %s" % key)
                    _db.delete(representation)
            _db.commit()

        for key in keys:
            bucket, key = self.bucket_and_filename(key)
            requests.append(self.pool.delete(key, bucket))

        for response in self.pool.all_completed(requests):
            if response.status_code / 100 == 2:
                logging.info("DELETED S3 file %s" % response.request.url)
            else:
                status = response.status_code
                url = response.request.url
                logging.error("ERROR deleting file %s. Status code: %d", url, status)


class DummyS3Uploader(BaseDummyS3Uploader, S3Uploader):

    @classmethod
    def static_feed_root(cls, open_access=True):
        return cls._static_feed_root('test.static_feed.bucket', open_access)
