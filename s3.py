import logging
from nose.tools import set_trace

from sqlalchemy.orm import joinedload

from config import Configuration
from core.s3 import (
    S3Uploader as BaseS3Uploader,
    DummyS3Uploader as BaseDummyS3Uploader,
)


class S3Uploader(BaseS3Uploader):

    @classmethod
    def feed_url(cls, bucket, filename, extension='.xml'):
        """The path to the hosted file for an OPDS feed with the given filename"""
        root = cls.url(bucket, '/')
        if not extension.startswith('.'):
            extension = '.' + extension
        if not filename.endswith(extension):
            filename += extension
        return root + filename

    def delete_batch(self, keys, _db=None, external_hosts=None):
        """Deletes files identified by their keys (i.e. mirror urls)
        from s3 bucket and--if a database session is provided--their
        saved Representations.

        This method is intended for utility use.

            :param _db: A database session to delete Representations,
                Resources, and Hyperlinks associated with a particular
                S3 key / Representation.mirror_url.
            :param external_hosts: A list of strings representing hosts
                that are not relevant S3 buckets.
        """
        requests = list()
        keys = [unicode(k) for k in keys]

        if _db:
            from core.model import Representation, Resource
            representations = _db.query(Representation)\
                .join(Representation.resource).join(Resource.links)\
                .options(
                    joinedload(Representation.resource)\
                    .joinedload(Resource.links))\
                .filter(Representation.mirror_url.in_(keys))

            for representation in representations:
                url = representation.mirror_url
                resource = representation.resource
                logging.info('Deleting %s...', url)

                _db.delete(representation)
                logging.info("\t- DELETED Representation %r", representation)

                if resource:
                    count = len(resource.links)
                    [_db.delete(link) for link in resource.links]
                    logging.info("\t- DELETED %d Hyperlink(s)", count)

                    _db.delete(resource)
                    logging.info("\t- DELETED Resource %r", resource)

        for key in keys:
            bucket, key = self.bucket_and_filename(key)
            if bucket not in external_hosts:
                requests.append(self.pool.delete(key, bucket))

        successes = 0
        failures = list()
        for response in self.pool.all_completed(requests):
            if response.status_code / 100 == 2:
                logging.info("DELETED S3 file %s" % response.request.url)
                successes += 1
            else:
                status = response.status_code
                url = response.request.url
                logging.error("ERROR deleting file %s. Status code: %d", url, status)
                failures.append(url)

        logging.info("Successfully deleted %d files.", successes)
        if failures:
            failure_strings = '\n'.join(['\t- '+f for f in failures])
            logging.info("Error deleting %d files:\n%s", len(failures), failure_strings)


class DummyS3Uploader(BaseDummyS3Uploader, S3Uploader):
    pass
