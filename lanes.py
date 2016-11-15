from nose.tools import set_trace
from sqlalchemy.sql.expression import func

from core.model import (
    LicensePool,
    Work,
)
from core.lane import QueryGeneratedLane

class IdentifiersLane(QueryGeneratedLane):

    def __init__(self, _db, identifiers, lane_name, **kwargs):
        if not identifiers:
            raise ValueError(
                "IdentifierGeneratedLane can't be created without identifiers"
            )

        self.identifiers = identifiers
        full_name = display_name = lane_name
        super(IdentifiersLane, self).__init__(
            _db, full_name, display_name=display_name, **kwargs
        )

    def lane_query_hook(self, qu, work_model=Work):
        if work_model != Work:
            qu = qu.join(LicensePool.identifier)

        qu = Work.from_identifiers(
            self._db, self.identifiers, base_query=qu
        )
        return qu


class StaticFeedParentLane(QueryGeneratedLane):

    FEATURED_LANE_SIZE = 10

    @classmethod
    def unify_lane_queries(cls, lanes):
        """Return a query encapsulating the entries in all of the lanes
        provided
        """
        queries = [l.works() for l in lanes]
        return queries[0].union(*queries[1:])

    def lane_query_hook(self, qu, work_model=Work):
        if not (self.parent and self.sublanes):
            return None

        return self.unify_lane_queries(self.sublanes)

    def featured_works(self, use_materialized_works=True):
        """Find a random sample of books for the feed"""

        if not use_materialized_works:
            qu = self.works()
        else:
            qu = self.materialized_works()
        if not qu:
            return []

        qu = qu.order_by(None)
        works = qu.order_by(func.random()).limit(self.FEATURED_LANE_SIZE).all()
        return works
