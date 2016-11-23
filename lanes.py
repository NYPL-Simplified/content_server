from nose.tools import set_trace
from sqlalchemy.sql.expression import func

from core.model import (
    LicensePool,
    Work,
)
from core.lane import QueryGeneratedLane


class StaticFeedBaseLane(QueryGeneratedLane):

    def __init__(self, _db, identifiers, lane_name, featured=None, **kwargs):
        if not identifiers:
            raise ValueError(
                "IdentifierGeneratedLane can't be created without identifiers"
            )
        self.identifiers = identifiers
        self._featured = featured or list()

        full_name = display_name = lane_name
        super(StaticFeedBaseLane, self).__init__(
            _db, full_name, display_name=display_name, **kwargs
        )

    @property
    def featured(self):
        return self._featured

    def feature(self, identifiers):
        if not isinstance(identifiers, list):
            identifiers = [identifiers]

        excluded = filter(lambda i: i not in self.identifiers, identifiers)
        if excluded:
            raise ValueError(
                "Identifiers that are not in the lane cannot be featured:",
                ','.join(['%r' % i for i in excluded])
            )

        self._featured += identifiers

    def lane_query_hook(self, qu, work_model=Work):
        if work_model != Work:
            qu = qu.join(LicensePool.identifier)

        qu = Work.from_identifiers(
            self._db, self.identifiers, base_query=qu
        )
        return qu


class StaticFeedParentLane(QueryGeneratedLane):

    """An empty head or intermediate lane For use with static feeds"""

    FEATURED_LANE_SIZE = 10

    @classmethod
    def unify_lane_queries(cls, lanes):
        """Return a query encapsulating the entries in all of the lanes
        provided
        """
        queries = [l.works() for l in lanes]
        return queries[0].union(*queries[1:])

    @property
    def base_sublanes(self):
        base_sublanes = [s for s in self.sublanes if isinstance(s, StaticFeedBaseLane)]
        for s in self.sublanes:
            if isinstance(s, type(self)):
                base_sublanes += s.base_sublanes
        return base_sublanes

    def lane_query_hook(self, qu, work_model=Work):
        if not (self.parent and self.sublanes):
            return None
        return self.unify_lane_queries(self.sublanes)

    def featured_works(self, use_materialized_works=True):
        """Find a random sample of books for the feed"""
        sublane_features = list()
        for s in self.base_sublanes:
            sublane_features += s.featured

        works = list()
        from_sublanes = Work.from_identifiers(self._db, sublane_features)
        if from_sublanes:
            works += from_sublanes.all()

        remaining_spots = self.FEATURED_LANE_SIZE - len(works)
        if remaining_spots <= 0:
            return works

        if not use_materialized_works:
            qu = self.works()
        else:
            qu = self.materialized_works()
        if not qu:
            return []

        random = qu.order_by(func.random())
        if len(works) > 0:
            # Remove any duplicate works.
            subquery = from_sublanes.with_labels().subquery()
            random = random.outerjoin(subquery, Work.id==subquery.c.works_id).\
                filter(subquery.c.works_id==None)
        works += random.limit(remaining_spots).all()
        return works
