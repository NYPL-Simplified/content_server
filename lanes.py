import random
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
                "StaticFeedBaseLane can't be created without identifiers"
            )
        self.identifiers = identifiers
        self.featured = featured or list()

        full_name = display_name = lane_name
        super(StaticFeedBaseLane, self).__init__(
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

    """An empty head or intermediate lane For use with static feeds"""

    @property
    def base_sublanes(self):
        base_sublanes = [s for s in self.sublanes if isinstance(s, StaticFeedBaseLane)]
        for s in self.sublanes:
            if isinstance(s, type(self)):
                base_sublanes += s.base_sublanes
        return base_sublanes

    def lane_query_hook(self, qu, work_model=Work):
        if work_model != Work:
            qu = qu.join(LicensePool.identifier)

        identifiers = list()
        for lane in self.base_sublanes:
            identifiers += lane.identifiers
        return Work.from_identifiers(self._db, identifiers, base_query=qu)
