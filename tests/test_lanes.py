from nose.tools import eq_

from . import DatabaseTest

from ..lanes import (
    IdentifiersLane,
    StaticFeedParentLane,
)

class TestStaticFeedParentLane(DatabaseTest):

    def test_unify_lane_queries(self):
        def create_work_lane(name):
            work = self._work(with_open_access_download=True)
            identifier = work.license_pools[0].identifier
            lane = IdentifiersLane(self._db, [identifier], name)
            return work, identifier, lane

        w1, w1_identifier, w1_lane = create_work_lane("W1")
        w2, w2_identifier, w2_lane = create_work_lane("W2")
        w3, w3_identifier, w3_lane = create_work_lane("W3")

        # The results of  all of the results from a number of lanes.
        result_qu = StaticFeedParentLane.unify_lane_queries([w1_lane, w2_lane])
        eq_(sorted([w1, w2]), sorted(result_qu.all()))

        # Even if there are more than two lanes.
        result_qu = StaticFeedParentLane.unify_lane_queries([w2_lane, w1_lane, w3_lane])
        eq_(sorted([w1, w2, w3]), sorted(result_qu.all()))

        # Or only one.
        result_qu = StaticFeedParentLane.unify_lane_queries([w3_lane])
        eq_([w3], result_qu.all())

        # Only distinct works are returned.
        w2_lane.identifiers.append(w1_identifier)
        result_qu = StaticFeedParentLane.unify_lane_queries([w1_lane, w2_lane])
        eq_(sorted([w1, w2]), sorted(result_qu.all()))
