from nose.tools import eq_, set_trace

from . import DatabaseTest

from ..lanes import (
    StaticFeedBaseLane,
    StaticFeedParentLane,
)

class TestStaticFeedParentLane(DatabaseTest):

    def create_work_lane(self, name):
        work = self._work(with_open_access_download=True)
        identifier = work.license_pools[0].identifier
        lane = StaticFeedBaseLane(self._db, [identifier], name)
        return work, identifier, lane

    def test_unify_lane_queries(self):

        w1, w1_identifier, w1_lane = self.create_work_lane("W1")
        w2, w2_identifier, w2_lane = self.create_work_lane("W2")
        w3, w3_identifier, w3_lane = self.create_work_lane("W3")

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

    def test_featured_works(self):
        # A lane to be ignored by all searches, for science.
        _ = self._work(with_open_access_download=True)

        # Set the featured lane size smaller to minimize sampling needs.
        class SmallFeatureSizeLane(StaticFeedParentLane):
            FEATURED_LANE_SIZE = 2
        parent = SmallFeatureSizeLane(self._db, 'Test Parent')
        lane = SmallFeatureSizeLane(self._db, 'Test Lane', parent=parent)

        # With only one work, only one featured work is returned.
        # There aren't any duplicates.
        w1, w1_identifier, w1_lane = self.create_work_lane("W1")
        w1_lane.featured.append(w1_identifier)
        w1_lane.parent = lane
        lane.sublanes.add(w1_lane)
        result = lane.featured_works()
        eq_([w1], result)

        # When more works are available, they're added to fill the
        # feature amount, even if they're not marked as featured.
        w2, w2_identifier, w2_lane = self.create_work_lane("W2")
        w2_lane.parent = lane
        lane.sublanes.add(w2_lane)
        result = lane.featured_works()
        eq_(sorted([w1, w2]), sorted(result))

        # The featured work will always be in the query, even when
        # random works are also selected.
        w3, w3_identifier, w3_lane = self.create_work_lane("W3")
        w3_lane.parent = lane
        lane.sublanes.add(w3_lane)
        result = lane.featured_works()
        eq_(2, len(result))
        assert w1 in result

        # If featured works can fill the lane size, random works aren't
        # included at all.
        w3_lane.featured.append(w3_identifier)
        result = lane.featured_works()
        eq_(sorted([w1, w3]), sorted(result))

        # If featured works go above the lane size, they're all included.
        w2_lane.featured.append(w2_identifier)
        result = lane.featured_works()
        eq_(sorted([w1, w2, w3]), sorted(result))

        # Parent lanes retrieve the featured works of sublanes.
        parent.sublanes.add(lane)
        result = parent.featured_works()
        eq_(sorted([w1, w2, w3]), sorted(result))
