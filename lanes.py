from core.model import (
    LicensePool,
    Work,
)
from core.lane import QueryGeneratedLane

class IdentifiersLane(QueryGeneratedLane):

    def __init__(self, _db, identifiers, lane_name, parent=None):
        if not identifiers:
            raise ValueError(
                "IdentifierGeneratedLane can't be created without Identifiers"
            )
        self.identifiers = identifiers
        full_name = display_name = lane_name
        super(IdentifiersLane, self).__init__(
            _db, full_name, display_name=display_name
        )

    def lane_query_hook(self, qu, work_model=Work):
        if work_model != Work:
            qu = qu.join(LicensePool.identifier)

        qu = Work.from_identifiers(
            self._db, self.identifiers, base_query=qu
        )
        return qu
