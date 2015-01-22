from core.monitor import Monitor
from core.model import (
    DataSource,
    CirculationEvent,
)

class GutenbergMonitor(Monitor):
    """Maintain license pool and metadata info for Gutenberg titles.

    TODO: This monitor doesn't really use the normal monitor process,
    but since it doesn't access an 'API' in the traditional sense it
    doesn't matter much.
    """

    def __init__(self, _db, data_directory):
        self._db = _db
        path = os.path.join(data_directory, DataSource.GUTENBERG)
        if not os.path.exists(path):
            os.makedirs(path)
        self.source = GutenbergAPI(_db, path)

    def run(self, subset=None):
        added_books = 0
        for edition, license_pool in self.source.create_missing_books(subset):
            # Log a circulation event for this title.
            event = get_one_or_create(
                self._db, CirculationEvent,
                type=CirculationEvent.TITLE_ADD,
                license_pool=license_pool,
                create_method_kwargs=dict(
                    start=license_pool.last_checked
                )
            )

            self._db.commit()

