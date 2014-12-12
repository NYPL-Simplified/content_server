import os

from core.model import (
    production_session,
    Edition,
    Identifier,
    LicensePool,
    Work,
)

from flask import Flask, url_for, redirect, Response

app = Flask(__name__)
app.config['DEBUG'] = True
app.debug = True

from opds import ContentServerAnnotator
from core.opds import AcquisitionFeed

class Conf:
    db = None

    @classmethod
    def initialize(cls, _db):
        cls.db = _db

if os.environ.get('TESTING') == "True":
    Conf.testing = True
else:
    Conf.testing = False
    _db = production_session()
    Conf.initialize(_db)

@app.route('/feed/')
def feed():
    works = Conf.db.query(Work).join(Work.primary_edition).join(Edition.primary_identifier).join(Identifier.licensed_through).order_by(LicensePool.availability_time.desc())
    this_url = url_for('feed', _external=True)
    opds_feed = AcquisitionFeed(Conf.db, "blah", this_url, works,
                                ContentServerAnnotator)

    feed = unicode(opds_feed)
    return feed

if __name__ == '__main__':

    debug = True
    host = "0.0.0.0"
    app.run(debug=debug, host=host)
