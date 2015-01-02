from nose.tools import set_trace
import os

from core.model import (
    production_session,
    Edition,
    Identifier,
    LicensePool,
    Work,
    WorkFeed,
)
from core.opds import OPDSFeed

from sqlalchemy.orm.exc import (
    NoResultFound,
)

import flask
from flask import Flask, url_for, redirect, Response

app = Flask(__name__)
app.config['DEBUG'] = True
app.debug = True

from opds import ContentServerAnnotator
from core.opds import AcquisitionFeed
from core.util.flask_util import languages_for_request

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

@app.route('/')
def feed():

    arg = flask.request.args.get
    last_seen_id = arg('after', None)
    size = arg('size', "100")
    try:
        size = int(size)
    except ValueError:
        return problem("Invalid size: %s" % size, 400)
    languages = languages_for_request()

    this_url = url_for('feed', _external=True)

    last_work_seen = None
    last_id = arg('after', None)
    if last_id:
        try:
            last_id = int(last_id)
        except ValueError:
            return problem("Invalid work ID: %s" % last_id, 400)
        try:
            last_work_seen = Conf.db.query(Work).filter(Work.id==last_id).one()
        except NoResultFound:
            return problem("No such work id: %s" % last_id, 400)

    feed = WorkFeed(None, languages, [Work.last_update_time, Work.id], False, WorkFeed.ALL)
    work_q = feed.page_query(Conf.db, last_work_seen, size)
    page = work_q.all()
    opds_feed = AcquisitionFeed(Conf.db, "Open-Access Content", this_url, page,
                                ContentServerAnnotator)
    if page and len(page) >= size:
        after = page[-1].id
        next_url = url_for(
            'feed', after=after, size=str(size), _external=True,)
        opds_feed.add_link(rel="next", href=next_url,
                           type=OPDSFeed.ACQUISITION_FEED_TYPE)

    feed = unicode(opds_feed)
    return feed

if __name__ == '__main__':

    debug = True
    host = "0.0.0.0"
    app.run(debug=debug, host=host)
