from nose.tools import set_trace
import os
import urlparse

from core.util.flask_util import problem
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
from core.app_server import (
    feed_response,
    URNLookupController,
)

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
    last_update_datetime = arg('before', None)
    size = arg('size', "100")
    try:
        size = int(size)
    except ValueError:
        return problem("Invalid size: %s" % size, 400)
    languages = languages_for_request()

    this_url = url_for('feed', _external=True)

    last_work_seen = None

    feed = WorkFeed(languages, [Work.last_update_time, Work.id], False, WorkFeed.ALL)
    extra_filter = None
    if last_update_datetime:
        extra_filter = Work.last_update_time < last_update_datetime
    work_q = feed.page_query(Conf.db, None, size, extra_filter)
    page = work_q.all()
    opds_feed = AcquisitionFeed(Conf.db, "Open-Access Content", this_url, page,
                                ContentServerAnnotator)
    if page and len(page) >= size:
        before = page[-1].id
        next_url = url_for(
            'feed', before=page[-1].last_update_time, size=str(size), _external=True,)
        opds_feed.add_link(rel="next", href=next_url,
                           type=OPDSFeed.ACQUISITION_FEED_TYPE)

    return feed_response(opds_feed)

@app.route('/lookup')
def lookup():
    return URNLookupController(Conf.db).work_lookup(ContentServerAnnotator)

if __name__ == '__main__':
    debug = True
    url = os.environ['CONTENT_WEB_APP_URL']
    scheme, netloc, path, parameters, query, fragment = urlparse.urlparse(url)
    if ':' in netloc:
        host, port = netloc.split(':')
        port = int(port)
    else:
        host = netloc
        port = 80
    app.run(debug=debug, host=host, port=port)
