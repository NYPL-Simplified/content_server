from nose.tools import set_trace
import os
import urlparse
from functools import wraps

from core.util.problem_detail import ProblemDetail
from core.util.flask_util import problem
from config import Configuration
from controller import ContentServer

import flask
from flask import Flask, url_for, redirect, Response
from flask.ext.babel import Babel

from opds import ContentServerAnnotator
from core.opds import AcquisitionFeed
from core.util.flask_util import languages_for_request
from core.app_server import (
    URNLookupController,
    HeartbeatController,
)

app = Flask(__name__)
debug = Configuration.logging_policy().get("level") == 'DEBUG'
app.config['DEBUG'] = debug
app.debug = debug
babel = Babel(app)

if os.environ.get('AUTOINITIALIZE') == 'False':
    pass
    # It's the responsibility of the importing code to set app.content_server
    # appropriately.
else:
    if getattr(app, 'content_server', None) is None:
        app.content_server = ContentServer()

def returns_problem_detail(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        v = f(*args, **kwargs)
        if isinstance(v, ProblemDetail):
            return v.response
        return v
    return decorated

@app.teardown_request
def shutdown_session(exception):
    if (hasattr(app, 'content_server',)
        and hasattr(app.content_server, '_db')
        and app.content_server._db
    ):
        if exception:
            app.content_server._db.rollback()
        else:
            app.content_server._db.commit()

@app.route('/')
@returns_problem_detail
def feed():
    return app.content_server.opds_feeds.feed()

@app.route('/works/sources/<license_source_name>')
@returns_problem_detail
def feed_from_license_source(license_source_name):
    return app.content_server.opds_feeds.feed(license_source_name)

@app.route('/works/lists/<list_identifier>')
@returns_problem_detail
def feed_from_custom_list(list_identifier):
    return app.content_server.opds_feeds.custom_list_feed(list_identifier)

@app.route('/lookup')
def lookup():
    return URNLookupController(app.content_server._db).work_lookup(ContentServerAnnotator)

# Controllers used for operations purposes
@app.route('/heartbeat')
@returns_problem_detail
def hearbeat():
    return HeartbeatController().heartbeat()

if __name__ == '__main__':
    debug = True
    url = Configuration.integration_url(
        Configuration.CONTENT_SERVER_INTEGRATION, required=True)
    scheme, netloc, path, parameters, query, fragment = urlparse.urlparse(url)
    if ':' in netloc:
        host, port = netloc.split(':')
        port = int(port)
    else:
        host = netloc
        port = 80

    # Workaround for a "Resource temporarily unavailable" error when
    # running in debug mode with the global socket timeout set by isbnlib
    if debug:
        import socket
        socket.setdefaulttimeout(None)

    app.content_server.log.info("Starting app on %s:%s", host, port)
    app.run(debug=debug, host=host, port=port)
