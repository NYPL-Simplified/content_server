from nose.tools import set_trace
import os
import urlparse
from functools import wraps

from core.util.problem_detail import ProblemDetail
from core.util.flask_util import problem
from core.config import Configuration
from controller import ContentServer

import flask
from flask import Flask, url_for, redirect, Response

from opds import ContentServerAnnotator
from core.opds import AcquisitionFeed
from core.util.flask_util import languages_for_request
from core.app_server import (
    URNLookupController,
)

app = Flask(__name__)
debug = Configuration.logging_policy().get("level") == 'DEBUG'
app.config['DEBUG'] = debug
app.debug = debug

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

@app.route('/')
@returns_problem_detail
def feed():
    return app.content_server.opds_feeds.feed()

@app.route('/lookup')
def lookup():
    return URNLookupController(app.content_server._db).work_lookup(ContentServerAnnotator)

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
    app.content_server.log.info("Starting app on %s:%s", host, port)
    app.run(debug=debug, host=host, port=port)
