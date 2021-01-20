# Imports
import os
import pytest
import logging
from datetime import datetime
from flask import Flask
from service.routes import bp
# functions
@pytest.fixture(scope='session')
def app(request):
    """
    Create a Flask app context for the tests
    """
    # Initialize application
    app = Flask(__name__)
    app.register_blueprint(bp)

    # Set up logger
    custom_logger = logging.getLogger(__name__)
    app.logger.handlers = custom_logger.handlers
    app.logger.setLevel(custom_logger.level)

    current_datetime:datetime = datetime(2016, 12, 1, 0)
    bp.config['current_dt'] = current_datetime
    bp.config['cassandra_hosts']  = ['127.0.0.1']

    curr_dir:str = os.path.dirname(os.path.abspath(__file__))
    data_path:str = f"{curr_dir}/../data/assignment_data.jsonl.bz2"
    logging.error(data_path)

    bp.config['data_path'] = data_path
    ctx = app.app_context()
    ctx.push()
    def teardown():
        ctx.pop()
    request.addfinalizer(teardown)
    return app
        




