# imports
import os
import logging
from flask import Flask
from cassandra.cluster import Cluster
from datetime import datetime

from service.routes import bp
def createApplication() -> Flask:
    """
    Returns:
        flask app -- the  flask application
    """
    app:Flask = Flask(__name__)
    app.register_blueprint(bp)
    # retrieve gunicorn logger
    gunicorn_logger: logging.Logger = logging.getLogger('gunicorn.error')
    if 'API_LOG_LEVE' in os.environ:
        try:
            log_level = getattr(logging, os.environ['API_LOG_LEVEL'].upper())
            gunicorn_logger.setLevel(log_level)
        except AttributeError:
            pass
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)
    app.logger = gunicorn_logger
    # For the purpose of this exercise we need to hard code the current time
    # because the data we have are from 2016
    current_datetime:datetime = datetime(2016, 12, 1, 0)
    bp.config['current_dt'] = current_datetime
    # Pass cassandra hostnames
    curr_dir:str = os.path.dirname(os.path.abspath(__file__))
    data_path:str = f"{curr_dir}/../data/assignment_data.jsonl.bz2"
    bp.config['data_path'] = data_path

    return app

# begin
if __name__ == "__main__":
    app:Flask = createApplication()
    app.run()


