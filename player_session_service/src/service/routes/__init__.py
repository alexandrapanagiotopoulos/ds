import os
from flask import Blueprint
API_DEFAULT_PREFIX = '/api/v1'
# define blueprints
bp = Blueprint('api', __name__,
               url_prefix=os.environ.get('API_URL_PREFIX', API_DEFAULT_PREFIX)
               )
bp.config = {}
import service.routes.apis