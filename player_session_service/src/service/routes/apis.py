from __future__ import annotations
import os
import bz2
import logging
from itertools import islice
from flask import request
from cassandra.cluster import Cluster

from service.routes import bp
import service.routes.utils as util
# class
class ApplicationError(Exception):
    pass
@bp.before_request
def before_request():
    cassandra_hots = ['127.17.0.2']
    bp.cluster = Cluster()
    bp.db = bp.cluster.connect('pss_cassandra')

@bp.teardown_request
def teardown_request(expcetion):
    bp.cluster.shutdown()

## functions
@bp.route("/store_events/<int:event_batch>", methods=['POST'])
def store_events(event_batch:int) -> None:
    response: Dict[str, Any] = {}
    insert_query = "INSERT INTO %s JSON '%s';"
    # if batch size not between 1 and 10 through an error
    if event_batch < 1 or event_batch > 10:
       raise ApplicationError(f'Batch size {event_batch} is not between 1-10')
    curr_dir = os.path.dirname(os.path.abspath(__file__))
    data_path = f"{curr_dir}/../../../data/assignment_data.jsonl.bz2"
    with bz2.open(data_path, 'r') as f:
        seek_from = bp.config.get('seek_from', 0)
        f.seek(seek_from, 0)
        next_n_lines = list(islice(f, event_batch))
        for line in next_n_lines:
            line_dict = eval(line)
            event_value = line_dict.pop('event', None)
            insert_stmt = insert_query % (f"event_{event_value}", str(line_dict).replace("'",'"'))
            bp.db.execute(insert_stmt)


        seek_from = f.tell()
        bp.config['seek_from'] = seek_from
    response = {'message': 'session events added'}
    return util.createResponse(status_code=200, message=response)

@bp.route("/fetch_sessions/<str:player_id>", methods=['GET'])
def fetch_session(player_id:str) -> None:
    response: Dict[str, Any] = {}
    select_query = "SELECT INTO %s JSON '%s';"
    # if batch size not between 1 and 10 through an error
    if event_batch < 1 or event_batch > 10:
       raise ApplicationError(f'Batch size {event_batch} is not between 1-10')
    curr_dir = os.path.dirname(os.path.abspath(__file__))
    data_path = f"{curr_dir}/../../../data/assignment_data.jsonl.bz2"
    with bz2.open(data_path, 'r') as f:
        seek_from = bp.config.get('seek_from', 0)
        f.seek(seek_from, 0)
        next_n_lines = list(islice(f, event_batch))
        for line in next_n_lines:
            line_dict = eval(line)
            event_value = line_dict.pop('event', None)
            insert_stmt = insert_query % (f"event_{event_value}", str(line_dict).replace("'",'"'))
            bp.db.execute(insert_stmt)


        seek_from = f.tell()
        bp.config['seek_from'] = seek_from
    response = {'message': 'session events added'}
    return util.createResponse(status_code=200, message=response)

