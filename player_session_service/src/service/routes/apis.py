from __future__ import annotations
from typing import Any, List, Dict
import bz2
import datetime
import logging
from itertools import islice
from flask import request
from cassandra.cluster import Cluster
from werkzeug.wrappers import BaseResponse

from service.routes import bp
import service.routes.utils as util

# class
class ApplicationError(Exception):
    pass

# functions
@bp.before_request
def before_request():
    hosts = bp.config['cassandra_hosts'] 
    bp.cluster = Cluster(hosts)
    bp.db = bp.cluster.connect('pss_cassandra')

@bp.teardown_request
def teardown_request(expcetion):
    bp.cluster.shutdown()

## functions
@bp.route("/store_events/<int:event_batch>", methods=['POST'])
def store_events(event_batch:int) -> Any:
    """
    Method to store 1-10 events from a file. It keeps 
    track of the status of the file and continues from where is left after 
    each API call
    Arguments:
        event_batch {int} -- [size of events to be uploaded]
    """
    response: Dict[str, Any] = {}
    insert_query:str = "INSERT INTO %s JSON '%s';"
    # if batch size not between 1 and 10 through an error
    if event_batch < 1 or event_batch > 10:
       raise ApplicationError(f'Batch size {event_batch} is not between 1-10')

    data_path:str =  bp.config['data_path']
    with bz2.open(data_path, 'r') as f:
        seek_from = bp.config.get('seek_from', 0)
        f.seek(seek_from, 0)
        next_n_lines = list(islice(f, event_batch))        
        for line in next_n_lines:
            line_dict = eval(line)
            event_value = line_dict.pop('event', None)
            # TODO: Add rows in db in batch instead of one by one
            insert_stmt = insert_query % (f"event_{event_value}", str(line_dict).replace("'",'"'))
            bp.db.execute(insert_stmt)

        seek_from = f.tell()
        bp.config['seek_from'] = seek_from
    response = {'message': f"{event_batch} sessions added"}
    return util.createResponse(status_code=200, message=response)

@bp.route("/fetch_sessions/<string:player_id>/<int:limit>", methods=['GET'])
def fetch_session(player_id:str, limit:int) -> Any:
    """
    Method to retieve the latest N completed sessions by 
    a player given a player_id and limit
    Arguments:
        player_id {str} -- [Player's id]
        limit {int} -- [The latest N sessions to retrieve]
    """
    # NOTE: Ideally data discard should be an additional microservice
    # that runs once a day and deletes from db data that are older than 1
    # However for the scope of this excerise I will not delete any data
    # or  create any microservie to do this. I will handle this requirement
    # on the application level.
    
    # Verify parameters exist
    if len(player_id) > 32 or not limit or limit < 0:
       raise ApplicationError('Please check your input')

    # Hold response information
    response: Dict[str, Any] = {}
    sessions: List = []
    # Get date 1 year ago
    date_window = bp.config['current_dt'] - datetime.timedelta(days=365)
    # increase the limit in case we have sessions that started a year ago which will be 
    # ignored bellow
    query_limit = limit + 50

    # 1. For a given player id get the completed sessions. Filter on the limit and the time
    # in case db consists only on sessions that were completed a year ago
    completed_events_query = "SELECT session_id,  ts FROM  {}  WHERE player_id='{}' and ts >= '{}' LIMIT {} ALLOW FILTERING;" 
    completed_event_stmt =  completed_events_query.format("event_end", player_id, date_window,query_limit)
    query_results: List = []
    query_results = bp.db.execute(completed_event_stmt)

    # 2. Get the start session given a player_id    
    limit_counter:int = 0
    start_event_results :List = []
    start_event_query = "SELECT country, ts FROM {} WHERE player_id='{}' and ts >= '{}' and session_id={} ALLOW FILTERING;"    
    for row in query_results:
        # If retieved the top N sessions exit the loop
        if limit_counter > limit:
            break

        session = {}
        session['player_id'] = player_id
        session['session_id'] = str(row.session_id)
        session['end_ts'] = str(row.ts)
        start_event_stmt = start_event_query.format("event_start", player_id, date_window, row.session_id)
        start_event_results = bp.db.execute(start_event_stmt)
        if len(start_event_results.current_rows):            
            single_row = start_event_results.one()
            session['country'] = str(single_row.country)
            session['start_ts'] = str(single_row.ts)
            sessions.append(session)
            limit_counter += 1

    response = {'message': 'session retrieved', 'results': sessions}
    return util.createResponse(status_code=200, message=response)

