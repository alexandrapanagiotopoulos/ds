import pytest
import json
def test_fetch_20_events(client) -> None:
    response = client.get('/api/v1/fetch_sessions/0a2d12a1a7e145de8bae44c0c6e06629/20')
    response_data =  json.loads(response.data)
    assert response.status_code == 200
    assert 2 == len(response_data['results'])


def test_fetch_more_events_player_id_does_not_exsit(client) -> None:
    """
    Scenario: player_id does not exist in db
    """
    response = client.get('/api/v1/fetch_sessions/0a2d12a1a7e145de8bae44c9/20')
    response_data =  json.loads(response.data)
    assert response.status_code == 200
    assert 0 == len(response_data['results'])

def test_fetch_events_player_id_not_valid(client) -> None:
    """
    Scenario: Request has a player_id more than 32 characters
    """
    response = client.get('/api/v1/fetch_sessions/0a2d12a1a7e145de8bae44c0c6e06629dddddddddd/20')
    response_data =  json.loads(response.data)
    assert response.status_code == 500

def test_fetch_events_limit_not_valid(client) -> None:
    """
    Scenario: Try limit is not a valid
    """
    response = client.get('/api/v1/fetch_sessions/0a2d12a1a7e145de8bae44c0c6e06629/-1')
    response_data =  json.loads(response.data)
    assert response.status_code == 500
