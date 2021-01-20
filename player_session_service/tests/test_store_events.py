import pytest
import json

def test_store_events(client) -> None:
    response = client.post('/api/v1/store_events/10')
    response_data =  json.loads(response.data)
    assert response.status_code == 200
    assert '10 sessions added' == response_data['message']


def test_store_events_batch_size_more_than_10(client) -> None:
    """
    Scenario: Batch size is more than the limit (10)

    """
    response = client.post('/api/v1/store_events/100')
    response_data =  json.loads(response.data)
    assert response.status_code == 500



def test_store_events_batch_size_not_int(client) -> None:
    """
    Scenario: Batch size is not an integer

    """
    response = client.post('/api/v1/store_events/ten')
    response_data =  json.loads(response.data)
    assert response.status_code == 500
    1
