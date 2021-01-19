from __future__ import annotations
import json
from typing import Dict, Any
from werkzeug.wrappers import BaseResponse

# functions
def createResponse(status_code:int, message: Dict[str, Any]) -> BaseResponse:
    """
    Create a standarized response from a message
    Arguments:
        status_code {int} -- [description]
        message {str} -- [description]
    Returns:
        [flask response]
    """
    response = BaseResponse(response=json.dumps(message, ensure_ascii=False),
                            status=status_code,
                            headers={'Contet-Type': 'application/json ; charset=utf-8'})
    return response
