from __future__ import annotations

from flask import jsonify
from service.routes import bp

@bp.app_errorhandler(500)
def handleUnexpectedError(error) -> Tuple[BaseResponse, int]:
    message: List[Any] = [str(x) for x in error.args]
    status_code: int = 500
    success:bool = False
    response: Dict[str, Any] = {
        'success': success,
        error:{
            'type': error.__class__.__name__,
            "message": message
        }

    }
    return jsonify(response), status_code
