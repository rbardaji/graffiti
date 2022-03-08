import datetime

from functools import wraps
from flask import request, abort

from .db_manager import data_ingestion
from .auth_manager import get_token_info

from config import (api_index, data_portal_token, data_portal_user,
                    data_portal_email, test_token, test_user, test_email)


def save_request(f):
    @wraps(f)
    def decorated(*args, **kwargs):

        token = None

        if 'Authorization' in request.headers:
            token = request.headers['Authorization']

        if not token:
            abort(401, "Authorization Token is missing.")
        elif token == data_portal_token:
            body = {
                'user': data_portal_user,
                'email': data_portal_email,
                'token': data_portal_token,
                'method': request.method,
                'base_url': request.base_url,
                'query_string': request.query_string.decode("utf-8"),
                'url': request.url,
                'time': datetime.datetime.utcnow().strftime(
                    '%Y-%m-%d %H:%M:%S.%f')[:-3]
            }
        elif token == test_token:
            body = {
                'user': test_user,
                'email': test_email,
                'token': test_token,
                'method': request.method,
                'base_url': request.base_url,
                'query_string': request.query_string.decode("utf-8"),
                'url': request.url,
                'time': datetime.datetime.utcnow().strftime(
                    '%Y-%m-%d %H:%M:%S.%f')[:-3]
            }
        else:
            data, _ = get_token_info(request)

            body = {
                'user': data.get('user_id', 'anonymus'),
                'email': data.get('email', 'anonymus'),
                'token': request.headers.get('Authorization', 'anonymus'),
                'method': request.method,
                'base_url': request.base_url,
                'query_string': request.query_string.decode("utf-8"),
                'url': request.url,
                'time': datetime.datetime.utcnow().strftime(
                    '%Y-%m-%d %H:%M:%S.%f')[:-3]
            }
        status = data_ingestion(api_index, body)
        if status == 201:
            return f(*args, **kwargs)
        elif status == 500:
            abort(503, 'Connection error with the DB.')

    return decorated


def token_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):

        token = None

        if 'Authorization' in request.headers:
            token = request.headers['Authorization']

        if not token:
            abort(401, "Authorization Token is missing.")
        elif token == data_portal_token:
            status_code = 200
        elif token == test_token:
            status_code = 200
        else:
            # Check if token exists
            _, status_code = get_token_info(request)

        if status_code != 200:
            abort(401, "Invalid token.")
        
        return f(*args, **kwargs)
    
    return decorated
