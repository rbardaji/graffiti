import datetime

from elasticsearch import Elasticsearch
from functools import wraps
from flask import request, abort

from .db_manager import data_ingestion
from .auth_manager import get_token_info

from config import api_index

def save_request(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        
        data, _ = get_token_info(request)

        body = {
            'user': data.get('user_id', 'anonymus'),
            'email': data.get('email', 'anonymus'),
            'token': request.headers.get('Authorization', 'anonymus'),
            'method': request.method,
            'base_url': request.base_url,
            'query_string': request.query_string.decode("utf-8"),
            'url': request.url,
            'time': datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
        }
        status = data_ingestion(api_index, body)
        if status == 201:
            return f(*args, **kwargs)
        elif status == 500:
            return {
                'status': False,
                'message': 'DB connection error. Please, report to help@emso-eu.org.' 
            }, 500

    return decorated


def token_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):

        token = None

        if 'Authorization' in request.headers:
            token = request.headers['Authorization']

        if not token:
            abort(401, "Authorization Token is missing.")

        # Check if token exists
        _, status_code = get_token_info(request)

        if status_code != 200:
            abort(401, "Invalid token.")
        
        return f(*args, **kwargs)
    
    return decorated