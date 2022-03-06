from flask_restx import Namespace, Resource
from flask import request
from .utils.db_manager import get_query_id, get_query
from .utils.auth_manager import Auth, get_token

api = Namespace('user', description='User operations')


query_parse = api.parser()
query_parse.add_argument('namespace')


@api.route('/<string:user>/<string:password>')
@api.response(201, 'Token created')
@api.response(404, 'User not found')
class GetToken(Resource):
    """ Authorization operations"""
    @api.doc(security=[])
    def get(self, user, password):
        """
        Get the token
        """
        return get_token(user, password)


@api.route('/query')
@api.response(201, 'Created')
class GetQuery(Resource):

    @api.expect(query_parse)
    def get(self):
        """
        Get previous queries from user
        """
        data, _ = Auth.get_logged_in_user(request)

        result = data.get('result', 'anonymus')
        if isinstance(result, dict):
            user_id = result.get('user_id', 'anonymus')
            email = result.get('email', 'anonymus')
        else:
            user_id = 'anonymus'
            email = 'anonymus'

        namespace = request.args.get('namespace')

        return get_query(user_id, namespace)


@api.route('/query/<string:id_query>')
@api.response(201, 'Found')
@api.response(204, 'No content. Query id not found')
@api.response(503, 'Internal error. Unable to connect to DB')
class GetQueryId(Resource):
    def get(self, id_query):
        """
        Get the content of the query
        """
        return get_query_id(id_query)
