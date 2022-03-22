from flask_restx import Namespace, Resource, fields
from flask import request

from .utils.db_manager import get_query_id, get_query
from .utils.auth_manager import get_token, get_token_info
from .utils.decorator import token_required

api = Namespace('user',
                description='Get an Authorization Token and check your ' + \
                    'previous API requests')


user_response = api.model('user_response', {
    'status': fields.Boolean(
        description='Indicates if the operation was successful'),
    'message': fields.String(description='Message for the user'),
    'result': fields.List(fields.Raw, description='Content of the response')})


query_parse = api.parser()
query_parse.add_argument('namespace')


@api.route('/token/<string:user>/<string:password>')
@api.response(404, 'Invalid email or password.')
@api.response(503, 'Connection error with the AAI.')
class GetToken(Resource):
    """ Authorization operations"""
    @api.marshal_with(user_response, code=201, skip_none=True)
    def get(self, user, password):
        """
        Get the Authorization Token to use this API.
        """
        return get_token(user, password)


@api.route('/history')
@api.response(401, "Authorization Token is missing or is invalid.")
@api.response(503, "Connection error with the DB.")
class GetHistory(Resource):
    @api.doc(security='apikey')
    @api.expect(query_parse)
    @api.marshal_with(user_response, code=200, skip_none=True)
    @token_required
    def get(self):
        """
        Get a list of the ids {id_query} from the previous API requests.
        """
        token_info, _ = get_token_info(request)

        user_id = token_info['result']['user_id']
        namespace = request.args.get('namespace')

        return get_query(user_id, namespace)


@api.route('/history/<string:id_query>')
@api.response(404, 'id_query not found')
@api.response(503, 'Internal error. Unable to connect to DB')
class GetQueryId(Resource):
    @api.doc(security='apikey')
    @api.marshal_with(user_response, code=200, skip_none=True)
    @token_required
    def get(self, id_query):
        """
        Get the content of the query
        """
        return get_query_id(id_query)
