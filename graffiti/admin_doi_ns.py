from flask_restx import Namespace, Resource, fields

from .utils.decorator import save_request, token_required
from .utils.db_manager import put_doi
from .user_ns import user_response


api = Namespace('admin_doi',
                description='Update the number of available DOIs per user')


number_payload = api.model('number_payload', {
    'num': fields.Integer(Required=True)})


@api.route('/<string:user>/<string:number_of_dois>')
@api.response(201, 'Updated')
@api.response(401, 'Provide a valid Token')
@api.response(403, 'Not available DOIs')
@api.response(503, 'Error connection with the DB')
class GetPostDOI(Resource):
    @api.doc(security='apikey')
    @api.marshal_with(user_response, code=200, skip_none=True)
    @token_required
    @save_request
    def put(self, user, number_of_dois):
        """
        Update the number of available DOIs per user.
        """
        return put_doi(user, number_of_dois)
