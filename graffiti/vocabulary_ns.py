from flask_restx import Namespace, Resource

from .utils.decorator import save_request, token_required
from .utils.db_manager import get_vocabulary
from .user_ns import user_response

api = Namespace('vocabulary',
                description='Understand the acronyms of the parameters used on platforms')


@api.route('/<string:platform_code>')
@api.param('platform_code', 'Platform code')
@api.response(204, 'Not found.')
@api.response(401, 'Invalid email or password.')
@api.response(503, 'Connection error with the DB.')
class GetVocabulary(Resource):
    @api.doc(security='apikey')
    @api.marshal_with(user_response, code=200, skip_none=True)
    @token_required
    @save_request
    def get(self, platform_code):
        """
        Understand the acronyms of the parameters used on the input platform
        """
        return get_vocabulary(platform_code)
