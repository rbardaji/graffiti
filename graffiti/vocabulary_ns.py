from flask_restx import Namespace, Resource

from .utils.decorator import save_request
from .utils.db_manager import get_vocabulary

api = Namespace('vocabulary',
                description='Understand the acronyms of the parameters used on platforms')


@api.route('/<string:platform_code>')
@api.param('platform_code', 'Platform code')
@api.response(200, 'Found')
@api.response(204, 'Vocabulary found')
@api.response(503, 'Internal error. Unable to connect to the DB')
class GetVocabulary(Resource):
    @save_request
    def get(self, platform_code):
        """
        Understand the acronyms of the parameters used on the input platform
        """
        return get_vocabulary(platform_code)
