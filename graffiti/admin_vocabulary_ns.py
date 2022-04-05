from flask_restx import Namespace, Resource, fields

from .utils.decorator import save_request, admin_token_required
from .utils.db_manager import post_vocabulary, put_vocabulary, delete_vocabulary

api = Namespace('admin_vocabulary', description='POST, PUT and DELETE vocabulary')


# vocabulary_payload = api.model('vocabulary', {
#     'standard_name': fields.String(),
#     'units': fields.String(),
#     '_FillValue': fields.String(),
#     'coordinates': fields.String(),
#     'long_name': fields.String(),
#     'qc_indicator': fields.String(),
#     'processing_level': fields.Integer(),
#     'valid_min': fields.Float(),
#     'valid_max': fields.Float(),
#     'comment': fields.Integer(),
#     'ancillary_variables': fields.String(),
#     'DM_indicator': fields.String(),
#     'sensor_orientation': fields.String(),
#     'flag_values': fields.List(fields.Integer()),
#     'flag_meanings': fields.List(fields.String())})

vocabulary_payload = api.model('vocabulary', {})


@api.route('/<string:platform_code>')
@api.param('platform_code', 'Platform code (ID of the vocabulary document)')
@api.response(404, 'Not found')
@api.response(406, 'Not Acceptable. Bad vocabulary payload')
@api.response(503, 'Internal error. Unable to connect to DB')
class PostPutDeleteAddVocabulary(Resource):
    @api.doc(security='apikey')
    @api.response(201, 'Added')
    @api.expect(vocabulary_payload)
    @admin_token_required
    @save_request
    def post(self, platform_code):
        """
        Add a vocabulary document
        """
        return post_vocabulary(platform_code, api.payload)

    @api.doc(security='apikey')
    @api.expect(vocabulary_payload)
    @admin_token_required
    @save_request
    def put(self, platform_code):
        """
        Update a vocabulary document
        """
        return put_vocabulary(platform_code, api.payload)

    @api.doc(security='apikey')
    @api.response(202, 'Deleted')
    @admin_token_required
    @save_request
    def delete(self, platform_code):
        """
        Delete a vocabulary document
        """
        return '', delete_vocabulary(platform_code)
