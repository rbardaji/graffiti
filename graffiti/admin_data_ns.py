from flask_restx import Namespace, Resource, reqparse, fields
from flask import request

from .utils.decorator import save_request, admin_token_required
from .utils.db_manager import post_data, delete_data
from .user_ns import user_response


api = Namespace('admin_data', description='Data operations')


data_payload = api.model('data_payload', {
    'platform_code': fields.String(required=True),
    'parameter': fields.String(required=True),
    'depth': fields.Float(required=True),
    'depth_qc': fields.Integer(required=True),
    'time': fields.String(required=True, help="Format: Y-m-d'T'H:M:S'Z'"),
    'time_qc': fields.Integer(required=True),
    'lat': fields.Float(required=True),
    'lat_qc': fields.Integer(required=True),
    'lon': fields.Float(required=True),
    'lon_qc': fields.Integer(required=True),
    'value': fields.Float(required=True),
    'qc': fields.Integer(required=True)})


@api.route('/<string:rule>')
@api.param('rule', 'Options: M, 15D, 10D, 6D, 5D, 4D, 3D, 2D, D, 12H, 8H, 6H, 3H, 2H, H, R')
class PostData(Resource):
    @api.doc(security='apikey')
    @api.marshal_with(user_response, code=201, skip_none=True)
    @api.expect(data_payload)
    @admin_token_required
    @save_request
    def post(self, rule):
        """
        Add data to the database
        """
        return post_data(rule, api.payload)


@api.route('/<string:rule>/<string:id_data>')
@api.param('rule', 'Options: M, 15D, 10D, 6D, 5D, 4D, 3D, 2D, D, 12H, 8H, 6H, 3H, 2H, H, R')
@api.param('id_data', 'The id is <platform_code>_<parameter>_<depth>_<time>')
@api.response(202, 'Deleted.')
@api.response(404, 'Data ID not found.')
@api.response(503, 'Connection error with the DB.')
class DeleteIngestionData(Resource):
    @api.doc(security='apikey')
    @admin_token_required
    @save_request
    def delete(self, rule, id_data):
        """
        Delete an existing data record
        """
        return delete_data(rule, id_data)
