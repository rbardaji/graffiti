from flask_restx import Namespace, Resource, reqparse, fields
from flask import request

from .utils.decorator import save_request
from .utils.db_manager import post_data, delete_data

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


@api.route('/data/<string:rule>')
@api.param('rule', 'Options: M, 15D, 10D, 6D, 5D, 4D, 3D, 2D, D, 12H, 8H, 6H, 3H, 2H, H, R')
@api.response(201, 'Added')
class PostData(Resource):
    @api.expect(data_payload)
    @save_request
    def post(self, rule):
        """
        Add data to the database
        """
        return post_data(rule, api.payload)


@api.route('/data/<string:id_data>')
@api.response(202, 'Deleted')
@api.response(204, 'Data ID not found')
@api.response(503, 'Internal error. Unable to connect to DB')
class DeleteIngestionData(Resource):
    @save_request
    def delete(self, id_data):
        """
        Delete an existing data record
        """
        return delete_data(id_data)
