from email.policy import default
from secrets import choice
from flask_restx import Namespace, Resource, reqparse
from flask import request

from .utils.decorator import save_request, token_required
from .utils.db_manager import get_metadata_id, get_metadata
from .user_ns import user_response

api = Namespace('metadata', description='Get the metadata information')


metadata_parser = reqparse.RequestParser()
metadata_parser.add_argument('depth_min', type=float,
                             help='Minimum depth of the measurement')
metadata_parser.add_argument('depth_max', type=float,
                             help='Maximum depth of the measurement')
metadata_parser.add_argument('time_min',
                             help='Minimum date and time of the measurement')
metadata_parser.add_argument('time_max',
                             help='Maximum date and time of the measurement')
metadata_parser.add_argument('qc', type=int,
                             help='Quality Control flag value of the measurement',
                             choices=[0, 1, 2, 3, 4, 5, 6, 7, 8, 9])
metadata_parser.add_argument('platform_code', type=str, help='Platform code')
metadata_parser.add_argument('parameter', type=str, help='Parameter acronym')


format_parser = reqparse.RequestParser()
format_parser.add_argument('format', help='Output format',
                           choices=['json', 'csv'], default='json')


@api.route('')
@api.response(204, 'Not found.')
@api.response(401, 'Invalid email or password.')
@api.response(503, 'Connection error with the DB')
class GetMetadata(Resource):
    @api.doc(security='apikey')
    @api.marshal_with(user_response, code=200, skip_none=True)
    @api.expect(metadata_parser)
    @token_required
    @save_request
    def get(self):
        """
        Get metadata IDs {platform_code}
        """
        platform_code = request.args.get("platform_code")
        parameter = request.args.get("parameter")
        depth_min = request.args.get("depth_min")
        depth_max = request.args.get("depth_max")
        time_min = request.args.get("time_min")
        time_max = request.args.get("time_max")
        qc = request.args.get("qc")

        return get_metadata(platform_code, parameter, depth_min, depth_max,
                            time_min, time_max, qc)


@api.route('/<string:platform_code>')
@api.param('platform_code', 'Platform code')
@api.response(204, 'Not found.')
@api.response(401, 'Invalid email or password.')
@api.response(503, 'Connection error with the DB.')
class GetMetadataId(Resource):
    @api.doc(security='apikey')
    @api.expect(format_parser)
    @api.marshal_with(user_response, code=200, skip_none=True)
    @token_required
    @save_request
    def get(self, platform_code):
        """
        Get the metadata from the input {platform_code}
        """
        format = request.args.get('format', 'json')
        return get_metadata_id(platform_code=platform_code, format=format)
