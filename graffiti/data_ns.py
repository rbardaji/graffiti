from flask_restx import Namespace, Resource, reqparse
from flask import request

from .utils.decorator import save_request, token_required
from .utils.db_manager import get_data_count, get_df
from .user_ns import user_response

api = Namespace('data',
                description='Get data and stats related to the data values')


data_parser = reqparse.RequestParser()
data_parser.add_argument('depth_min', type=float,
                         help='Minimum depth of the measurement')
data_parser.add_argument('depth_max', type=float,
                         help='Maximum depth of the measurement')
data_parser.add_argument('time_min',
                         help='Minimum date and time of the measurement')
data_parser.add_argument('time_max',
                         help='Maximum date and time of the measurement')
data_parser.add_argument('qc', type=int,
                         help='Quality Control flag value of the measurement',
                         choices=[0, 1, 2, 3, 4, 5, 6, 7, 8, 9])

data_complete_parser = data_parser.copy()
data_complete_parser.add_argument('platform_code', type=str, help='Platform code')
data_complete_parser.add_argument('parameter', type=str, help='Parameter acronym')


@api.route('/count/<string:rule>')
@api.param('rule', 'Options: M, 15D, 10D, 6D, 5D, 4D, 3D, 2D, D, 12H, 8H, 6H, 3H, 2H, H, R')
@api.response(204, 'Not found.')
@api.response(401, 'Invalid email or password.')
@api.response(503, 'Connection error with the DB.')
class GetDataCount(Resource):
    @api.doc(security='apikey')
    @api.marshal_with(user_response, code=200, skip_none=True)
    @api.expect(data_complete_parser)
    @token_required
    @save_request
    def get(self, rule):
        """
        Get the number of data values
        """
        platform_code = request.args.get("platform_code")
        parameter = request.args.get("parameter")
        depth_min = request.args.get("depth_min")
        depth_max = request.args.get("depth_max")
        time_min = request.args.get("time_min")
        time_max = request.args.get("time_max")
        qc = request.args.get("qc")

        return get_data_count(rule, platform_code, parameter, depth_min,
                              depth_max, time_min, time_max, qc)


@api.route('/<string:rule>/<string:platform_code>/<string:parameter>')
@api.param('rule', 'Options: M, 15D, 10D, 6D, 5D, 4D, 3D, 2D, D, 12H, 8H, 6H, 3H, 2H, H, R')
@api.param('platform_code', 'Platform code')
@api.param('parameter', 'Parameter acronym')
@api.response(204, 'Not found.')
@api.response(401, 'Invalid email or password.')
@api.response(503, 'Connection error with the DB.')
class GetData(Resource):
    @api.doc(security='apikey')
    @api.marshal_with(user_response, code=200, skip_none=True)
    @api.expect(data_parser)
    @token_required
    @save_request
    def get(self, rule, platform_code, parameter):
        """
        Get data
        """
        depth_min = request.args.get("depth_min")
        depth_max = request.args.get("depth_max")
        time_min = request.args.get("time_min")
        time_max = request.args.get("time_max")
        qc = request.args.get("qc")

        df = get_df(platform_code, parameter, rule, depth_min, depth_max,
                    time_min, time_max, qc)
        df_dict = df.to_dict()
        return {
            'status': True,
            'message': f'Data from {platform_code}, parameter {parameter}',
            'result': [df_dict]}
