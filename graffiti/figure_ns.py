from flask_restx import Namespace, Resource, reqparse
from flask import request

from .services.figure_service import (get_line, get_platform_pie, get_area,
                                      get_parameter_availability, get_map,
                                      get_platform_availability, get_scatter,
                                      get_parameter_pie)
from .utils.decorator import save_request, token_required
from .user_ns import user_response

api = Namespace('figure', description='Make figures')

fig_parser = reqparse.RequestParser()
fig_parser.add_argument('depth_min', type=float,
                        help='Minimum depth of the measurement')
fig_parser.add_argument('depth_max', type=float,
                        help='Maximum depth of the measurement')
fig_parser.add_argument('time_min',
                        help='Minimum date and time of the measurement')
fig_parser.add_argument('time_max',
                        help='Maximum date and time of the measurement')
fig_parser.add_argument('qc', type=int,
                        help='Quality Control flag value of the measurement',
                        choices=[0, 1, 2, 3, 4, 5, 6, 7, 8, 9])
fig_parser.add_argument('template', type=str, help='Layout template',
                        choices= ['ggplot2', 'seaborn', 'simple_white',
                                  'plotly', 'plotly_white', 'plotly_dark',
                                  'presentation', 'xgridoff', 'ygridoff',
                                  'gridon'])


platform_parser = fig_parser.copy()
platform_parser.add_argument('platform_code', type=str, help='Platform code',
                             action='split')

parameter_parser = fig_parser.copy()
parameter_parser.add_argument('parameter', type=str, help='Parameter acronym',
                              action='split')

complete_parser = platform_parser.copy()
complete_parser.add_argument('parameter', type=str, help='Parameter acronym',
                             action='split')

advanced_parser = fig_parser.copy()
advanced_parser.add_argument('color', type=str,
                             help='Variable referenced to the color',
                             choices=['depth', 'time'])
advanced_parser.add_argument('marginal_x', type=str,
                             help='Additional chart in the x axis',
                             choices=['histogram', 'rug', 'box', 'violin'])
advanced_parser.add_argument('marginal_y', type=str,
                             help='Additional chart in the x axis',
                             choices=['histogram', 'rug', 'box', 'violin'])
advanced_parser.add_argument('trendline', type=str, help='Make a trendline',
                             choices=['ols', 'lowess', 'expanding', 'rolling'])


@api.route('/area/<string:platform_code>/<string:parameter>')
@api.param('platform_code', 'Platform code (you can write multiple platforms separated by ,)')
@api.param('parameter', 'Parameter acronym (you can write multiple parameters separated by ,)')
@api.response(401, 'Invalid token')
@api.response(404, 'Data not found')
@api.response(503, 'Connection error with the DB')
class GetArea(Resource):
    @api.doc(security='apikey')
    @api.marshal_with(user_response, code=201, skip_none=True)
    @api.expect(fig_parser)
    @token_required
    @save_request
    def get(self, platform_code, parameter):
        """
        Get an area plot
        """
        depth_min = request.args.get("depth_min")
        depth_max = request.args.get("depth_max")
        time_min = request.args.get("time_min")
        time_max = request.args.get("time_max")
        qc = request.args.get("qc")
        template = request.args.get('template')

        platform_code_list = platform_code.split(',')
        parameter_list = parameter.split(',')

        return get_area(platform_code_list, parameter_list, depth_min,
                        depth_max, time_min, time_max, qc, template, False)


@api.route('/line/<string:platform_code>/<string:parameter>')
@api.param('platform_code', 'Platform code (you can write multiple platforms separated by ,)')
@api.param('parameter', 'Parameter acronym (you can write multiple platforms separated by ,)')
@api.response(204, 'Data not found')
@api.response(401, 'Invalid token')
@api.response(503, 'Connection error with the DB')
class GetLine(Resource):
    @api.doc(security='apikey')
    @api.marshal_with(user_response, code=201, skip_none=True)
    @api.expect(fig_parser)
    @token_required
    @save_request
    def get(self, platform_code, parameter):
        """
        Get a time series line plot
        """
        depth_min = request.args.get("depth_min")
        depth_max = request.args.get("depth_max")
        time_min = request.args.get("time_min")
        time_max = request.args.get("time_max")
        qc = request.args.get("qc")
        template = request.args.get('template')

        platform_code_list = platform_code.split(',')
        parameter_list = parameter.split(',')

        return get_line(platform_code_list, parameter_list, depth_min,
                        depth_max, time_min, time_max, qc, template, False)


@api.route('/parameter_availability/<string:parameter>')
@api.param('parameter', 'Parameter acronym')
@api.response(204, 'Data not found')
@api.response(401, 'Invalid token')
@api.response(503, 'Connection error with the DB')
class GetParameterAvailability(Resource):
    @api.doc(security='apikey')
    @api.marshal_with(user_response, code=201, skip_none=True)
    @api.expect(fig_parser)
    @token_required
    @save_request
    def get(self, parameter):
        """
        Get data availability from a parameter
        """
        depth_min = request.args.get("depth_min")
        depth_max = request.args.get("depth_max")
        time_min = request.args.get("time_min")
        time_max = request.args.get("time_max")
        qc = request.args.get("qc")
        template = request.args.get('template')

        return get_parameter_availability(parameter, depth_min, depth_max,
                                          time_min, time_max, qc, template,
                                          multithread = False)


@api.route('/platform_availability/<string:platform_code>')
@api.response(204, 'Data not found')
@api.response(401, 'Invalid token')
@api.response(503, 'Connection error with the DB')
class GetPlatformAvailability(Resource):
    @api.doc(security='apikey')
    @api.marshal_with(user_response, code=201, skip_none=True)
    @api.expect(fig_parser)
    @token_required
    @save_request
    def get(self, platform_code):
        """
        Get data availability from a platform
        """
        depth_min = request.args.get("depth_min")
        depth_max = request.args.get("depth_max")
        time_min = request.args.get("time_min")
        time_max = request.args.get("time_max")
        qc = request.args.get("qc")
        template = request.args.get('template')

        return get_platform_availability(platform_code, depth_min, depth_max,
                                         time_min, time_max, qc, template,
                                         multithread = False)


@api.route('/parameter_pie/<string:rule>')
@api.param('rule', 'Options: M, 15D, 10D, 6D, 5D, 4D, 3D, 2D, D, 12H, 8H, 6H, 3H, 2H, H, R')
@api.response(204, 'Data not found')
@api.response(401, 'Invalid token')
@api.response(503, 'Connection error with the DB')
class GetFigParameterPie(Resource):
    @api.doc(security='apikey')
    @api.marshal_with(user_response, code=201, skip_none=True)
    @api.expect(platform_parser)
    @token_required
    @save_request
    def get(self, rule):
        """
        Obtain a graph with the number of data available for each parameter
        """
        platform_code = request.args.get("platform_code")
        if platform_code:
            platform_code_list = platform_code.split(',')
        else:
            platform_code_list = None
        depth_min = request.args.get("depth_min")
        depth_max = request.args.get("depth_max")
        time_min = request.args.get("time_min")
        time_max = request.args.get("time_max")
        qc = request.args.get("qc")
        template = request.args.get('template')

        return get_parameter_pie(rule, platform_code_list, depth_min, depth_max,
                                 time_min, time_max, qc, template)


@api.route('/platform_pie/<string:rule>')
@api.param('rule', 'Options: M, 15D, 10D, 6D, 5D, 4D, 3D, 2D, D, 12H, 8H, 6H, 3H, 2H, H, R')
@api.response(204, 'Data not found')
@api.response(401, 'Invalid token')
@api.response(503, 'Connection error with the DB')
class GetFigPlatformPie(Resource):
    @api.doc(security='apikey')
    @api.marshal_with(user_response, code=201, skip_none=True)
    @api.expect(parameter_parser)
    @token_required
    @save_request
    def get(self, rule):
        """
        Obtain a graph with the number of data available for each platform
        """
        parameter = request.args.get("parameter")
        if parameter:
            parameter_list = parameter.split(',')
        else:
            parameter_list = None
        depth_min = request.args.get("depth_min")
        depth_max = request.args.get("depth_max")
        time_min = request.args.get("time_min")
        time_max = request.args.get("time_max")
        qc = request.args.get("qc")
        template = request.args.get('template')

        return get_platform_pie(rule, parameter_list, depth_min, depth_max,
                                time_min, time_max, qc, template)


@api.route('/map/<string:rule>')
@api.param(
    'rule',
    'Options: M, 15D, 10D, 6D, 5D, 4D, 3D, 2D, D, 12H, 8H, 6H, 3H, 2H, H, R')
@api.response(204, 'Data not found')
@api.response(401, 'Invalid token')
@api.response(503, 'Connection error with the DB')
class GetFigMap(Resource):
    @api.doc(security='apikey')
    @api.marshal_with(user_response, code=201, skip_none=True)
    @api.expect(complete_parser)
    @token_required
    @save_request
    def get(self, rule):
        """
        Obtain a map with the location of the data sources
        """
        platform_code = request.args.get("platform_code")
        if platform_code:
            platform_code_list = platform_code.split(',')
        else:
            platform_code_list = None
        parameter = request.args.get("parameter")
        if parameter:
            parameter_list = parameter.split(',')
        else:
            parameter_list = None
        depth_min = request.args.get("depth_min")
        depth_max = request.args.get("depth_max")
        time_min = request.args.get("time_min")
        time_max = request.args.get("time_max")
        qc = request.args.get("qc")
        template = request.args.get('template')

        return get_map(rule, platform_code_list, parameter_list, depth_min,
                       depth_max, time_min, time_max, qc, template)


@api.route('/scatter/<string:platform_code_x>/<string:parameter_x>/<string:platform_code_y>/<string:parameter_y>')
@api.response(204, 'Data not found')
@api.response(401, 'Invalid token')
@api.response(503, 'Connection error with the DB')
class GetScatter(Resource):
    @api.doc(security='apikey')
    @api.marshal_with(user_response, code=201, skip_none=True)
    @api.expect(advanced_parser)
    @token_required
    @save_request
    def get(self, platform_code_x, parameter_x, platform_code_y, parameter_y):
        """
        Obtain a time series line plot
        """
        color = request.args.get('color', 'depth')
        marginal_x = request.args.get('marginal_x')
        marginal_y = request.args.get('marginal_y')
        trendline = request.args.get('trendline')
        template = request.args.get('template')
        depth_min = request.args.get("depth_min")
        depth_max = request.args.get("depth_max")
        time_min = request.args.get("time_min")
        time_max = request.args.get("time_max")
        qc = request.args.get("qc")

        return get_scatter(platform_code_x, parameter_x, platform_code_y,
                           parameter_y, color, marginal_x, marginal_y,
                           trendline, template, depth_min, depth_max, time_min,
                           time_max, qc, False)
