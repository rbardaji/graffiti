from flask_restx import Namespace, Resource

from .services.admin_figure_service import (delete_scatter, delete_platform_pie,
                                            delete_parameter_availability,
                                            delete_platform_availability,
                                            delete_map, delete_parameter_pie,
                                            delete_line, delete_area)
from .utils.decorator import save_request


api = Namespace('admin_figure', description='Delete figures')

@api.route('/scatter/<string:platform_code>')
@api.param('platform_code', 'Platform code')
@api.response(204, 'Deleted')
class DelScatter(Resource):
    @save_request
    def delete(self, platform_code):
        """
        Delete a scatter plot
        """
        return delete_scatter(platform_code)


@api.route('/platform_pie')
@api.response(204, 'Deleted')
class DelFigPlatformPie(Resource):
    @save_request
    def delete(self):
        """
        Delete all platform pies
        """
        return delete_platform_pie()


@api.route('/parameter_availability/<string:parameter>')
@api.param('parameter', 'Parameter acronym')
@api.response(204, 'Deleted')
class DelParameterAvailability(Resource):    
    @save_request
    def delete(self, parameter):
        """
        Delete all plots of the parameter availability
        """
        return delete_parameter_availability(parameter)


@api.route('/platform_availability/<string:platform_code>')
@api.param('platform_code', 'Platform code')
@api.response(204, 'Deleted')
class DelPlatformAvailability(Resource):
    @save_request
    def delete(self, platform_code):
        """
        Delete all plots of availability from a platform
        """
        return delete_platform_availability(platform_code)


@api.route('/map')
@api.response(204, 'Deleted')
class DelFigMap(Resource):
    @save_request
    def delete(self):
        """
        Delete all existing maps.
        """
        return delete_map()


@api.route('/parameter_pie')
@api.response(204, 'Deleted')
class DelFigPlatformPie(Resource):
    @save_request
    def delete(self):
        """
        Delete all parameter pie plots
        """
        return delete_parameter_pie()


@api.route('/area/<string:platform_code>/<string:parameter>')
@api.param('platform_code', 'Platform code')
@api.param('parameter', 'Parameter acronym')
@api.response(204, 'Deleted')
class DelDeleteArea(Resource):
    @save_request
    def delete(self, platform_code, parameter):
        """
        Delete an area plot
        """
        return delete_area(platform_code, parameter)


@api.route('/line/<string:platform_code>/<string:parameter>')
@api.param('platform_code', 'Platform code')
@api.param('parameter', 'Parameter acronym')
@api.response(204, 'Deleted')
class GetDeleteLine(Resource):
    @save_request
    def delete(self, platform_code, parameter):
        """
        Delete a time series line plot
        """
        return delete_line(platform_code, parameter)
