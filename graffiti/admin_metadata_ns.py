from flask_restx import Namespace, Resource, fields

from .utils.decorator import save_request
from .utils.db_manager import post_metadata, put_metadata, delete_metadata

api = Namespace('admin_metadata', description='Metadata operations')


metadata_payload = api.model('metadata', {
    'platform_code': fields.String(),
    'platform_name': fields.String(),
    'institution': fields.String(),
    'date_update': fields.String(),
    'wmo_platform_code': fields.String(),
    'institution_edmo_code': fields.Integer(),
    'source': fields.String(),
    'source_platform_category_code': fields.Integer(),
    'source_platform_category_code': fields.Integer(),
    'history': fields.String(),
    'data_mode': fields.String(),
    'comment': fields.String(),
    'summary': fields.String(),
    'id': fields.String(),
    'area': fields.String(),
    'geospatial_lat_min': fields.Float(),
    'geospatial_lat_max': fields.Float(),
    'geospatial_lon_min': fields.Float(),
    'geospatial_lon_max': fields.Float(),
    'geospatial_vertical_min': fields.Float(),
    'geospatial_vertical_max': fields.Float(),
    'time_coverage_start': fields.String(),
    'time_coverage_end': fields.String(),
    'pi_name': fields.String(),
    'bottom_depth': fields.Float(),
    'wmo_inst_type': fields.String(),
    'ices_platform_code': fields.String(),
    'doi': fields.String(),
    'naming_authority': fields.String(),
    'format_version': fields.String(),
    'title': fields.String(),
    'data_type': fields.String(),
    'references': fields.String(),
    'data_assembly_center': fields.String(),
    'cdm_data_type': fields.String(),
    'citation': fields.String(),
    'Conventions': fields.String(),
    'qc_manual': fields.String(),
    'netcdf_version': fields.String(),
    'update_interval': fields.String(),
    'distribution_statement': fields.String(),
    'last_latitude_observation': fields.Float(),
    'last_longitude_observation': fields.Float(),
    'last_date_observation': fields.Float(),
    'site_code': fields.Float(),
    'contact': fields.Float(),
    'institution_references': fields.Float(),
    'network': fields.Float(),
    'parameters': fields.List(fields.String())})


@api.route('/metadata/<string:platform_code>')
@api.param('platform_code', 'Platform code (ID of the metadata document)')
@api.response(201, 'Added')
@api.response(202, 'Deleted')
@api.response(204,
    'No content. Platform code not found or metadata not updated')
@api.response(406, 'Not Acceptable. Bad metadata payload')
@api.response(503, 'Internal error. Unable to connect to DB')
class PostPutDeleteAddMetadata(Resource):
    @api.expect(metadata_payload)
    @save_request
    def post(self, platform_code):
        """
        Add a metadata document
        """
        return post_metadata(platform_code, api.payload)

    @api.expect(metadata_payload)
    @save_request
    def put(self, platform_code):
        """
        Update a metadata document
        """
        return put_metadata(platform_code, api.payload)

    @save_request
    def delete(self, platform_code):
        """
        Delete a metadata document
        """
        return delete_metadata(platform_code)
