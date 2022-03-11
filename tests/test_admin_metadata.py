import unittest

from run import app
from config import test_token

class ResourceTest(unittest.TestCase):

    def setUp(self):
        self.app = app.test_client()

    def test_post_metadata_201(self):
        """
        POST /admin_metadata/{platform_code} - platform_code: test_platform
        The status_code of the response should be 201.
        """
        query = '/admin_metadata/test_platform'
        payload = {
            'platform_code': 'test_platform',
            'platform_name': 'test_platform',
            'institution': 'test_institution',
            'date_update': '2022-03-11T18:14:00Z',
            'wmo_platform_code': 'test_wmo',
            'institution_edmo_code': 0,
            'source': 'test_source',
            'source_platform_category_code': 0,
            'history': 'test_history',
            'data_mode': 'test_data_mode',
            'comment': 'test_comment',
            'summary': 'test_summary',
            'id': 'test_platform',
            'area': 'test_area',
            'geospatial_lat_min': 0,
            'geospatial_lat_max': 0,
            'geospatial_lon_min': 0,
            'geospatial_lon_max': 0,
            'geospatial_vertical_min': 10,
            'geospatial_vertical_max': 10,
            'time_coverage_start': '2022-03-11T18:14:00Z',
            'time_coverage_end': '2022-03-11T18:14:00Z',
            'pi_name': 'test_name',
            'bottom_depth': 10,
            'wmo_inst_type': 'test_wmo',
            'ices_platform_code': 'test_ices',
            'doi': 'test_doi',
            'naming_authority': 'test_authority',
            'format_version': 'test_version',
            'title': 'test_title',
            'data_type': 'test_type',
            'references': 'test_reference',
            'data_assembly_center': 'test_center',
            'cdm_data_type': 'test_type',
            'citation': 'test_citation',
            'Conventions': 'test_convention',
            'qc_manual': 'test_manual',
            'netcdf_version': 'test_version',
            'update_interval': 'test_interval',
            'distribution_statement': 'test_statement',
            'last_latitude_observation': 0,
            'last_longitude_observation': 0,
            'last_date_observation': 0,
            'site_code': 0,
            'contact': 'test_contact',
            'institution_references': 'test_reference',
            'network': 'test_network',
            'parameters': ['test_parameter']}

        response = self.app.post(query, json=payload, 
                                 headers={'Authorization': test_token})
        self.assertEqual(201, response.status_code)

    def test_delete_metadata_202(self):
        """
        First,
        DELETE /admin_metadata/{platform_code} - platform_code: test_platform2
        Then,
        DELETE /admin_metadata/{platform_code} - platform_code: test_platform2
        The status_code of the response should be 202.
        """
        query = '/admin_metadata/test_platform'
        payload = {
            'platform_code': 'test_platform2',
            'platform_name': 'test_platform2',
            'institution': 'test_institution2',
            'date_update': '2022-03-11T18:42:00Z',
            'wmo_platform_code': 'test_wmo2',
            'institution_edmo_code': 0,
            'source': 'test_source',
            'source_platform_category_code': 0,
            'history': 'test_history',
            'data_mode': 'test_data_mode',
            'comment': 'test_comment',
            'summary': 'test_summary',
            'id': 'test_platform',
            'area': 'test_area',
            'geospatial_lat_min': 0,
            'geospatial_lat_max': 0,
            'geospatial_lon_min': 0,
            'geospatial_lon_max': 0,
            'geospatial_vertical_min': 10,
            'geospatial_vertical_max': 10,
            'time_coverage_start': '2022-03-11T18:14:00Z',
            'time_coverage_end': '2022-03-11T18:14:00Z',
            'pi_name': 'test_name',
            'bottom_depth': 10,
            'wmo_inst_type': 'test_wmo',
            'ices_platform_code': 'test_ices',
            'doi': 'test_doi',
            'naming_authority': 'test_authority',
            'format_version': 'test_version',
            'title': 'test_title',
            'data_type': 'test_type',
            'references': 'test_reference',
            'data_assembly_center': 'test_center',
            'cdm_data_type': 'test_type',
            'citation': 'test_citation',
            'Conventions': 'test_convention',
            'qc_manual': 'test_manual',
            'netcdf_version': 'test_version',
            'update_interval': 'test_interval',
            'distribution_statement': 'test_statement',
            'last_latitude_observation': 0,
            'last_longitude_observation': 0,
            'last_date_observation': 0,
            'site_code': 0,
            'contact': 'test_contact',
            'institution_references': 'test_reference',
            'network': 'test_network',
            'parameters': ['test_parameter']}

        self.app.post(query, json=payload,
                      headers={'Authorization': test_token})

        response = self.app.delete(query, headers={'Authorization': test_token})

        self.assertEqual(202, response.status_code)

    def test_delete_data_404(self):
        """
        DELETE /admin_data/{rule}/{id} - rule: R,
            id: not_found_id
        The status_code of the response should be 204.
        """
        query = '/admin_metadata/not_found_id'
        response = self.app.delete(query, headers={'Authorization': test_token})

        self.assertEqual(404, response.status_code)

    def tearDown(self):
        """
        Delete all generated data
        """
        query = '/admin_metadata/test_platform'
        self.app.delete(query, headers={'Authorization': test_token})
