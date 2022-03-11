import unittest

from run import app
from config import test_token

class ResourceTest(unittest.TestCase):

    def setUp(self):
        self.app = app.test_client()
        # Add metadata to the DB
        query = '/admin_metadata/test_platform44'
        payload = {
            'platform_code': 'test_platform44',
            'platform_name': 'test_platform44',
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

        self.app.post(query, json=payload,
                      headers={'Authorization': test_token})
        # Add some data to the DB
        query = '/admin_data/M'
        for i in range(3):
            payload = {
                'platform_code': 'test_platform44',
                'parameter': 'test_parameter',
                'depth': 10,
                'depth_qc': 1,
                'time': f'2200-03-09T21:4{i}:00Z',
                "time_qc": 1,
                "lat": 20,
                "lat_qc": 1,
                "lon": 20,
                "lon_qc": 1,
                "value": i,
                "qc": 1
            }
            self.app.post(query, json=payload,
                        headers={'Authorization': test_token})

    def test_get_map_201(self):
        """
        GET figure/map/R should return a status_code = 201
        """
        query = 'figure/map/M'
        response = self.app.get(query, headers={'Authorization': test_token})
        self.assertEqual(201, response.status_code)

    def test_get_map_201_depth_min(self):
        """
        GET figure/map?depth_min=0 should return a status_code = 201
        """
        query = 'figure/map?depth_min=0'
        response = self.app.get(query, headers={'Authorization': test_token})
        self.assertEqual(201, response.status_code)

    # def test_get_platform_availability_201_depth_max(self):
    #     """
    #     GET figure/platform_availability/test_platform44?depth_max=20 should
    #     return a status_code = 201
    #     """
    #     query = 'figure/platform_availability/test_platform44?depth_max=20'
    #     response = self.app.get(query, headers={'Authorization': test_token})
    #     self.assertEqual(201, response.status_code)

    # def test_get_platform_availability_201_time_min(self):
    #     """
    #     GET
    #     figure/platform_availability/test_platform44?time_min=2000-01-01T00:00:00Z
    #     should return a status_code = 201
    #     """
    #     query = 'figure/platform_availability/test_platform44?time_min' + \
    #         '=2000-01-01T00:00:00Z'
    #     response = self.app.get(query, headers={'Authorization': test_token})
    #     self.assertEqual(201, response.status_code)

    # def test_get_platform_availability_201_time_max(self):
    #     """
    #     GET
    #     figure/platform_availability/test_platform44?time_max=4000-01-01T00:00:00Z
    #     should return a status_code = 201
    #     """
    #     query = 'figure/platform_availability/test_platform44?time_max=' + \
    #         '2200-10-01T00:00:00Z'
    #     response = self.app.get(query, headers={'Authorization': test_token})
    #     self.assertEqual(201, response.status_code)

    # def test_get_parameter_pie_201_qc(self):
    #     """
    #     GET figure/platform_availability/test_platform44?qc=1 should return a
    #     status_code = 201
    #     """
    #     query = 'figure/platform_availability/test_platform44?qc=1'
    #     response = self.app.get(query, headers={'Authorization': test_token})
    #     self.assertEqual(201, response.status_code)

    # def test_get_line_201_all(self):
    #     """
    #     GET figure/parameter_pie/R?
    #     should return a status_code = 201
    #     """
    #     query = 'figure/platform_availability/test_platform44?qc=1&' + \
    #         'time_max=4000-01-01T00:00:00Z&time_min=2000-01-01T00:00:00Z&' + \
    #         'depth_max=20&depth_min=0'
    #     response = self.app.get(query, headers={'Authorization': test_token})
    #     self.assertEqual(201, response.status_code)

    def tearDown(self):
        """
        Delete all generated data
        """
        query = '/admin_metadata/test_platform44'
        self.app.delete(query, headers={'Authorization': test_token})
        for i in range(3):
            query = '/admin_data/M/test_platform44_test_parameter_10_' + \
                f'2200-03-09T21:4{i}:00Z'
            self.app.delete(query, headers={'Authorization': test_token})
