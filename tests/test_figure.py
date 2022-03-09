import unittest

from run import app
from config import test_token

class ResourceTest(unittest.TestCase):

    def setUp(self):
        self.app = app.test_client()
        # Add some data to the DB
        query = '/admin_data/R'
        for i in range(3):
            payload = {
                'platform_code': 'test_platform',
                'parameter': 'test_parameter',
                'depth': 10,
                'depth_qc': 1,
                'time': f'3000-03-09T21:4{i}:00Z',
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

    def test_get_area_201(self):
        """
        GET figure/area/test_platform/test_parameter should return a
        status_code = 201
        """
        query = 'figure/area/test_platform/test_parameter'
        response = self.app.get(query, headers={'Authorization': test_token})
        self.assertEqual(201, response.status_code)
    
    def test_get_area_404_bad_parameter(self):
        """
        GET figure/area/test_platform/bad_parameter should return a
        status_code = 404
        """
        query = 'figure/area/test_platform/bad_parameter'
        response = self.app.get(query, headers={'Authorization': test_token})
        self.assertEqual(404, response.status_code)

    def test_get_area_404_bad_platform(self):
        """
        GET figure/area/bad_platform/test_parameter should return a
        status_code = 404
        """
        query = 'figure/area/test_platform/bad_parameter'
        response = self.app.get(query, headers={'Authorization': test_token})
        self.assertEqual(404, response.status_code)

    def test_get_area_404_bad_platform_and_parameter(self):
        """
        GET figure/area/bad_platform/test_parameter should return a
        status_code = 404
        """
        query = 'figure/area/bad_platform/bad_parameter'
        response = self.app.get(query, headers={'Authorization': test_token})
        self.assertEqual(404, response.status_code)

    def test_get_area_201_depth_min(self):
        """
        GET figure/area/test_platform/test_parameter?depth_min=0 should return a
        status_code = 201
        """
        query = 'figure/area/test_platform/test_parameter?depth_min=0'
        response = self.app.get(query, headers={'Authorization': test_token})
        self.assertEqual(201, response.status_code)

    def test_get_area_201_depth_max(self):
        """
        GET figure/area/test_platform/test_parameter?depth_max=20 should return a
        status_code = 201
        """
        query = 'figure/area/test_platform/test_parameter?depth_max=20'
        response = self.app.get(query, headers={'Authorization': test_token})
        self.assertEqual(201, response.status_code)

    def test_get_area_201_time_min(self):
        """
        GET figure/area/test_platform/test_parameter?time_min=2000-01-01T00:00:00Z
        should return a status_code = 201
        """
        query = 'figure/area/test_platform/test_parameter?time_min=2000-01-01T00:00:00Z'
        response = self.app.get(query, headers={'Authorization': test_token})
        self.assertEqual(201, response.status_code)

    def test_get_area_201_time_max(self):
        """
        GET figure/area/test_platform/test_parameter?time_max=4000-01-01T00:00:00Z
        should return a status_code = 201
        """
        query = 'figure/area/test_platform/test_parameter?time_min=4000-01-01T00:00:00Z'
        response = self.app.get(query, headers={'Authorization': test_token})
        self.assertEqual(201, response.status_code)

    def test_get_area_201_qc(self):
        """
        GET figure/area/test_platform/test_parameter?qc=1
        should return a status_code = 201
        """
        query = 'figure/area/test_platform/test_parameter?qc=1'
        response = self.app.get(query, headers={'Authorization': test_token})
        self.assertEqual(201, response.status_code)

    def tearDown(self):
        """
        Delete all generated data
        """
        for i in range(3):
            query = '/admin_data/R/test_platform_test_parameter_10_' + \
                f'3000-03-09T21:4{i}:00Z'
            self.app.delete(query, headers={'Authorization': test_token})
