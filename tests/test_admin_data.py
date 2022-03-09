import unittest

from run import app
from config import test_token

class ResourceTest(unittest.TestCase):

    def setUp(self):
        self.app = app.test_client()

    def test_post_data_201(self):
        """
        POST /admin_data/{rule} - rule: R with a valid payload.
        The status_code of the response should be 201.
        """
        query = '/admin_data/R'
        payload = {
            'platform_code': 'test_platform',
            'parameter': 'test_parameter',
            'depth': 10,
            'depth_qc': 1,
            'time': '3000-03-09T18:10:00Z',
            "time_qc": 1,
            "lat": 20,
            "lat_qc": 1,
            "lon": 20,
            "lon_qc": 1,
            "value": 15,
            "qc": 1
        }
        response = self.app.post(query, json=payload, 
                                 headers={'Authorization': test_token})
        self.assertEqual(201, response.status_code)

    def test_delete_data_202(self):
        """
        First,
        POST /admin_data/{rule} - rule: R with a valid payload.
        Then,
        DELETE /admin_data/{rule}/{id} - rule: R,
            id: test_platform_test_parameter_10_3000-03-09T18:45:00Z
        The status_code of the response should be 202.
        """
        query = '/admin_data/R'
        payload = {
            'platform_code': 'test_platform',
            'parameter': 'test_parameter',
            'depth': 10,
            'depth_qc': 1,
            'time': '3000-03-09T18:45:00Z',
            "time_qc": 1,
            "lat": 20,
            "lat_qc": 1,
            "lon": 20,
            "lon_qc": 1,
            "value": 15,
            "qc": 1
        }
        self.app.post(query, json=payload,
                      headers={'Authorization': test_token})
        
        query = '/admin_data/R/test_platform_test_parameter_10_3000-03-09T18:45:00Z'
        response = self.app.delete(query, headers={'Authorization': test_token})

        self.assertEqual(202, response.status_code)

    def test_delete_data_404(self):
        """
        DELETE /admin_data/{rule}/{id} - rule: R,
            id: not_found_id
        The status_code of the response should be 204.
        """
        query = '/admin_data/R/not_found_id'
        response = self.app.delete(query, headers={'Authorization': test_token})

        self.assertEqual(404, response.status_code)

    def tearDown(self):
        """
        Delete all generated data
        """
        query = '/admin_data/R/test_platform_test_parameter_10_3000-03-09T18:10:00Z'
        self.app.delete(query, headers={'Authorization': test_token})
