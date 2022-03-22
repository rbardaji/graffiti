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

    def test_get_data_count_200(self):
        """
        GET /data/count/{rule} - rule: R with a valid payload.
        The status_code of the response should be 200.
        """
        query = '/data/count/R'
    
        response = self.app.get(query, headers={'Authorization': test_token})
        self.assertEqual(200, response.status_code)

    def test_get_data_200(self):
        """
        GET /data/R/test_platform/test_parameter.
        The status_code of the response should be 200.
        """
        query = '/data/R/test_platform/test_parameter'
    
        response = self.app.get(query, headers={'Authorization': test_token})
        self.assertEqual(200, response.status_code)

    def tearDown(self):
        """
        Delete all generated data
        """
        for i in range(3):
            query = '/admin_data/R/test_platform_test_parameter_10_' + \
                f'3000-03-09T21:4{i}:00Z'
            self.app.delete(query, headers={'Authorization': test_token})
