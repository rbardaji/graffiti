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
                'parameter': 'test_parameter1',
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
        for i in range(3):
            payload = {
                'platform_code': 'test_platform',
                'parameter': 'test_parameter2',
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

    def test_get_scatter_201(self):
        """
        GET figure/scatter/test_platform/test_parameter1/test_parameter2 should
        return a status_code = 201
        """
        query = 'figure/scatter/test_platform/test_parameter1/test_parameter2'
        response = self.app.get(query, headers={'Authorization': test_token})
        self.assertEqual(201, response.status_code)
    
    def test_get_scatter_404_bad_parameter(self):
        """
        GET figure/scatter/test_platform/test_parameter1/bad_parameter should
        return a status_code = 404
        """
        query = 'figure/scatter/test_platform/test_parameter1/bad_parameter'
        response = self.app.get(query, headers={'Authorization': test_token})
        self.assertEqual(404, response.status_code)

    def test_get_scatter_404_bad_platform(self):
        """
        GET figure/scatter/bad_platform/test_parameter1/test_parameter2 should
        return a status_code = 404
        """
        query = 'figure/scatter/bad_platform/test_parameter1/test_parameter2'
        response = self.app.get(query, headers={'Authorization': test_token})
        self.assertEqual(404, response.status_code)

    def test_get_scatter_404_bad_platform_and_parameter(self):
        """
        GET figure/scatter/bad_platform/bad_parameter/test_parameter2 should
        return a status_code = 404
        """
        query = 'figure/scatter/bad_platform/bad_parameter/test_parameter2'
        response = self.app.get(query, headers={'Authorization': test_token})
        self.assertEqual(404, response.status_code)

    def test_get_scatter_201_depth_min(self):
        """
        GET
        figure/scatter/test_platform/test_parameter1/test_parameter2?depth_min=0
        should return a status_code = 201
        """
        query = 'figure/scatter/test_platform/test_parameter1/test_parameter2?depth_min=0'
        response = self.app.get(query, headers={'Authorization': test_token})
        self.assertEqual(201, response.status_code)

    def test_get_scatter_201_depth_max(self):
        """
        GET
        figure/scatter/test_platform/test_parameter1/test_parameter2?depth_max=20
        should return a status_code = 201
        """
        query = 'figure/scatter/test_platform/test_parameter1/test_parameter2?depth_max=20'
        response = self.app.get(query, headers={'Authorization': test_token})
        self.assertEqual(201, response.status_code)

    def test_get_scatter_201_time_min(self):
        """
        GET
        figure/scatter/test_platform/test_parameter1/test_parameter2?time_min=2000-01-01T00:00:00Z
        should return a status_code = 201
        """
        query = 'figure/scatter/test_platform/test_parameter1/test_parameter2?time_min=2000-01-01T00:00:00Z'
        response = self.app.get(query, headers={'Authorization': test_token})
        self.assertEqual(201, response.status_code)

    def test_get_scatter_201_time_max(self):
        """
        GET
        figure/scatter/test_platform/test_parameter1/test_parameter2?time_max=4000-01-01T00:00:00Z
        should return a status_code = 201
        """
        query = 'figure/scatter/test_platform/test_parameter1/test_parameter2?time_max=4000-01-01T00:00:00Z'
        response = self.app.get(query, headers={'Authorization': test_token})
        self.assertEqual(201, response.status_code)

    def test_get_scatter_201_qc(self):
        """
        GET figure/scatter/test_platform/test_parameter1/test_parameter2?qc=1
        should return a status_code = 201
        """
        query = 'figure/scatter/test_platform/test_parameter1/test_parameter2?qc=1'
        response = self.app.get(query, headers={'Authorization': test_token})
        self.assertEqual(201, response.status_code)

    def test_get_scatter_201_color_depth(self):
        """
        GET
        figure/scatter/test_platform/test_parameter1/test_parameter2?
        color=depth
        should return a status_code = 201
        """
        query = 'figure/scatter/test_platform/test_parameter1/test_' + \
            'parameter2?color=depth'
        response = self.app.get(query, headers={'Authorization': test_token})
        self.assertEqual(201, response.status_code)

    def test_get_scatter_201_color_time(self):
        """
        GET
        figure/scatter/test_platform/test_parameter1/test_parameter2?
        color=time
        should return a status_code = 201
        """
        query = 'figure/scatter/test_platform/test_parameter1/test_' + \
            'parameter2?color=time'
        response = self.app.get(query, headers={'Authorization': test_token})
        self.assertEqual(201, response.status_code)

    def test_get_scatter_201_marginal_x_histogram(self):
        """
        GET
        figure/scatter/test_platform/test_parameter1/test_parameter2?
        marginal_x=histogram
        should return a status_code = 201
        """
        query = 'figure/scatter/test_platform/test_parameter1/test_' + \
            'parameter2?marginal_x=histogram'
        response = self.app.get(query, headers={'Authorization': test_token})
        self.assertEqual(201, response.status_code)

    def test_get_scatter_201_marginal_x_rug(self):
        """
        GET
        figure/scatter/test_platform/test_parameter1/test_parameter2?
        marginal_x=rug
        should return a status_code = 201
        """
        query = 'figure/scatter/test_platform/test_parameter1/test_' + \
            'parameter2?marginal_x=rug'
        response = self.app.get(query, headers={'Authorization': test_token})
        self.assertEqual(201, response.status_code)

    def test_get_scatter_201_marginal_x_box(self):
        """
        GET
        figure/scatter/test_platform/test_parameter1/test_parameter2?
        marginal_x=box
        should return a status_code = 201
        """
        query = 'figure/scatter/test_platform/test_parameter1/test_' + \
            'parameter2?marginal_x=box'
        response = self.app.get(query, headers={'Authorization': test_token})
        self.assertEqual(201, response.status_code)

    def test_get_scatter_201_marginal_x_violin(self):
        """
        GET
        figure/scatter/test_platform/test_parameter1/test_parameter2?
        marginal_x=violin
        should return a status_code = 201
        """
        query = 'figure/scatter/test_platform/test_parameter1/test_' + \
            'parameter2?marginal_x=violin'
        response = self.app.get(query, headers={'Authorization': test_token})
        self.assertEqual(201, response.status_code)

    def test_get_scatter_201_marginal_y_histogram(self):
        """
        GET
        figure/scatter/test_platform/test_parameter1/test_parameter2?
        marginal_y=histogram
        should return a status_code = 201
        """
        query = 'figure/scatter/test_platform/test_parameter1/test_' + \
            'parameter2?marginal_y=histogram'
        response = self.app.get(query, headers={'Authorization': test_token})
        self.assertEqual(201, response.status_code)

    def test_get_scatter_201_marginal_y_rug(self):
        """
        GET
        figure/scatter/test_platform/test_parameter1/test_parameter2?
        marginal_y=rug
        should return a status_code = 201
        """
        query = 'figure/scatter/test_platform/test_parameter1/test_' + \
            'parameter2?marginal_y=rug'
        response = self.app.get(query, headers={'Authorization': test_token})
        self.assertEqual(201, response.status_code)

    def test_get_scatter_201_marginal_y_box(self):
        """
        GET
        figure/scatter/test_platform/test_parameter1/test_parameter2?
        marginal_y=box
        should return a status_code = 201
        """
        query = 'figure/scatter/test_platform/test_parameter1/test_' + \
            'parameter2?marginal_y=box'
        response = self.app.get(query, headers={'Authorization': test_token})
        self.assertEqual(201, response.status_code)

    def test_get_scatter_201_marginal_y_violin(self):
        """
        GET
        figure/scatter/test_platform/test_parameter1/test_parameter2?
        marginal_y=violin
        should return a status_code = 201
        """
        query = 'figure/scatter/test_platform/test_parameter1/test_' + \
            'parameter2?marginal_y=violin'
        response = self.app.get(query, headers={'Authorization': test_token})
        self.assertEqual(201, response.status_code)

    def test_get_scatter_201_trendline_ols(self):
        """
        GET
        figure/scatter/test_platform/test_parameter1/test_parameter2?
        trendline=ols
        should return a status_code = 201
        """
        query = 'figure/scatter/test_platform/test_parameter1/test_' + \
            'parameter2?trendline=ols'
        response = self.app.get(query, headers={'Authorization': test_token})
        self.assertEqual(201, response.status_code)

    def test_get_scatter_201_trendline_lowess(self):
        """
        GET
        figure/scatter/test_platform/test_parameter1/test_parameter2?
        trendline=lowess
        should return a status_code = 201
        """
        query = 'figure/scatter/test_platform/test_parameter1/test_' + \
            'parameter2?trendline=lowess'
        response = self.app.get(query, headers={'Authorization': test_token})
        self.assertEqual(201, response.status_code)

    def test_get_scatter_201_trendline_expanding(self):
        """
        GET
        figure/scatter/test_platform/test_parameter1/test_parameter2?
        trendline=expanding
        should return a status_code = 201
        """
        query = 'figure/scatter/test_platform/test_parameter1/test_' + \
            'parameter2?trendline=expanding'
        response = self.app.get(query, headers={'Authorization': test_token})
        self.assertEqual(201, response.status_code)

    def test_get_scatter_201_template_ggplot2(self):
        """
        GET
        figure/scatter/test_platform/test_parameter1/test_parameter2?
        template=ggplot2
        should return a status_code = 201
        """
        query = 'figure/scatter/test_platform/test_parameter1/test_' + \
            'parameter2?template=ggplot2'
        response = self.app.get(query, headers={'Authorization': test_token})
        self.assertEqual(201, response.status_code)

    def test_get_scatter_201_template_seaborn(self):
        """
        GET
        figure/scatter/test_platform/test_parameter1/test_parameter2?
        template=seaborn
        should return a status_code = 201
        """
        query = 'figure/scatter/test_platform/test_parameter1/test_' + \
            'parameter2?template=seaborn'
        response = self.app.get(query, headers={'Authorization': test_token})
        self.assertEqual(201, response.status_code)

    def test_get_scatter_201_template_simple_white(self):
        """
        GET
        figure/scatter/test_platform/test_parameter1/test_parameter2?
        template=simple_white
        should return a status_code = 201
        """
        query = 'figure/scatter/test_platform/test_parameter1/test_' + \
            'parameter2?template=simple_white'
        response = self.app.get(query, headers={'Authorization': test_token})
        self.assertEqual(201, response.status_code)

    def test_get_scatter_201_template_plotly(self):
        """
        GET
        figure/scatter/test_platform/test_parameter1/test_parameter2?
        template=plotly
        should return a status_code = 201
        """
        query = 'figure/scatter/test_platform/test_parameter1/test_' + \
            'parameter2?template=plotly'
        response = self.app.get(query, headers={'Authorization': test_token})
        self.assertEqual(201, response.status_code)

    def test_get_scatter_201_template_plotly_white(self):
        """
        GET
        figure/scatter/test_platform/test_parameter1/test_parameter2?
        template=plotly_white
        should return a status_code = 201
        """
        query = 'figure/scatter/test_platform/test_parameter1/test_' + \
            'parameter2?template=plotly_white'
        response = self.app.get(query, headers={'Authorization': test_token})
        self.assertEqual(201, response.status_code)

    def test_get_scatter_201_template_plotly_dark(self):
        """
        GET
        figure/scatter/test_platform/test_parameter1/test_parameter2?
        template=plotly_dark
        should return a status_code = 201
        """
        query = 'figure/scatter/test_platform/test_parameter1/test_' + \
            'parameter2?template=plotly_dark'
        response = self.app.get(query, headers={'Authorization': test_token})
        self.assertEqual(201, response.status_code)

    def test_get_scatter_201_template_presentation(self):
        """
        GET
        figure/scatter/test_platform/test_parameter1/test_parameter2?
        template=presentation
        should return a status_code = 201
        """
        query = 'figure/scatter/test_platform/test_parameter1/test_' + \
            'parameter2?template=presentation'
        response = self.app.get(query, headers={'Authorization': test_token})
        self.assertEqual(201, response.status_code)

    def test_get_scatter_201_template_xgridoff(self):
        """
        GET
        figure/scatter/test_platform/test_parameter1/test_parameter2?
        template=xgridoff
        should return a status_code = 201
        """
        query = 'figure/scatter/test_platform/test_parameter1/test_' + \
            'parameter2?template=xgridoff'
        response = self.app.get(query, headers={'Authorization': test_token})
        self.assertEqual(201, response.status_code)

    def test_get_scatter_201_template_ygridoff(self):
        """
        GET
        figure/scatter/test_platform/test_parameter1/test_parameter2?
        template=ygridoff
        should return a status_code = 201
        """
        query = 'figure/scatter/test_platform/test_parameter1/test_' + \
            'parameter2?template=ygridoff'
        response = self.app.get(query, headers={'Authorization': test_token})
        self.assertEqual(201, response.status_code)

    def test_get_scatter_201_template_gridon(self):
        """
        GET
        figure/scatter/test_platform/test_parameter1/test_parameter2?
        template=gridon
        should return a status_code = 201
        """
        query = 'figure/scatter/test_platform/test_parameter1/test_' + \
            'parameter2?template=gridon'
        response = self.app.get(query, headers={'Authorization': test_token})
        self.assertEqual(201, response.status_code)

    def test_get_scatter_201_all(self):
        """
        GET figure/scatter/test_platform/test_parameter1/test_parameter2?
        should return a status_code = 201
        """
        query = 'figure/scatter/test_platform/test_parameter1/test_' + \
            'parameter2?qc=1&' + \
            'time_max=4000-01-01T00:00:00Z&time_min=2000-01-01T00:00:00Z&' + \
            'depth_max=20&depth_min=0&color=depth&marginal_x=histogram' + \
            '&marginal_y=histogram&trendline=ols&template=ggplot2'
        response = self.app.get(query, headers={'Authorization': test_token})
        self.assertEqual(201, response.status_code)

    def tearDown(self):
        """
        Delete all generated data
        """
        for i in range(3):
            query = '/admin_data/R/test_platform_test_parameter1_10_' + \
                f'3000-03-09T21:4{i}:00Z'
            self.app.delete(query, headers={'Authorization': test_token})
        for i in range(3):
            query = '/admin_data/R/test_platform_test_parameter2_10_' + \
                f'3000-03-09T21:4{i}:00Z'
            self.app.delete(query, headers={'Authorization': test_token})
