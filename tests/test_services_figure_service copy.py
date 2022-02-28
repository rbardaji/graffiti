import unittest
import os
import glob

from graffiti.services.figure_service import (thread_line, get_line,
                                              thread_area, get_area)
from config import (platform_code_test, parameter_test, depth_min_test,
                    depth_max_test, time_min_test, time_max_test, qc_test,
                    fig_folder)


class ServicesFigureServiceTest(unittest.TestCase):

    def test_thread_line(self):
        """
        Make a basic line plot. It there is data, thread_line returns the
        location of the html file with the figure. Otherwise, it returns False
        """
        fig_name_test = 'unittest-line'
        figure_path = thread_line(platform_code=platform_code_test,
                                  parameter=parameter_test,
                                  fig_name=fig_name_test)
        if figure_path:
            self.assertEqual(True, os.path.isfile(figure_path))

    def test_thread_line_bad_platform(self):
        """
        Make a basic line plot with a non existing platform_code.
        It returns False.
        """
        fig_name_test = 'unittest-line'
        platform= 'bad_platform'
        figure_path = thread_line(platform_code=platform,
                                  parameter=parameter_test,
                                  fig_name=fig_name_test)

        self.assertEqual(False, figure_path)

    def test_thread_line_bad_parameter(self):
        """
        Make a basic line plot with a non existing parameter.
        It returns False.
        """
        fig_name_test = 'unittest-line'
        param= 'bad_parameter'
        figure_path = thread_line(platform_code=platform_code_test,
                                  parameter=param, fig_name=fig_name_test)

        self.assertEqual(False, figure_path)

    def test_thread_line_depth_min_max(self):
        """
        Make a basic line plot with depth_min, depth_max, time_min, time_max
        and qc.
        It there is data, thread_line returns the location of the html file with
        the figure. Otherwise, it returns False
        """
        fig_name_test = 'unittest-line'
        figure_path = thread_line(platform_code=platform_code_test,
                                  parameter=parameter_test,
                                  fig_name=fig_name_test,
                                  depth_min=depth_min_test,
                                  depth_max=depth_max_test,
                                  time_min=time_min_test,
                                  time_max=time_max_test, qc=qc_test)
        if figure_path:
            self.assertEqual(True, os.path.isfile(figure_path))

    def test_get_line_with_multithreading_check_status_code(self):
        """
        Make a basic line plot with depth_min, depth_max, time_min, time_max
        and qc, with multithreading.
        The status_code is always 201
        """
        _, status_code = get_line(platform_code=platform_code_test,
                                  parameter=parameter_test,
                                  depth_min=depth_min_test,
                                  depth_max=depth_max_test,
                                  time_min=time_min_test,
                                  time_max=time_max_test, qc=qc_test)
    
        self.assertEqual(201, status_code)

    def test_get_line_with_multithreading_check_result(self):
        """
        Make a basic line plot with depth_min, depth_max, time_min, time_max
        and qc, with multithreading.
        The status_code is always 201
        """
        result, _ = get_line(platform_code=platform_code_test,
                                  parameter=parameter_test,
                                  depth_min=depth_min_test,
                                  depth_max=depth_max_test,
                                  time_min=time_min_test,
                                  time_max=time_max_test, qc=qc_test)
        self.assertEqual(1, len(result['result']))

    def test_thread_area(self):
        """
        Make a basic area plot. It there is data, thread_area returns the
        location of the html file with the figure. Otherwise, it returns False
        """
        fig_name_test = 'unittest-area'
        figure_path = thread_area(platform_code=platform_code_test,
                                  parameter=parameter_test,
                                  fig_name=fig_name_test)

        if figure_path:
            self.assertEqual(True, os.path.isfile(figure_path))

    def test_thread_area_bad_platform(self):
        """
        Make a basic area plot with a non existing platform_code.
        It returns False.
        """
        fig_name_test = 'unittest-area'
        platform= 'bad_platform'
        figure_path = thread_area(platform_code=platform,
                                  parameter=parameter_test,
                                  fig_name=fig_name_test)

        self.assertEqual(False, figure_path)

    def test_thread_area_bad_parameter(self):
        """
        Make a basic area plot with a non existing parameter.
        It returns False.
        """
        fig_name_test = 'unittest-area'
        param= 'bad_parameter'
        figure_path = thread_area(platform_code=platform_code_test,
                                  parameter=param, fig_name=fig_name_test)

        self.assertEqual(False, figure_path)

    def test_thread_area_depth_min_max(self):
        """
        Make a basic area plot with depth_min, depth_max, time_min, time_max
        and qc.
        It there is data, thread_area returns the location of the html file with
        the figure. Otherwise, it returns False
        """
        fig_name_test = 'unittest-area'
        figure_path = thread_area(platform_code=platform_code_test,
                                  parameter=parameter_test,
                                  fig_name=fig_name_test,
                                  depth_min=depth_min_test,
                                  depth_max=depth_max_test,
                                  time_min=time_min_test,
                                  time_max=time_max_test, qc=qc_test)
        if figure_path:
            self.assertEqual(True, os.path.isfile(figure_path))

    def test_get_area_with_multithreading_check_status_code(self):
        """
        Make a basic area plot with depth_min, depth_max, time_min, time_max
        and qc, with multithreading.
        The status_code is always 201
        """
        _, status_code = get_area(platform_code=platform_code_test,
                                  parameter=parameter_test,
                                  depth_min=depth_min_test,
                                  depth_max=depth_max_test,
                                  time_min=time_min_test,
                                  time_max=time_max_test, qc=qc_test)
    
        self.assertEqual(201, status_code)

    def test_get_area_with_multithreading_check_result(self):
        """
        Make a basic area plot with depth_min, depth_max, time_min, time_max
        and qc, with multithreading.
        The status_code is always 201
        """
        result, _ = get_area(platform_code=platform_code_test,
                             parameter=parameter_test, depth_min=depth_min_test,
                             depth_max=depth_max_test, time_min=time_min_test,
                             time_max=time_max_test, qc=qc_test)
        self.assertEqual(1, len(result['result']))

    def tearDown(self):
        # Remove html files
        #Loop Through the folder projects all files and deleting them one by one
        for file in glob.glob(f'{fig_folder}/unittest-*'):
            os.remove(file)
