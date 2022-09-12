import os
import glob
import pandas as pd
import hashlib as hash
import json
import requests
import datetime

from elasticsearch import Elasticsearch, exceptions
from elasticsearch_dsl import Search, A
from xml.etree import ElementTree as ET
from flask import abort, request

from .helper import index_name, time_to_str
from .auth_manager import get_token_info

from config import (elastic_host, data_index_r, data_index_h, data_index_2h,
                    data_index_3h, data_index_6h, data_index_8h, data_index_12h,
                    data_index_d, data_index_2d, data_index_3d, data_index_4d,
                    data_index_5d, data_index_6d, data_index_10d, api_index,
                    data_index_15d, data_index_m, max_plot_points, df_folder,
                    metadata_index, vocabulary_index, fig_folder, pid_folder,
                    pid_url, csv_folder, csv_url)


def data_ingestion(index_name, data):
    """
    Add data to a DB.

    Parameters
    ----------
        index_name: str
            Name of the index or the table
        data: dict
            Data to ingest.
    
    Returns
    -------
        status: int
            201 if data is ingested, 500 if Connection Error
    """
    status = 500
    elastic = Elasticsearch(elastic_host)
    try:
        elastic.index(index=index_name, document=data)

        status = 201
    except exceptions.ConnectionError:
        status = 500
    elastic.close()
    return status


def good_rule(search_string):
    """
    Data is ingested in several average periods. This function helps to decide
    the average rule according to the input search string and the configured
    max_plot_points.

    Parameters
    ----------
        search_string: str
            Search string for Elastic Search
    
    Returns
    -------
        rule: str - bool
            The best rule to use. If the function detects a connection error
            or a bad search query (check the dates), it returns False
    """
    elastic_indexes = [
        ('R', data_index_r),
        ('H', data_index_h),
        ('2H', data_index_2h),
        ('3H', data_index_3h),
        ('6H', data_index_6h),
        ('8H', data_index_8h),
        ('12H', data_index_12h),
        ('D', data_index_d),
        ('2D', data_index_2d),
        ('3D', data_index_3d),
        ('4D', data_index_4d),
        ('5D', data_index_5d),
        ('6D', data_index_6d),
        ('10D', data_index_10d),
        ('15D', data_index_15d),
        ('M', data_index_m)
    ]
    # Convert search string to dict
    search_dict = eval(search_string)

    depth_min = search_dict.pop('depth_min', False)
    depth_max = search_dict.pop('depth_max', False)
    time_min = search_dict.pop('time_min', False)
    time_max = search_dict.pop('time_max', False)

    search_body = {}
    search_body['query'] = {'bool': {'must': []}}
    for key, value in search_dict.items():
        if isinstance(value, list):
            should_search = {'bool': {'should': []}}
            for element in value:
                should_search['bool']['should'].append(
                    {'match': {key: element}})
            search_body['query']['bool']['must'].append(should_search)
        else:
            search_body['query']['bool']['must'].append(
                {'match': {key: value}})

    if depth_min or depth_max:
        search_range = {'range': {}}
        search_range['range']['depth'] = {}

        if depth_min:
            search_range['range']['depth']['gte'] = depth_min
        if depth_max:
            search_range['range']['depth']['lte'] = depth_max

        search_body['query']['bool']['must'].append(search_range)

    if time_min or time_max:
        search_range = {'range': {}}
        search_range['range']['time'] = {}

        if time_min:
            search_range['range']['time']['gte'] = time_min
        if time_max:
            search_range['range']['time']['lte'] = time_max

        search_body['query']['bool']['must'].append(search_range)

    elastic = Elasticsearch(elastic_host)

    rule = 'R'
    for rule_in, index in elastic_indexes:
        try:
            elastic_search = Search(
                    using=elastic, index=index).update_from_dict(search_body)
            
        except exceptions.ConnectionError:
            elastic.close()
            return False

        try:
            count = elastic_search.count()
        except exceptions.NotFoundError:
            count = 0
        except exceptions.ConnectionError:
            elastic.close()
            return False
        except exceptions.RequestError:
            elastic.close()
            return False
        
        if (count != 0 and count < max_plot_points) or rule == 'M':
            rule = rule_in
    elastic.close()
    return rule


def get_data(search_string=None, rule=None):
    """
    Get a list if ids odf data from the database that match with the input
    search_string.

    Parameters
    ----------
        search_string: str
            Search query for elasticsearch
        rule: str
            Options - M, 15D, 10D, 6D, 5D, 4D, 3D, 2D, D, 12H, 8H, 6H, 3H, 2H, H

    Returns
    -------
        (response, status_code): (dict, int)
            The response is a dict with keys - status, message and result.
                The status is a bool that indicates if there are data in the
                result field.
                The message is a str with comments for the user.
                The reslut is a list of the ids of data.
            The status_code is 200 (found) or 404 (not found).
    """
    elastic = Elasticsearch(elastic_host)

    if search_string:
        # Convert search string to dict
        search_dict = eval(search_string)

        depth_min = search_dict.pop('depth_min', False)
        depth_max = search_dict.pop('depth_max', False)
        time_min = search_dict.pop('time_min', False)
        time_max = search_dict.pop('time_max', False)

        search_body = {}
        search_body['query'] = {'bool': {'must': []}}
        for key, value in search_dict.items():
            if isinstance(value, list):
                should_search = {'bool': {'should': []}}
                for element in value:
                    should_search['bool']['should'].append(
                        {'match': {key: element}})
                search_body['query']['bool']['must'].append(should_search)
            else:
                search_body['query']['bool']['must'].append(
                    {'match': {key: value}})

        if depth_min or depth_max:
            search_range = {'range': {}}
            search_range['range']['depth'] = {}

            if depth_min:
                search_range['range']['depth']['gte'] = depth_min
            if depth_max:
                search_range['range']['depth']['lte'] = depth_max

            search_body['query']['bool']['must'].append(search_range)

        if time_min or time_max:
            search_range = {'range': {}}
            search_range['range']['time'] = {}

            if time_min:
                search_range['range']['time']['gte'] = time_min
            if time_max:
                search_range['range']['time']['lte'] = time_max

            search_body['query']['bool']['must'].append(search_range)

        elastic_search = Search(
            using=elastic, index=index_name(rule)).update_from_dict(search_body)

    else:
        # Get all ids
        elastic_search = Search(using=elastic, index=index_name(rule))

    elastic_search = elastic_search.source([])  # only get ids
    ids = [h.meta.id for h in elastic_search.scan()]
    if ids:
        response = {
            'status': True,
            'message': 'List of data records',
            'result': ids
        }
        status_code = 200
    else:
        # elastic.close()
        # abort(404, 'Data not found')
        response = {
            'status': False,
            'message': 'Data not found',
            'result': []
        }
        status_code = 404

    elastic.close()

    return response, status_code


def get_data_id(id_data, rule=None):
    """
    Get the data values from the input id_data.

    Parameters
    ----------
        id_data: str
            Id from the data.
        rule: str
            Options - M, 15D, 10D, 6D, 5D, 4D, 3D, 2D, D, 12H, 8H, 6H, 3H, 2H, H
    
    Returns
    -------
        (response, status_code): (dict, int)
            The response is a dict with keys: status, message and result.
                The status is a bool that indicates that data is found.
                The message is a str with comments for the user.
                The result is a list with a dict. The key of the dict is the
                data_id and the value is the content of the data_id fro the
                database.
            The status_code is 200 - found, 204 - not found or 503 -
            connection error
    """
    elastic = Elasticsearch(elastic_host)

    try:
        response = elastic.get(index=index_name(rule), id=id_data)
        if response.get('found'):
            response = {
                'status': True,
                'message': '',
                'result': [{id_data: response['_source']}]
            }
            status_code = 200
        else:
            response = {
                'status': False,
                'message': 'Data not found',
                'result': []
            }
            status_code = 204
    
    except exceptions.NotFoundError:
            response = {
                'status': False,
                'message': 'Data not found',
                'result': []
            }
            status_code = 204
    except exceptions.ConnectionError:
        response = {
            'status': False,
            'message': 'Internal error. Unable to connect to DB',
            'result': []
        }
        status_code = 503

    elastic.close()

    return response, status_code


def get_df(platform_code_list, parameter_list, rule, depth_min=None,
           depth_max=None, time_min=None, time_max=None, qc=None):
    """
    Get data from the database and make a pandas DataFrame.

    Parameters
    ----------
        platform_code_list: str or list of str
            Platform code
        parameter_list: str or list of str
            Variable to add to the DataFrame.
        rule: str
            Rule of the average values to get the data.
            Options - M, 15D, 10D, 6D, 5D, 4D, 3D, 2D, D, 12H, 8H, 6H, 3H, 2H, H
        depth_min: float
            Minimum depth of the measurement.
        depth_max: float
            Maximum depth of the measurement.
        time_min: str
            Minimum date and time of the measurement. A generic ISO datetime
            parser, where the date must include the year at a minimum, and the
            time (separated by T), is optional.
            Examples: yyyy-MM-dd'T'HH:mm:ss.SSSZ or yyyy-MM-dd.
        time_max: str
            Maximum date and time of the measurement. A generic ISO datetime
            parser, where the date must include the year at a minimum, and the
            time (separated by T), is optional.
            Examples: yyyy-MM-dd'T'HH:mm:ss.SSSZ or yyyy-MM-dd.
        qc: int
            Quality Flag value of the measurement.

    Returns
    -------
        df: pandas DataFrame
    """
    print('Getting data from the database...')

    if isinstance(platform_code_list, str):
        platform_code_list = [platform_code_list]
    if isinstance(parameter_list, str):
        parameter_list = [parameter_list]

    time_min_str, time_max_str = time_to_str(time_min, time_max)

    df = pd.DataFrame()

    for platform_code in platform_code_list:

        print('Getting data from platform {}...'.format(platform_code))
        for parameter in parameter_list:
            print('Getting data from parameter {}...'.format(parameter))

            df_name = f'{platform_code}_{parameter}_{rule}_dmin{depth_min}' + \
                f'_dmax{depth_max}_tmin{time_min_str}_tmax{time_max_str}_qc{qc}'

            if not os.path.exists(f'{df_folder}/{df_name}.pkl'):

                # Check if folder exist
                if not os.path.exists(df_folder):
                    os.makedirs(df_folder)

                # Get all data from 'platform_code'
                search_string = '{"platform_code":' + f'"{platform_code}"' + \
                    ',"parameter":' + f'"{parameter}"' + '}'
                if depth_min:
                    search_string = search_string[:-1] + \
                        f',"depth_min":{depth_min}' + '}'
                if depth_max:
                    search_string = search_string[:-1] + \
                        f',"depth_max":{depth_max}' + '}'
                if time_min:
                    search_string = search_string[:-1] + \
                        f',"time_min":"{time_min}"' + '}'
                if time_max:
                    search_string = search_string[:-1] + \
                        f',"time_max":"{time_max}"' + '}'
                if qc:
                    search_string = search_string[:-1] + \
                        f',"qc":{qc}' + '}'

                response, status_code = get_data(search_string, rule)

                data_id_list = response['result']
                data = []
                for data_id in data_id_list:
                    print('Getting data from id {}...'.format(data_id))
                    response, status_code = get_data_id(data_id, rule)


                    if status_code != 200:
                        return response, status_code

                    # Some data has erroneus values, let's delete it
                    if float(
                        response['result'][0][data_id]['value']) > 40 and \
                            parameter == 'TEMP':
                        continue
                    if float(
                        response['result'][0][data_id]['value']) < 1 and \
                            parameter == 'TEMP':
                        continue
                    if float(
                        response['result'][0][data_id]['value']) < 30 and \
                            parameter == 'PSAL':
                        continue
                    if platform_code == 'OBSEA' and \
                        float(response['result'][0][data_id]['value']) < 10 and \
                            parameter == 'TEMP':
                        continue
                    if platform_code == 'OBSEA' and \
                        float(response['result'][0][data_id]['value']) > 30 and \
                            parameter == 'TEMP':
                        continue

                    data.append(response['result'][0][data_id])

                if data:
                    df_part = pd.DataFrame(data)
                    df_part = df_part.sort_values(by='time')
                    df_part.to_pickle(f'{df_folder}/{df_name}.pkl')
                else:
                    df_part = pd.DataFrame()
            else:
                df_part = pd.read_pickle(f'{df_folder}/{df_name}.pkl')

            if not df_part.empty:
                if df.empty:
                    # Copy the first time
                    df = df_part.copy()
                else:
                    df = pd.concat([df, df_part])
    if not df.empty:
        df = df.sort_values(by='time')
    return df


def get_metadata(platform_code: str=None, parameter: str=None,
                 depth_min: float=None, depth_max: float=None,
                 time_min: str=None, time_max: str=None, qc: int=None,
                 output: str=None):
    """
    Get a list of the avialable platform_codes (the ID of the metadata resurces)

    Parameters
    ----------
        platform_code: str
            Platform code
        parameter: str
            Variable to add to the DataFrame.
        depth_min: float
            Minimum depth of the measurement.
        depth_max: float
            Maximum depth of the measurement.
        time_min: str
            Minimum date and time of the measurement. A generic ISO datetime
            parser, where the date must include the year at a minimum, and the
            time (separated by T), is optional.
            Examples: yyyy-MM-dd'T'HH:mm:ss.SSSZ or yyyy-MM-dd.
        time_max: str
            Maximum date and time of the measurement. A generic ISO datetime
            parser, where the date must include the year at a minimum, and the
            time (separated by T), is optional.
            Examples: yyyy-MM-dd'T'HH:mm:ss.SSSZ or yyyy-MM-dd.
        qc: int
            Quality Flag value of the measurement.
        output: str
            Field to be in the result of the response. It can be 'platform_code'
            or 'parameter'. By default, it is 'platform_code'.

    Returns
    -------
        (response, status_code): (dict, int)
            The response is a dict with keys: status, message and result.
                The status is a bool that indicates that data is found.
                The message is a str with comments for the user.
                The result is a list with the platform_code's.
            The status_code is 200 - found, 204 - Not found or
            503 - connection error
    """

    if output == 'parameter':
        platform_code_list = platform_code.split(',')
        parameter_list = []
        for one_platform in platform_code_list:
            parameter_response, status_code = get_parameter(
                platform_code=one_platform, depth_min=depth_min,
                depth_max=depth_max, time_min=time_min, time_max=time_max,
                qc=qc)

            if status_code == 200:
                one_parameter_list = [
                    one_param[
                        'key'] for one_param in parameter_response['result']]
            else:
                one_parameter_list = []
            parameter_list += one_parameter_list

        parameter_list = list(set(parameter_list))
        response = {
            'status': True,
            'message': 'List of parameters',
            'result': parameter_list
        }
        status_code = 200
    else:
        elastic = Elasticsearch(elastic_host)

        try:
            elastic_search = Search(using=elastic, index=metadata_index)

            elastic_search = elastic_search.source([])  # only get ids
            ids = [h.meta.id for h in elastic_search.scan()]

            # Only return the platform code if it contains data
            platform_code_list = []
            for platform_code in ids:
                response, status_code = get_data_count('M', platform_code,
                                                    parameter, depth_min,
                                                    depth_max, time_min,
                                                    time_max, qc)
                if status_code == 200 and response['result'][0] > 0:
                    platform_code_list.append(platform_code)

            if platform_code_list:
                response = {
                    'status': True,
                    'message': 'List of platform codes',
                    'result': platform_code_list
                }
                status_code = 200
            else:
                abort(404, 'List of platform codes - empty')
        except exceptions.NotFoundError:
                abort(404, 'List of platform codes - empty')
        except exceptions.ConnectionError:
            abort(503, 'Connection error with the DB')

        elastic.close()
    return response, status_code


def get_parameter(platform_code=None, depth_min=None, depth_max=None,
                  time_min=None, time_max=None, qc=None, rule=None):
    """
    Get a list of the available parameters

    Parameters
    ----------
        platform_code: str or list of str
            Platform code or list of platform_code
        depth_min: float
            Minimum depth of the measurement.
        depth_max: float
            Maximum depth of the measurement.
        time_min: str
            Minimum date and time of the measurement. A generic ISO datetime
            parser, where the date must include the year at a minimum, and the
            time (separated by T), is optional.
            Examples: yyyy-MM-dd'T'HH:mm:ss.SSSZ or yyyy-MM-dd.
        time_max: str
            Maximum date and time of the measurement. A generic ISO datetime
            parser, where the date must include the year at a minimum, and the
            time (separated by T), is optional.
            Examples: yyyy-MM-dd'T'HH:mm:ss.SSSZ or yyyy-MM-dd.
        qc: int
            Quality Control value of the measurement.
        rule: str
            Rule if the index.

    Returns
    -------
        (response, status_code): (dict, int)
            The response is a dict with keys: status, message and result.
                The status is a bool that indicates that data is found.
                The message is a str with comments for the user.
                The result is a list with the platform_code's.
            The status_code is 200 - found or 503 - connection error
    """
    elastic = Elasticsearch(elastic_host)

    a = A('terms', field='parameter')

    if depth_min or depth_max or time_min or time_max or qc or platform_code:

        search_body = {}
        search_body['query'] = {'bool': {'must': []}}

        search_dict = {}
        if platform_code:
            search_dict['platform_code'] = platform_code
        if qc:
            search_dict['qc'] = qc

        for key, value in search_dict.items():
            if isinstance(value, list):
                should_search = {'bool': {'should': []}}
                for element in value:
                    should_search['bool']['should'].append(
                        {'match': {key: element}})
                search_body['query']['bool']['must'].append(should_search)
            else:
                search_body['query']['bool']['must'].append(
                    {'match': {key: value}})

        if depth_min or depth_max:
            search_range = {'range': {}}
            search_range['range']['depth'] = {}

            if depth_min:
                search_range['range']['depth']['gte'] = depth_min
            if depth_max:
                search_range['range']['depth']['lte'] = depth_max

            search_body['query']['bool']['must'].append(search_range)

        if time_min or time_max:
            search_range = {'range': {}}
            search_range['range']['time'] = {}

            if time_min:
                search_range['range']['time']['gte'] = time_min
            if time_max:
                search_range['range']['time']['lte'] = time_max

            search_body['query']['bool']['must'].append(search_range)

        connected = True
        try:
            elastic_search = Search(
                using=elastic,
                index=index_name(rule)).update_from_dict(search_body)
        except exceptions.ConnectionError:
            connected = False
            response = {
                'status': False,
                'message': 'Internal error. Unable to connect to DB',
                'result': []
            }
            status_code = 503

    else:
        connected = True
        try:
            # Get all ids
            elastic_search = Search(using=elastic, index=index_name(rule))
        except exceptions.ConnectionError:
            connected = False
            response = {
                'status': False,
                'message': 'Internal error. Unable to connect to DB',
                'result': []
            }
            status_code = 503

    if connected:
        elastic_search.aggs.bucket('parameter_terms', a)
        response = elastic_search.execute()
        diccionario = response.to_dict()
        elastic.close()
        response = {
            'status': True,
            'message': 'List of parameters',
            'result': diccionario['aggregations']['parameter_terms']['buckets']
        }
        status_code = 200

    return response, status_code


def get_data_count(rule, platform_code=None, parameter=None, depth_min=None,
                   depth_max=None, time_min=None, time_max=None, qc=None):
    """
    Get the number of values obtained that match the given search string and
    rule.

    Parameters
    ----------
        rule: str
            Options - M, 15D, 10D, 6D, 5D, 4D, 3D, 2D, D, 12H, 8H, 6H, 3H, 2H, H
         platform_code: str or list of str
            Platform code or list of platform_code
        parameter: str or list of str
            Parameter acronym or list of parameters.
        depth_min: float
            Minimum depth of the measurement.
        depth_max: float
            Maximum depth of the measurement.
        time_min: str
            Minimum date and time of the measurement. A generic ISO datetime
            parser, where the date must include the year at a minimum, and the
            time (separated by T), is optional.
            Examples: yyyy-MM-dd'T'HH:mm:ss.SSSZ or yyyy-MM-dd.
        time_max: str
            Maximum date and time of the measurement. A generic ISO datetime
            parser, where the date must include the year at a minimum, and the
            time (separated by T), is optional.
            Examples: yyyy-MM-dd'T'HH:mm:ss.SSSZ or yyyy-MM-dd.
        qc: int
            Quality Control value of the measurement.

    Returns
    -------
        (response, status_code): (dict, int)
            The response is a dict with keys - status, message and result.
                The status is a bool that indicates if there are data in the
                result field.
                The message is a str with comments for the user.
                The reslut is the number of the values.
            The status_code is 200 (found).
    """
    search_body = {'query': {'match_all': {}}}

    if platform_code or parameter or depth_min or depth_max or time_min or \
        time_max or qc:

        search_body = {}
        search_body['query'] = {'bool': {'must': []}}

        if platform_code:
            if isinstance(platform_code, str):
                platform_code = [platform_code]

            should_search = {'bool': {'should': []}}
            for platform in platform_code:
                should_search['bool']['should'].append(
                    {'match': {'platform_code': platform}})
            search_body['query']['bool']['must'].append(should_search)

        if parameter:
            if isinstance(parameter, str):
                parameter = [parameter]

            should_search = {'bool': {'should': []}}
            for param in parameter:
                should_search['bool']['should'].append(
                    {'match': {'parameter': param}})
            search_body['query']['bool']['must'].append(should_search)

        if qc:
            if isinstance(qc, str):
                qc = [qc]

            should_search = {'bool': {'should': []}}
            for flag in qc:
                should_search['bool']['should'].append(
                    {'match': {'qc': flag}})
            search_body['query']['bool']['must'].append(should_search)

        if depth_min or depth_max:
            search_range = {'range': {}}
            search_range['range']['depth'] = {}

            if depth_min:
                search_range['range']['depth']['gte'] = depth_min
            if depth_max:
                search_range['range']['depth']['lte'] = depth_max

            search_body['query']['bool']['must'].append(search_range)

        if time_min or time_max:
            search_range = {'range': {}}
            search_range['range']['time'] = {}

            if time_min:
                search_range['range']['time']['gte'] = time_min
            if time_max:
                search_range['range']['time']['lte'] = time_max

            search_body['query']['bool']['must'].append(search_range)

    elastic = Elasticsearch(elastic_host)

    elastic_search = Search(
            using=elastic, index=index_name(rule)).update_from_dict(search_body)

    try:
        count = elastic_search.count()
    except exceptions.NotFoundError:
        count = 0

    response = {
        'status': True,
        'message': 'Number of records',
        'result': [count]
    }
    status_code = 200

    elastic.close()
    return response, status_code


def get_metadata_id(platform_code: str, format: str='json'):
    """
    Get the metadata values from the input platform_code (the ID).

    Parameters
    ----------
        platform_code: str
            Platform code
    
    Returns
    -------
        (response, status_code): (dict, int)
            The response is a dict with keys: status, message and result.
                The status is a bool that indicates that data is found.
                The message is a str with comments for the user.
                The result is a list with a dict or a list with a url.
            The status_code is 200 - found, 404 - not found or 503 -
            connection error
    """
    elastic = Elasticsearch(elastic_host)

    try:
        el_response = elastic.get(index=metadata_index, id=platform_code)
        if el_response.get('found'):
            if format == 'json':
                response = {
                    'status': True,
                    'message': 'Metadata information',
                    'result': [{platform_code: el_response['_source']}]
                }
            else:
                filename = f'{csv_folder}/{platform_code}.csv'
                # Check if folder exist
                if not os.path.exists(csv_folder):
                    os.makedirs(csv_folder)

                if not os.path.exists(filename):
                    metadata_dict = el_response['_source']
                    if 'parameters' in metadata_dict:
                        metadata_dict['parameters'] = \
                            (' - ').join(metadata_dict['parameters'])

                    # Convert a dict to a csv
                    df = pd.DataFrame.from_dict([metadata_dict])
                    df.to_csv(filename, index = False, header=True)

                response = {
                    'status': True,
                    'message': 'Metadata information',
                    'result': [f'{csv_url}/{platform_code}.csv']
                }
            status_code = 200
        else:
            abort(404, 'platform_code not found.')
    except exceptions.NotFoundError:
            abort(204, 'platform_code not found.')
    except exceptions.ConnectionError:
        abort(503, 'Connection error with the DB.')

    elastic.close()
    return response, status_code


def get_vocabulary(platform_code: str):
    """
    Get the vocabulary of the parameter acronyms for a given platform_code

    Parameters
    ----------
        platform_code: str
            Platform code
    
    Returns
    -------
        (response, status_code): (dict, int)
            The response is a dict with keys: status, message and result.
                The status is a bool that indicates that data is found.
                The message is a str with comments for the user.
                The result is a list with a dict. The key of the dict is the
                platform_code and the value is the vocabulary.
            The status_code is 200 - found, 204 - not found or 503 -
            connection error
    """
    elastic = Elasticsearch(elastic_host)

    try:
        response = elastic.get(vocabulary_index, platform_code)
        if response.get('found'):
            response = {
                'status': True,
                'message': f'Vocabulary from {platform_code}',
                'result': [{platform_code: response['_source']}]
            }
            status_code = 200
        else:
            abort(204, 'Vocabulary not found')

    except exceptions.NotFoundError:
            abort(204, 'Vocabulary not found')
    except exceptions.ConnectionError:
        abort(503, 'Connection error with the DB')

    elastic.close()
    return response, status_code


def post_data(rule: str, data: dict):
    """
    Add data to the database.

    Parameters
    ----------
        rule: str
            Options - M, 15D, 10D, 6D, 5D, 4D, 3D, 2D, D, 12H, 8H, 6H, 3H, 2H, H
        data: dict
            The keys of data are: 'platform_code', 'parameter', 'time', 'lat',
            'lon', 'depth', 'value', 'qc', 'time_qc', 'lat_qc', 'lon_qc' and
            'deth_qc'
    
    Return
    --------
        (response, status_code): (dict, int)
            The response is a dict with 3 keys:
                'status': True,
                'message': 'Created'
                'result': [{data_id: the_inout_data}]
            The status_code is always 201 (created)
    """
    elastic = Elasticsearch(elastic_host)

    data_id = f"{data['platform_code']}_{data['parameter']}_{data['depth']}" + \
        f"_{data['time'].replace(' ', '_')}"

    elastic.index(index=index_name(rule), id=data_id, document=data)

    response = {
        'status': True,
        'message': 'Created',
        'result': [{data_id: data}]
    }
    status_code = 201
    elastic.close()
    return response, status_code


def delete_data(rule: str, data_id: str):
    """
    Delete data from a given rule and id

    Parameters
    ----------
        rule: str
            Options - M, 15D, 10D, 6D, 5D, 4D, 3D, 2D, D, 12H, 8H, 6H, 3H, 2H, H
        data_id: dict
            Id of the data document
    
    Returns
    -------
        status_code: int
            202 - Deleted.
            404 - Data not found.
            503 - Connection error with the DB
    """
    elastic = Elasticsearch(elastic_host)

    try:
        response = elastic.delete(index=index_name(rule), id=data_id)

        if response['result'] == 'deleted':
            status_code = 202
        else:
            elastic.close()
            abort(404, 'Data not found.')

    except exceptions.NotFoundError:
        elastic.close()
        abort(404, 'Data not found.')
    except exceptions.ConnectionError:
        elastic.close()
        abort(404, 'Connection error with the DB.')
    elastic.close()
    return response, status_code


def post_metadata(platform_code: str, metadata: dict):
    """
    Add the metadata dictionary to the DB. The ID of the document is
    the platform code.

    Parameters
    ----------
        platform_code: str
            Platform code. ID of the document (metadata) to add.
        metadata: dict
            Document to add to the DB.
    
    Returns
    -------
        (response, status_code): (dict, int)
            response is a dictionary with the following keys:
                'status': True,
                'message': 'Added',
                'result': It is a list with a dict. The key of the dict is the
                    input platform_code and the value is the input metadata
            status_code is always 201, (Added)
    """
    # Delete all metadata_map figures
    #Loop Through the folder projects all files and deleting them one by one
    for file_map in glob.glob(f'{fig_folder}/metadata_map*'):
        os.remove(file_map)

    elastic = Elasticsearch(elastic_host)

    elastic.index(index=metadata_index, id=platform_code, document=metadata)

    response = {
        'status': True,
        'message': 'Added',
        'result': [{platform_code: metadata}]
    }
    status_code = 201

    elastic.close()
    return response, status_code


def put_metadata(platform_code, metadata):
    """
    Update a metadata document.

    Parameters
    ----------
        platform_code: str
            Platform code. ID of the document (metadata) to add.
        metadata: dict
            Document to add to elasticsearch.

    Returns
    -------
        (response, status_code): (dict, int)
            response is a dictionary with the following keys:
                'status': True,
                'message': 'Added',
                'result': It is a list with a dict. The key of the dict is the
                    input platform_code and the value is the updated metadata
            status_code is:
                201 - Updated
                204 - Metadata not found
                406 - Bad payload (input metadata)
                503 - Connection error with the DB
    """
    # Delete all metadata_map figures
    #Loop Through the folder projects all files and deleting them one by one
    for file_map in glob.glob(f'{fig_folder}/metadata_map*'):
        os.remove(file_map)


    upload_metadata = {'doc': metadata}

    elastic = Elasticsearch(elastic_host)
    try:
        response = elastic.update(
            index=metadata_index, id=platform_code, body=upload_metadata)
        if response['result'] == 'updated':
            response = elastic.get(index=metadata_index, id=platform_code)
            response = {
                'status': True,
                'message': 'Updated',
                'result': [{platform_code: response['_source']}]
            }
            status_code = 201

        elif response['result'] == 'noop':
            # Comprobar esto
            response = elastic.get(index=metadata_index, id=platform_code)
            response = {
                'status': False,
                'message': 'Metadata not updated',
                'result': [{platform_code: response['_source']}]
            }
            status_code = 204
    except exceptions.NotFoundError:
        response = {
            'status': False,
            'message': 'Platform code not found',
            'result': []
        }
        status_code = 204
    except exceptions.RequestError:
        response =  {
            'status': False,
            'message': 'Not Acceptable. Bad metadata payload',
            'result': []
        }
        status_code = 406
    except exceptions.ConnectionError:
        response =   {
            'status': False,
            'message': 'Internal error. Unable to connect to DB',
            'result': []
        }
        status_code = 503

    elastic.close()
    return response, status_code


def delete_metadata(platform_code: str):
    """
    Delete a metadata document from the DB.
    The input platform_code is the ID of the document.

    Parameters
    ----------
        platform_code: str
            Platform code. ID of the document (metadata) to add.

    Returns
    -------
        status_code: int
            202 - Deleted,
            404 - Metadata not found,
            503 - Connection error with the DB
    """
    elastic = Elasticsearch(elastic_host)

    try:
        response = elastic.delete(index=metadata_index, id=platform_code)
        if response['result'] == 'deleted':
            status_code = 202
    except exceptions.NotFoundError:
        status_code = 404
    except exceptions.ConnectionError:
        status_code = 503

    elastic.close()
    return status_code


def get_doi():
    """
    Update the available DOI numbers of an user
    """
    data, _ = get_token_info(request)
    user = data['result'].get('user_id', 'anonymus')
    try:
        elastic = Elasticsearch(elastic_host)

        # Search for user
        search_body = {
            'query': {
                'match': {
                    'user': user}}}

        response = elastic.search(index='emso-doi', body=search_body)
    except:
        return {
            'status': False,
            'message': 'API cannot access to the DB',
            'result': []
        }, 502

    if response['hits']['hits']:
        num_doi = response['hits']['hits'][0]['_source']['num_doi']
    else:
        num_doi = 0

    return {
        'status': True,
        'message': 'Number of DOIs to create',
        'result': num_doi
        }, 201


def put_doi(user: str, num_doi: int):
    """
    Update the available DOI numbers of an user.

    Parameters
    ----------
        user: str
            User to update the number of available DOIs.
        num_doi: int
            Number of available DOIs.

    Returns
    -------
        (response, status_code): (dict, int)
            response is a dict with the following keys:
                'status': True,
                'message': 'The available DOIs for the user are in result[0]'
                'result': The available DOIs are in result[0]
        status_code is 201.
        If user is not found, the function aborts Flask with:
            204 - User not found.
            503 - Connection error with the DB.

    """
    elastic = Elasticsearch(elastic_host)

    # Search for user
    search_body = {
        'query': {
            'match': {
                'user': user}}}
    try:
        response = elastic.search(index='emso-doi', body=search_body)
        if response['hits']['hits']:
            user_id = response['hits']['hits'][0]['_id']
        else:
            user_id = None

        body = {
            'user': user,
            'num_doi': num_doi
        }

        elastic.index('emso-doi', id=user_id, body=body)
    except exceptions.NotFoundError:
            abort(204, 'User not found')
    except exceptions.ConnectionError:
        abort(503, 'Connection error with the DB')

    response = {
        'status': True,
        'message': 'The available DOIs for the user are in result[0]',
        'result': [num_doi]
        }
    status_code = 201

    return response, status_code


def post_doi(payload):
    """
    Create a DOI
    """

    def create_doi(payload):
        # subtract a DOI value from the user info of elastic
        # Get the user
        data, _ = get_token_info(request)
        user = data['result'].get('user_id', 'anonymus')
        # Get the DOI number
        response, status_code = get_doi()
        doy_number = int(response['result'])
        # Update the DOI number
        response, status_code = put_doi(user, doy_number - 1)
        # TODO: AQUI HAY QUE COMPROBAR QUE EL STATUS_CODE ES 201
        response = requests.post(
            'https://api.test.datacite.org/dois',
            headers={'Content-Type': 'application/vnd.api+json'},
            data=payload,
            auth=('emso.emso', 'cmo@EMSO!'))
        if response.status_code == 201:

            return {
                'status': True,
                'message': 'DOI correctly generated',
                'result': json.loads(response.text)['data']['id']
            }, 201

        else:
            try:
                message = response.json()['errors'][0]['title']
            except:
                message = 'Invalid DOI payload'
            # message = payload.json()['errors'][0]['title']
            return {
                'status': False,
                'message': message,
                'result': []
            }, 422


    payload = json.loads(json.dumps(payload))

    fixed_attributes = {
        "event": "publish",
        "prefix": "10.80110",
        "language": "en",
        "isActive": True,
        "metadataVersion": "1.0.0",
        "schemaVersion": "http://datacite.org/schema/kernel-4",
        # "publisher": "European Multidisciplinary Seafloor and water-column " + \
        #     "Observatory (EMSO)",
        "publicationYear": datetime.datetime.now().year
    }

    final_payload = {
        "data": {
            "type": "dois",
            "attributes": {
                "event": "",
                "prefix": "",
                "language": "",
                "isActive": True,
                "metadataVersion": "",
                "schemaVersion": "",
                "publisher": "",
                "publicationYear": None
            }
        }
    }

    final_payload["data"]["attributes"].update(payload)
    final_payload["data"]["attributes"].update(fixed_attributes)

    # Check if required keys exist:
    for key in ["url", "creators", "titles", "types"]:
        if key not in payload:
            raise KeyError(
                'Required key "' + key + '" does not exist on the DOI payload.')

    # Check if requried values are correctly configured
    for key, value in final_payload["data"]["attributes"].items():
        if key == "url":
            if len(value) <= 0:
                raise KeyError('Resource URL must be filled correctly.')

        if key == "creators":
            if len(value) == 0:
                raise KeyError('At least one creator must be added.')
            for creator in value:
                if len(creator["name"]) == 0:
                    return {
                        'status': False,
                        'message': 'Creator name must be filled correctly.',
                        'result': []
                    }, 400
                if creator["nameType"] not in ["Organizational", "Personal"]:
                    return {
                        'status': False,
                        'message': 'Creator nameType must be "Organizational" or "Personal".',
                        'result': []
                    }, 400

        if key == "titles":
            if len(value) == 0:
                return {
                    'status': False,
                    'message': 'At least one title must be added.',
                    'result': []
                }, 400
            for i, title in enumerate(value):
                if len(title["title"]) == 0:
                    return {
                        'status': False,
                        'message': 'Title must be filled correctly.',
                        'result': []
                    }, 400
                if i != 0 and title["titleType"] not in ["AlternativeTitle",
                                                         "Subtitle",
                                                         "TranslatedTitle",
                                                         "Other"]:
                    return {
                        'status': False,
                        'message': 'TitleType must be "AlternativeTitle", "Subtitle", "TranslatedTitle" or "Other".',
                        'result': []
                    }, 400

        if key == "types":
            if value["resourceTypeGeneral"] not in ["Audiovisual", "Book",
                                                    "BookChapter", "Collection",
                                                    "ComputationalNotebook",
                                                    "ConferencePaper",
                                                    "ConferenceProceeding",
                                                    "DataPaper", "Dataset",
                                                    "Dissertation", "Event",
                                                    "Image",
                                                    "InteractiveResource",
                                                    "Model", "PeerReview",
                                                    "Report", "Software",
                                                    "Sound", "Standard", "Text",
                                                    "Workflow", "Other"]:
                return {
                    'status': False,
                    'message': 'ResourceTypeGeneral must be "Audiovisual", "Book", "BookChapter", "Collection", "ComputationalNotebook", "ConferencePaper", "ConferenceProceeding", "DataPaper", "Dataset", "Dissertation", "Event", "Image", "InteractiveResource", "Model", "PeerReview", "Report", "Software", "Sound", "Standard", "Text", "Workflow", "Other".',
                    'result': []
                }, 400

            if len(value["resourceType"]) == 0:
                return {
                    'status': False,
                    'message': 'ResourceType must be filled correctly.',
                    'result': []
                }, 400

    return create_doi(json.dumps(final_payload))


def get_pid(email):
    """
    Get PIDs from email
    """
    elastic = Elasticsearch(elastic_host)

    # Search for user
    search_body = {
        'size': 1000,
        'query': {
            'match': {
                'email': email}}}

    try:
        response = elastic.search(index='emso-pid', body=search_body)
    except exceptions.NotFoundError:
        # Index does not exist because is empty
        return {
            'status': False,
            'message': f'Email {email} not found',
            'result': []
        }, 204
    pid_list = []
    if response['hits']['hits']:
        
        for hit in response['hits']['hits']:
            try:
                if hit['_source']['email'] == email:
                    new_pid = {}
                    new_pid['pid'] = hit['_source'].get('pid')
                    new_pid['email'] = hit['_source'].get('email')
                    new_pid['description'] = hit['_source'].get('description')
                    new_pid['resource'] = hit['_source'].get('resource')
                    new_pid['url_pid'] = hit['_source'].get('url_pid')
                    pid_list.append(new_pid)
            except KeyError:
                pass

    if pid_list:
        return {
            'status': True,
            'message': f'List of files from {email}',
            'result': pid_list
        }, 201
    else:
        return {
            'status': False,
            'message': f'Email {email} not found',
            'result': []
        }, 204


def add_values_v2(payload, mail, DOI=False):
    PID = hash.md5(repr(payload).encode()).hexdigest()
    payload = json.loads(json.dumps(payload))

    # TAGS:
    html = ET.Element('html')
    head = ET.Element('head')
    style = ET.Element('style')
    body = ET.Element('body')
    principal_div = ET.Element('div', attrib={'class': 'principal'})
    toolbar_div = ET.Element('div', attrib={'class': 'toolbar'})
    row1_div = ET.Element('div', attrib={'class': 'row1'})
    row2_div = ET.Element('div', attrib={'class': 'row2'})
    row3_div = ET.Element('div', attrib={'class': 'row3'})
    emso_a = ET.Element('a',
                        attrib={'target': '_blank', 'href': 'http://emso.eu/', 'target': '_blank',
                                'href': 'https://data.emso.eu/home'})
    emso_logo = ET.Element('img', attrib={'src': 'http://emso.eu/wp-content/uploads/2018/03/logo-w-150.png'})
    title_div = ET.Element('div', attrib={'class': 'title'})
    title = ET.Element('h1', attrib={'style': 'padding-top: 1em;'})
    subtitle = ET.Element('h2')
    hr = ET.Element('hr')
    content_div = ET.Element('div', attrib={'class': 'content'})
    link_div = ET.Element('div', attrib={'style': 'margin-left: 3em; padding-top: 1em;'})
    link_h3 = ET.Element('h3')
    link_a = ET.Element('a',
                        attrib={'href': payload["resource"],
                                "target": "_blank"})
    information = ET.Element('h3', attrib={'style': 'margin-left: 3em; padding-top: 1em;'})
    table = ET.Element('table', attrib={'class': 'styled-table'})
    thead = ET.Element('thead')
    thead_tr = ET.Element('tr')
    thead_th1 = ET.Element('th', attrib={'style': 'border-right: 2px solid rgb(255, 255, 255)'})
    thead_th2 = ET.Element('th')
    tbody = ET.Element('tbody')

    version_tr = ET.Element('tr', attrib={'class': 'active-row'})
    version_td1 = ET.Element('td')
    version_td2 = ET.Element('td')

    user_tr = ET.Element('tr')
    user_td1 = ET.Element('td')
    user_td2 = ET.Element('td')

    mail_tr = ET.Element('tr', attrib={'class': 'active-row'})
    mail_td1 = ET.Element('td')
    mail_td2 = ET.Element('td')

    description_tr = ET.Element('tr')
    description_td1 = ET.Element('td')
    description_td2 = ET.Element('td')

    name_identifier_tr = ET.Element('tr', attrib={'class': 'active-row'})
    name_identifier_td1 = ET.Element('td')
    name_identifier_td2 = ET.Element('td')

    identifier_schema_tr = ET.Element('tr')
    identifier_schema_td1 = ET.Element('td')
    identifier_schema_td2 = ET.Element('td')

    affiliation_tr = ET.Element('tr', attrib={'class': 'active-row'})
    affiliation_td1 = ET.Element('td')
    affiliation_td2 = ET.Element('td')

    affiliation_identifier_tr = ET.Element('tr')
    affiliation_identifier_td1 = ET.Element('td')
    affiliation_identifier_td2 = ET.Element('td')

    affiliation_scheme_tr = ET.Element('tr', attrib={'class': 'active-row'})
    affiliation_scheme_td1 = ET.Element('td')
    affiliation_scheme_td2 = ET.Element('td')

    title_tr = ET.Element('tr')
    title_td1 = ET.Element('td')
    title_td2 = ET.Element('td')

    description_additional_tr = ET.Element('tr', attrib={'class': 'active-row'})
    description_additional_td1 = ET.Element('td')
    description_additional_td2 = ET.Element('td')

    minting_div = ET.Element('div', attrib={'class': 'minting'})
    minting_h3 = ET.Element('h4')
    minting_a = ET.Element('a',
                           attrib={'class': 'doiMinting', 'target': '_blank',
                                   'href': 'https://data.emso.eu/doi-minting?href=' + PID})

    # Append elements
    html.append(head)
    head.append(style)
    html.append(body)
    body.append(principal_div)
    principal_div.append(toolbar_div)
    toolbar_div.append(row1_div)
    row1_div.append(emso_a)
    emso_a.append(emso_logo)
    toolbar_div.append(row2_div)
    toolbar_div.append(row3_div)
    principal_div.append(content_div)
    content_div.append(title_div)
    title_div.append(title)
    title_div.append(subtitle)
    content_div.append(hr)
    content_div.append(link_div)
    link_div.append(link_h3)
    link_div.append(link_a)
    content_div.append(information)
    content_div.append(table)
    table.append(thead)
    thead.append(thead_tr)
    thead_tr.append(thead_th1)
    thead_tr.append(thead_th2)
    table.append(tbody)
    tbody.append(version_tr)
    version_tr.append(version_td1)
    version_tr.append(version_td2)
    tbody.append(user_tr)
    user_tr.append(user_td1)
    user_tr.append(user_td2)
    tbody.append(mail_tr)
    mail_tr.append(mail_td1)
    mail_tr.append(mail_td2)
    tbody.append(description_tr)
    description_tr.append(description_td1)
    description_tr.append(description_td2)
    tbody.append(name_identifier_tr)
    name_identifier_tr.append(name_identifier_td1)
    name_identifier_tr.append(name_identifier_td2)
    tbody.append(identifier_schema_tr)
    identifier_schema_tr.append(identifier_schema_td1)
    identifier_schema_tr.append(identifier_schema_td2)
    tbody.append(affiliation_tr)
    affiliation_tr.append(affiliation_td1)
    affiliation_tr.append(affiliation_td2)
    tbody.append(affiliation_identifier_tr)
    affiliation_identifier_tr.append(affiliation_identifier_td1)
    affiliation_identifier_tr.append(affiliation_identifier_td2)
    tbody.append(affiliation_scheme_tr)
    affiliation_scheme_tr.append(affiliation_scheme_td1)
    affiliation_scheme_tr.append(affiliation_scheme_td2)

    if payload.get('titles'):
        tbody.append(title_tr)
        title_tr.append(title_td1)
        title_tr.append(title_td2)
        if len(payload["titles"]) == 1:
            title_td1.text = "Title"
            title_td2.text = payload["titles"][0]["title"]
        else:
            titles = []
            types = []
            for t in payload["titles"]:
                titles.append(t["title"])
                types.append(t["titleType"])
            title_td1.text = ', '.join(types)
            title_td2.text = ', '.join(titles)

    if payload.get("descriptions"):
        tbody.append(description_additional_tr)
        description_additional_tr.append(description_additional_td1)
        description_additional_tr.append(description_additional_td2)
        if len(payload["descriptions"]) == 1:
            description_additional_td1.text = payload["descriptions"][0]["descriptionType"] + " description"
            description_additional_td2.text = payload["descriptions"][0]["description"]
        else:
            descriptions = []
            types = []
            for d in payload["descriptions"]:
                descriptions.append(d["description"])
                types.append(d["descriptionType"])
            description_additional_td1.text = ', '.join(types)
            description_additional_td2.text = ', '.join(descriptions)

    if DOI:
        content_div.append(minting_div)
        minting_div.append(minting_h3)
        minting_h3.append(minting_a)

    style.text = """body {
            background-color: #f4f8f9;
        }

        .principal {
            background-color: #f4f8f9;
            width: 100%;
            height: max-content;
            top: 0;
            left: 0;
            overflow-x: hidden;
            position: absolute;
        }

        .title {
            text-align: center;
            margin-top: 115px;
        }

        .toolbar {
            top: 0;
            background-color: rgb(188, 194, 194);
            min-height: 115px;
            max-height: 115px;
            align-content: center;
            display: flex;
            position: fixed;
            width: 100%;
        }

        .row1 {
            padding-left: 15%;
            margin-top: 0.5vh;
            width: 33%;
            min-height: fit-content;
            text-align: left;

        }

        .row2 {
            margin-top: 4vh;
            width: 33%;
            min-height: fit-content;
            text-align: center;

        }

        .row3 {
            padding-right: 15%;
            width: 33%;
            text-align: right;
            margin-top: 4vh;
        }

        .version {
            color: rgb(0, 0, 0);
            font-weight: 700;
        }

        .content {
            background-color: white;
            width: 70%;
            margin-left: 15%;
        }

        .doiMinting {
            font-weight: 400;
            font-size: 1em;
            text-decoration: none;
            width: max-content;
            padding: 15px 25px;
            text-align: center;
            cursor: pointer;
            outline: none;
            color: #fff;
            background-color: rgb(24, 128, 30, 0.8);
            border-radius: 8px;
        }

        .doiMinting:hover {
            background-color: rgb(24, 128, 30, 0.4)
        }

        .minting {
            margin-top: 3em;
            text-align: center;
            padding-bottom: 3em;
        }

        .padding {
            margin-top: 3em;
        }

        .styled-table {
            border-collapse: collapse;
            margin: 25px 10%;
            font-size: 1em;
            font-family: sans-serif;
            min-width: 80%;
            align-items: center;
            box-shadow: 0 0 20px rgba(0, 0, 0, 0.15);
        }

        .styled-table thead tr {
            background-color: rgb(24, 128, 30, 0.8);
            color: #ffffff;
            text-align: left;
            padding: 0.5em;
        }

        td {
            padding: 10px;
            border-left: 1px solid rgb(24, 128, 30, 0.8);
            border-right: 1px solid rgb(24, 128, 30, 0.8);
        }

        th {
            padding: 15px;
            font-size: 1em;
            text-align: center;
        }

        .styled-table tbody tr {
            border-bottom: 1px solid #dddddd;
        }

        .styled-table tbody tr:nth-of-type(even) {
            background-color: #f3f3f3;
        }

        .styled-table tbody tr:last-of-type {
            border-bottom: 2px solid rgb(24, 128, 30, 0.8);
        }

        .styled-table tbody tr.active-row {
            font-weight: bold;
            color: rgb(24, 128, 30, 0.8)rgb(24, 128, 30, 0.8);
        }
    """

    title.text = "Created PID"
    subtitle.text = PID
    link_h3.text = "Link to the data:"
    link_a.text = payload["resource"]
    information.text = "Information:"
    thead_th1.text = "User Claim"
    thead_th2.text = "Value"
    version_td1.text = 'Data portal version'
    version_td2.text = payload["version"]
    user_td1.text = "User"
    user_td2.text = payload["creators"][0]["name"]
    mail_td1.text = "Mail"
    mail_td2.text = mail
    description_td1.text = "Description"
    description_td2.text = payload["resourceTypeGeneral"]
    name_identifier_td1.text = "Name identifier"
    name_identifier_td2.text = payload["creators"][0]["nameIdentifiers"][0]["nameIdentifier"]
    identifier_schema_td1.text = "Identifier schema"
    identifier_schema_td2.text = payload["creators"][0]["nameIdentifiers"][0]["nameIdentifierScheme"]
    affiliation_td1.text = "Affiliation"
    affiliation_td2.text = payload["creators"][0]["affiliation"][0]["name"]
    affiliation_identifier_td1.text = "Affiliation identifier"
    affiliation_identifier_td2.text = payload["creators"][0]["affiliation"][0][
        "affiliationIdentifier"]
    affiliation_scheme_td1.text = "Affiliation scheme"
    affiliation_scheme_td2.text = payload["creators"][0]["affiliation"][0][
        "affiliationIdentifierScheme"]

    if DOI:
        minting_a.text = "Create DOI from PID"

    tree = ET.tostring(html, encoding='unicode', method='html')
    sha256 = hash.sha256(tree.encode('utf-8')).hexdigest()
    year = datetime.datetime.now().year

    _file_path = files_folder + '/' + str(year) + '/' + sha256 + '.html'
    os.makedirs(os.path.dirname(_file_path), exist_ok=True)

    # with open(_file_path, 'w+') as f:
    #     f.write(tree)

    tree = ET.tostring(html, encoding='unicode', method='html')
    # sha256 = hash.sha256(tree.encode('utf-8')).hexdigest()

    # with open(file_path + '/' + str(year) + '/' + sha256 + '.html', 'w+') as f:
    #     f.write(tree)

    filename = files_folder + '/' + str(year) + '/' + PID + '.html'
    with open(filename, 'w+') as f:
        f.write(tree)

    url_pid = files_url +  '/' + str(year) + '/' + PID + '.html'

    # Guarda el filename en el elastic
    elastic = Elasticsearch(elastic_host)
    body = {
        'email': mail,
        'filename': url_pid
    }
    elastic.index('emso-pid', body=body)

    return {
        'status': True,
        'message': 'Success',
        'result': url_pid
    }, 201


def get_query_id(query_id: str):
    """
    Get the content of a query_id.

    Parameters
    ----------
        query_id: str
            ID from the query.

    Returns
    -------
        (response, status_code): (dict, int)
            The response is a dict with keys:
                'status': True,
                'message': 'The result is a list with a dict containing a 
                    key (query_id) and a value (the content)',
                'result': A list of dicts
    """
    elastic = Elasticsearch(elastic_host)

    try:
        response = elastic.get(api_index, query_id)
        if response.get('found'):
            response = {
                'status': True,
                'message': 'The result is a list with a dict containing a ' + \
                    'key (query_id) and a value (the content)',
                'result': [{query_id: response['_source']}]
            }
            status_code = 200
        else:
            abort(404, 'id_queryid not found')
    
    except exceptions.NotFoundError:
        abort(404, 'query_id not found')
    except exceptions.ConnectionError:
        abort(503, 'Connection error with the DB')

    elastic.close()
    return response, status_code


def get_query(user_id: str, namespace: str=None):
    """
    Returns the previous API requests from the input user_id.

    Parameters
    ----------
        user_id: str
            User to search in the DB.
        namestace: str
            Namespace to search in the DB.

    Returns
    -------
        (response, status_code): (dict, int)
            response contains the following keys:
                'status': True, or False if the list is empty.
                'message': List of queries from user {user_id}
                'result': List with the previous API requests.
            The status_code is 200.
        The function can abort the request with a code:
            503 - Connection error with the DB
    """
    elastic = Elasticsearch(elastic_host)
    
    search_body = {}
    search_body['query'] = {'match': {'user': user_id}}

    try:
        elastic_search = Search(
            using=elastic, index=api_index).update_from_dict(search_body)

        elastic_search = elastic_search.source(['url'])  # only get url
        ids = []
        for h in elastic_search.scan():
            url = h.to_dict()['url']
            if namespace:
                if namespace in url:
                    ids.append(h.meta.id)
            else:
                ids.append(h.meta.id)

        if ids:
            response = {
                'status': True,
                'message': f'List of queries from user {user_id}',
                'result': ids
            }
            status_code = 200
        else:
            response = {
                'status': False,
                'message': f'List of queries from user {user_id}- empty',
                'result': ids
            }
            status_code = 200
    except exceptions.ConnectionError:
        abort(503, "Connection error with the DB")

    elastic.close()
    return response, status_code


def post_vocabulary(platform_code: str, vocabulary: dict):
    """
    Add the vocabulary dictionary to the DB. The ID of the document is
    the platform code.

    Parameters
    ----------
        platform_code: str
            Platform code. ID of the document (metadata) to add.
        vocabulary: dict
            Document to add to the DB.
    
    Returns
    -------
        (response, status_code): (dict, int)
            response is a dictionary with the following keys:
                'status': True,
                'message': 'Added',
                'result': It is a list with a dict. The key of the dict is the
                    input platform_code and the value is the input vocabulary
            status_code is always 201, (Added)
    """
    elastic = Elasticsearch(elastic_host)

    elastic.index(index=vocabulary_index, id=platform_code, document=vocabulary)

    response = {
        'status': True,
        'message': 'Added',
        'result': [{platform_code: vocabulary}]
    }
    status_code = 201

    elastic.close()
    return response, status_code


def put_vocabulary(platform_code, vocabulary):
    """
    Update a vocabulary document.

    Parameters
    ----------
        platform_code: str
            Platform code. ID of the document (vocabulary) to add.
        vocabulary: dict
            Document to add to elasticsearch.

    Returns
    -------
        (response, status_code): (dict, int)
            response is a dictionary with the following keys:
                'status': True,
                'message': 'Added',
                'result': It is a list with a dict. The key of the dict is the
                    input platform_code and the value is the updated vocabulary
            status_code is:
                201 - Updated
                204 - Vocabulary not found
                406 - Bad payload (input vocabulary)
                503 - Connection error with the DB
    """


    upload_vocabulary = {'doc': vocabulary}

    elastic = Elasticsearch(elastic_host)
    try:
        response = elastic.update(
            index=vocabulary_index, id=platform_code, body=upload_vocabulary)
        if response['result'] == 'updated':
            response = elastic.get(index=vocabulary_index, id=platform_code)
            response = {
                'status': True,
                'message': 'Updated',
                'result': [{platform_code: response['_source']}]
            }
            status_code = 201

        elif response['result'] == 'noop':
            # Comprobar esto
            response = elastic.get(index=vocabulary_index, id=platform_code)
            response = {
                'status': False,
                'message': 'Metadata not updated',
                'result': [{platform_code: response['_source']}]
            }
            status_code = 204
    except exceptions.NotFoundError:
        response = {
            'status': False,
            'message': 'Platform code not found',
            'result': []
        }
        status_code = 204
    except exceptions.RequestError:
        response =  {
            'status': False,
            'message': 'Not Acceptable. Bad metadata payload',
            'result': []
        }
        status_code = 406
    except exceptions.ConnectionError:
        response =   {
            'status': False,
            'message': 'Internal error. Unable to connect to the DB',
            'result': []
        }
        status_code = 503

    elastic.close()
    return response, status_code


def delete_vocabulary(platform_code: str):
    """
    Delete a vocabulary document from the DB.
    The input platform_code is the ID of the document.

    Parameters
    ----------
        platform_code: str
            Platform code. ID of the document (vocabulary) to add.

    Returns
    -------
        status_code: int
            202 - Deleted,
            404 - Vocabulary not found,
            503 - Connection error with the DB
    """
    elastic = Elasticsearch(elastic_host)

    try:
        response = elastic.delete(index=vocabulary_index, id=platform_code)
        if response['result'] == 'deleted':
            status_code = 202
    except exceptions.NotFoundError:
        status_code = 404
    except exceptions.ConnectionError:
        status_code = 503

    elastic.close()
    return status_code


def post_pid(payload, mail, DOI=False):
    """
    Add a PID to the DB.

    Parameters
    ----------
        payload: dict
            Payload to add to the DB.
        mail: str
            Mail of the user who is adding the PID.
        DOI: bool
            If True, the PID is a DOI.

    Returns
    -------
        (response, status_code): (dict, int)
            response is a dictionary with the following keys:
                'status': True,
                'message': 'Added',
                'result': It is a list with a dict. The key of the dict is the
                    input platform_code and the value is the input vocabulary
            status_code is:
                201 - Added
                406 - Bad payload (input vocabulary)
                503 - Connection error with the DB
    """

    PID = hash.md5(repr(payload).encode()).hexdigest()
    payload = json.loads(json.dumps(payload))

    # TAGS:
    html = ET.Element('html')
    head = ET.Element('head')
    style = ET.Element('style')
    body = ET.Element('body')
    principal_div = ET.Element('div', attrib={'class': 'principal'})
    toolbar_div = ET.Element('div', attrib={'class': 'toolbar'})
    row1_div = ET.Element('div', attrib={'class': 'row1'})
    row2_div = ET.Element('div', attrib={'class': 'row2'})
    row3_div = ET.Element('div', attrib={'class': 'row3'})
    emso_a = ET.Element('a', attrib={
        'target': '_blank',
        'href': 'http://emso.eu/',
        'target': '_blank',
        'href': 'https://data.emso.eu/home'})
    emso_logo = ET.Element('img', attrib={
        'src': 'http://emso.eu/wp-content/uploads/2018/03/logo-w-150.png'})
    title_div = ET.Element('div', attrib={'class': 'title'})
    title = ET.Element('h1', attrib={'style': 'padding-top: 1em;'})
    subtitle = ET.Element('h2')
    hr = ET.Element('hr')
    content_div = ET.Element('div', attrib={'class': 'content'})
    table = ET.Element('table', attrib={'class': 'styled-table'})
    thead = ET.Element('thead')
    thead_tr = ET.Element('tr')
    thead_th1 = ET.Element('th', attrib={
        'style': 'border-right: 2px solid rgb(255, 255, 255)'})
    thead_th2 = ET.Element('th')
    tbody = ET.Element('tbody')

    # Append elements
    html.append(head)
    head.append(style)
    html.append(body)
    body.append(principal_div)
    principal_div.append(toolbar_div)
    toolbar_div.append(row1_div)
    row1_div.append(emso_a)
    emso_a.append(emso_logo)
    toolbar_div.append(row2_div)
    toolbar_div.append(row3_div)
    principal_div.append(content_div)
    content_div.append(title_div)
    title_div.append(title)
    title_div.append(subtitle)
    content_div.append(hr)
    content_div.append(table)
    table.append(thead)
    thead.append(thead_tr)
    thead_tr.append(thead_th1)
    thead_tr.append(thead_th2)
    table.append(tbody)

    title.text = "EMSO ERIC Persistent IDentifier (alfa version)"
    subtitle.text = "PID: " + PID
    thead_th1.text = "User Claim"
    thead_th2.text = "Value"

    # Values:
    show_values = {
        "resource": "Resource",
        "resourceTypeGeneral": "Resource description",
        "version": "Version",
        "creators": "Creator",
        "name": "Full name",
        "givenName": "First name",
        "familyName": "Surname",
        "nameType": "Type",
        "nameIdentifiers": "Name identifier",
        "nameIdentifier": "Identifier",
        "nameIdentifierScheme": "Scheme",
        "schemeUri": "Scheme URI",
        "affiliation": "Affiliation",
        "affiliationIdentifier": "Identifier",
        "affiliationIdentifierScheme": "Scheme",
        "titles": "Title",
        "title": "Value",
        "titleType": "Type",
        "lang": "Language",
        "descriptions": "Description",
        "description": "Value",
        "descriptionType": "Type"
    }

    show_indexes = {
        1: 'First',
        2: 'Second',
        3: 'Third',
        4: 'Fourth',
        5: 'Fifth',
        6: 'Sixth',
        7: 'Seventh',
        8: 'Eighth',
        9: 'Ninth',
        10: 'Tenth',
        11: 'Eleventh',
        12: 'Twelfth',
        13: 'Thirteenth',
        14: 'Fourteenth',
        15: 'Fifteenth',
        16: 'Sixteenth',
        17: 'Seventeenth',
        18: 'Eighteenth',
        19: 'Nineteenth',
        20: 'Twentieth'
    }

    # Check if required keys and values exist:
    required = ["resource", "resourceTypeGeneral", "version", "creators"]
    optional = ["titles", "descriptions"]
    for key in required:
        if key not in payload:
            raise KeyError(
                'Required key "' + key + '" does not exist on the PID payload.')
        else:
            if key != 'creators' and len(payload[key]) == 0:
                raise ValueError('Key"' + key + '" does not have a value.')
            if key == 'creators' and not payload[key]:
                raise ValueError('No creators added, minimum one required.')

    for key, value in payload.items():

        if isinstance(value, str) and key in required:
            claim_tr = ET.Element('tr', attrib={'class': 'entrance'})
            claim_td1 = ET.Element('td')
            claim_td2 = ET.Element('td')

            tbody.append(claim_tr)
            claim_tr.append(claim_td1)
            claim_tr.append(claim_td2)

            claim_td1.text = show_values[key]
            if show_values[key] == 'Resource':
                claim_td2_a = ET.Element('a', attrib={'href': value, 'target': '_blank'})
                claim_td2_a.text = value
                claim_td2.append(claim_td2_a)
            else:
                claim_td2.text = value
        elif isinstance(
            value, list) and len(value) >= 1 and (
                key in optional or key in required):
            index = 1
            for entrance in value:
                claim_tr = ET.Element('tr', attrib={'class': 'entrance'})
                claim_td1 = ET.Element('td', attrib={
                    'class': 'multiple', 'colspan': "2"})

                tbody.append(claim_tr)
                claim_tr.append(claim_td1)
                if len(value) == 1:
                    claim_td1.text = show_values[key]
                else:
                    try:
                        claim_td1.text = show_indexes[index] + ' ' + \
                            show_values[key]
                    except:
                        claim_td1.text = str(index) + ' ' + show_values[key]
                if isinstance(entrance, dict):
                    for i, j in entrance.items():
                        if isinstance(j, list) and len(j) >= 1:
                            index_2 = 1
                            for val in j:
                                if isinstance(val, dict):
                                    claim_tr = ET.Element('tr', attrib={
                                        'class': 'active-row'})
                                    claim_td1 = ET.Element('td', attrib={
                                        'class': 'tab multiple_2',
                                        'colspan': "2"})

                                    tbody.append(claim_tr)
                                    claim_tr.append(claim_td1)

                                    if len(j) == 1:
                                        claim_td1.text = show_values[i]
                                    else:
                                        try:
                                            claim_td1.text = \
                                                show_indexes[index_2] + ' ' + \
                                                show_values[i]
                                        except:
                                            claim_td1.text = str(index_2) + \
                                                ' ' + show_values[i]

                                    for k, l in val.items():
                                        claim_tr = ET.Element('tr')
                                        claim_td1 = ET.Element('td', attrib={
                                            'class': 'tab_2'})
                                        claim_td2 = ET.Element('td')

                                        tbody.append(claim_tr)
                                        claim_tr.append(claim_td1)
                                        claim_tr.append(claim_td2)

                                        claim_td1.text = show_values[str(k)]
                                        claim_td2.text = str(l)
                                    index_2 += 1
                        else:
                            claim_tr = ET.Element('tr')
                            claim_td1 = ET.Element('td', attrib={
                                'class': 'tab'})
                            claim_td2 = ET.Element('td')

                            tbody.append(claim_tr)
                            claim_tr.append(claim_td1)
                            claim_tr.append(claim_td2)

                            claim_td1.text = show_values[str(i)]
                            claim_td2.text = str(j)
                    index += 1

    style.text = """body {
               background-color: #f4f8f9;
           }

           .principal {
               background-color: #f4f8f9;
               width: 100%;
               height: max-content;
               top: 0;
               left: 0;
               overflow-x: hidden;
               position: absolute;
           }

           .title {
               text-align: center;
               margin-top: 115px;
           }

           .toolbar {
               top: 0;
               background-color: rgb(188, 194, 194);
               min-height: 115px;
               max-height: 115px;
               align-content: center;
               display: flex;
               position: fixed;
               width: 100%;
           }

           .row1 {
               padding-left: 15%;
               margin-top: 0.5vh;
               width: 33%;
               min-height: fit-content;
               text-align: left;

           }

           .row2 {
               margin-top: 4vh;
               width: 33%;
               min-height: fit-content;
               text-align: center;

           }

           .row3 {
               padding-right: 15%;
               width: 33%;
               text-align: right;
               margin-top: 4vh;
           }

           .version {
               color: rgb(0, 0, 0);
               font-weight: 700;
           }

           .content {
               background-color: white;
               width: 70%;
               margin-left: 15%;
           }

           .doiMinting {
               font-weight: 400;
               font-size: 1em;
               text-decoration: none;
               width: max-content;
               padding: 15px 25px;
               text-align: center;
               cursor: pointer;
               outline: none;
               color: #fff;
               background-color: rgb(24, 128, 30, 0.8);
               border-radius: 8px;
           }

           .doiMinting:hover {
               background-color: rgb(24, 128, 30, 0.4)
           }

           .minting {
               margin-top: 3em;
               text-align: center;
               padding-bottom: 3em;
           }

           .padding {
               margin-top: 3em;
           }

           .styled-table {
               border-collapse: collapse;
               margin: 25px 10%;
               font-size: 1em;
               font-family: sans-serif;
               min-width: 80%;
               align-items: center;
               box-shadow: 0 0 20px rgba(0, 0, 0, 0.15);
           }

           .styled-table thead tr {
               background-color: rgb(24, 128, 30, 0.8);
               color: #ffffff;
               text-align: left;
               padding: 0.5em;
           }

           td {
               padding: 10px;
               border-left: 1px solid rgb(24, 128, 30, 0.8);
               border-right: 1px solid rgb(24, 128, 30, 0.8);
           }

           th {
               padding: 15px;
               font-size: 1em;
               text-align: center;
           }

           .styled-table tbody tr {
               border-bottom: 1px solid #dddddd;
           }

           .styled-table tbody tr:nth-of-type(even) {
               background-color: #f3f3f3;
           }

           .styled-table tbody tr:last-of-type {
               border-bottom: 2px solid rgb(24, 128, 30, 0.8);
           }

           .styled-table tbody tr.active-row {
               font-weight: bold;
               color: rgb(24, 128, 30, 0.8)rgb(24, 128, 30, 0.8);
           }

            .styled-table tbody tr.entrance {
               font-weight: bold;
               color: rgb(24, 128, 30, 0.8)rgb(24, 128, 30, 0.8);
               font-size: larger;
            }
            .tab {
                padding-left: 2em
            }  
            .tab_2 {
                padding-left: 4em
            }
            .multiple {
                background-color: aliceblue;
            }   
            .multiple_2 {
                background-color: azure;
            } 

           
       """

    tree = ET.tostring(html, encoding='unicode', method='html')
    sha256 = hash.sha256(tree.encode('utf-8')).hexdigest()
    year = datetime.datetime.now().year

    file_path = pid_folder + '/' + str(year) + '/' + sha256 + '.html'
    os.makedirs(os.path.dirname(file_path), exist_ok=True)

    filename = pid_folder + '/' + str(year) + '/' + PID + '.html'
    with open(filename, 'w+') as f:
        f.write(tree)

    url_pid = pid_url +  '/' + str(year) + '/' + PID + '.html'

    # Save the PID into the elasticsearch
    elastic = Elasticsearch(elastic_host)
    body = {
        'email': mail,
        'pid': PID,
        'url_pid': url_pid,
        'resource': payload['resource'],
        'description': payload['resourceTypeGeneral'],
    }
    elastic.index('emso-pid', body=body)

    return {
        'status': True,
        'message': 'Success',
        'result': url_pid
    }, 201
