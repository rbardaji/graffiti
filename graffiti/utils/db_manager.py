import os
import pandas as pd

from elasticsearch import Elasticsearch, exceptions
from elasticsearch_dsl import Search, A

from .helper import index_name

from config import (elastic_host, data_index_r, data_index_h, data_index_2h,
                    data_index_3h, data_index_6h, data_index_8h, data_index_12h,
                    data_index_d, data_index_2d, data_index_3d, data_index_4d,
                    data_index_5d, data_index_6d, data_index_10d,
                    data_index_15d, data_index_m, max_plot_points, df_folder,
                    metadata_index)


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
    try:
        elastic = Elasticsearch(elastic_host)
        elastic.index(index=index_name, body=data)

        status = 201
    except exceptions.ConnectionError:
        status = 500
    
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
            The status_code is 200 (found) or 204 (not found).
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
        response = {
            'status': False,
            'message': 'List of data records - empty',
            'result': ids
        }
        status_code = 204

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


def get_df(platform_code, parameter, rule, depth_min=None, depth_max=None,
           time_min=None, time_max=None, qc=None):
    """
    Get data from the database and make a pandas DataFrame.

    Parameters
    ----------
        platform_code: str
            Platform code
        parameter: str
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
    df_name = f'{platform_code}_{parameter}_{rule}_dmin{depth_min}' + \
        f'_dmax{depth_max}_tmin{time_min}_tmax{time_max}_qc{qc}'

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
            df = pd.DataFrame(data)
            df = df.sort_values(by='time')
            df.to_pickle(f'{df_folder}/{df_name}.pkl')
        else:
            df = pd.DataFrame()
        return df

    df = pd.read_pickle(f'{df_folder}/{df_name}.pkl')
    return df


def get_metadata():
    """
    Get a list of the avialable platform_codes (the ID of the metadata resurces)

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

    try:
        elastic_search = Search(using=elastic, index=metadata_index)

        elastic_search = elastic_search.source([])  # only get ids
        ids = [h.meta.id for h in elastic_search.scan()]
        elastic.close()
        if ids:
            response = {
                'status': True,
                'message': 'List of platform codes',
                'result': ids
            }
            status_code = 200
        else:
            response = {
                'status': False,
                'message': 'List of platform codes - empty',
                'result': ids
            }
            status_code = 200
    except exceptions.NotFoundError:
            response = {
                'status': False,
                'message': 'List of platform codes - empty',
                'result': ids
            }
            status_code = 200
    except exceptions.ConnectionError:
        response = {
            'status': False,
            'message': 'Internal error. Unable to connect to DB',
            'result': []
        }
        status_code = 503
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
                using=elastic, index=index_name(rule)).update_from_dict(search_body)
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


def get_metadata_id(platform_code):
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
                The result is a list with a dict. The key of the dict is the
                data_id and the value is the content of the data_id fro the
                database.
            The status_code is 200 - found, 204 - not found or 503 -
            connection error
    """
    elastic = Elasticsearch(elastic_host)

    try:
        el_response = elastic.get(metadata_index, platform_code)
        if el_response.get('found'):
            response = {
                'status': True,
                'message': 'Metadata information',
                'result': [{platform_code: el_response['_source']}]
            }
            status_code = 200
        else:
            response = {
                'status': False,
                'message': 'Metadata not updated',
                'result': []
            }
            status_code = 204

    except exceptions.NotFoundError:
            response = {
                'status': False,
                'message': 'Metadata not updated',
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
