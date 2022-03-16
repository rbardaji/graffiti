import os
import threading
import plotly
import plotly.express as px
import pandas as pd

from flask import abort

from config import fig_folder, fig_url, config_fig, mapbox_access_token
from ..utils.db_manager import (good_rule, get_df, get_metadata, get_parameter,
                                get_data_count, get_metadata_id)
from ..utils.helper import time_to_str


def create_fig_folder():
    """ Create fig folder """
    # Check if folder exist
    if not os.path.exists(fig_folder):
        os.makedirs(fig_folder)


def get_rule(platform_code, parameter, depth_min=None, depth_max=None,
             time_min=None, time_max=None, qc=None):
    """
    Returns the best rule tu use or False.

    Parameters
    ----------
        platform_code: str or list of str
            Platform code or list of platform_code
        parameter: str or list of str
            Parameter acronym or list of parameters
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
        rule: str - bool
            The best rule to use. If the function detects a connection error
            or a bad search query (check the dates), it returns False
    """
    rule_puntuation = {
        'None': 0,
        'R': 1,
        'H': 2,
        '2H': 3,
        '3H': 4,
        '6H': 5,
        '8H': 6,
        '12H': 7,
        'D': 8,
        '2D': 9,
        '3D': 10,
        '4D': 11,
        '5D': 12,
        '6D': 13,
        '10D': 14,
        '15D': 15,
        'M': 16}

    if isinstance(platform_code, str):
        platform_code = [platform_code]
    
    if isinstance(parameter, str):
        parameter = [parameter]
        
    rule = 'None'
    for platform in platform_code:  # platform_code is a list
        for param in parameter:  # parameter is a list
            search_string = '{"platform_code":' + f'"{platform}"' + \
                ',"parameter":' + f'"{param}"' + '}'
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

            rule_platform = good_rule(search_string)  # rule is False if there
                                                        # is a db connection
                                                        # error
            if rule_platform:
                if rule_puntuation[rule_platform] > rule_puntuation[rule]:
                    rule = rule_platform

    if rule == 'None':
        rule = False
    return rule


def thread_line(platform_code_list, parameter_list, fig_name, depth_min=None,
                depth_max=None, time_min=None, time_max=None, qc=None,
                template=None, detached=False):
    """
    It creates a line figure, the x axis is the time and the y axis is the
    averave of values from the input parameter of the platform_code.
    Save the figure in the {fig_folder}/{fig_name}.html

    Parameters
    ----------
        platform_code: str or list of str
            Platform code
        parameter: str or platform_code_list
            Parameter acronym
        fig_name: str
            Name of the figure
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
        template: str
            Options: 'ggplot2', 'seaborn', 'simple_white', 'plotly',
            'plotly_white', 'plotly_dark', 'presentation', 'xgridoff',
            'ygridoff' and 'gridon'
        detached: bool
            If detached is True, the function makes an html with the message
            'no data found'.
    
    Returns
    -------
        figure_path: str - bool
            Location of the figure (html file). If there is no data or a db
            connection error, it returns False
    """
    rule = get_rule(platform_code_list, parameter_list, depth_min, depth_max,
                    time_min, time_max, qc)  # rule is False if there is a db connection
                                             # error

    if rule:
        df = get_df(platform_code_list, parameter_list, rule, depth_min,
                    depth_max, time_min, time_max, qc)


        figure_path = f'{fig_folder}/{fig_name}.html'

        if df.empty:
            figure_path = False
        else:
            fig = px.line(df, x='time', y='value', color='depth',
                          symbol='parameter',
                          line_dash='platform_code', line_shape="spline",
                          render_mode="svg", template=template)

            plotly.io.write_html(fig, figure_path, config=config_fig,
                                 include_plotlyjs='cdn')
    else:
        figure_path = False

    if figure_path == False and detached == True:
        with open(f'{fig_folder}/{fig_name}.html', 'w') as fp:
            fp.write('No data found')
    return figure_path


def get_line(platform_code_list, parameter_list, depth_min=None, depth_max=None,
             time_min=None, time_max=None, qc=None, template=None,
             multithread=True):
    """
    Make a time series line figure using Plotly. The trace contains averages
    values of the input parameter. 

    Parameters
    ----------
        platform_code_list: str or list of str
            Platform code
        parameter_list: str or list of str
            Variable to plot in the y axis.
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
        template: str
            Options: 'ggplot2', 'seaborn', 'simple_white', 'plotly',
            'plotly_white', 'plotly_dark', 'presentation', 'xgridoff',
            'ygridoff' and 'gridon'
        multithread: bool
            Getting the data and making the plot takes a while.
            This argument makes the figure with a secondary thread to avoid
            blocking the main program.
    
    Returns
    -------
        (response, status_code): (dict, int)
            The response is a dictionary with the keys -> status, message and
            result.
                The status is a bool that says if the operation was successful.
                The message is a str with comments for the user.
                The result contains a list with of the figure.
            The status_code is always 201 (created) if multithread = True,
            otherwhise status_code can be 404 if data is not found.
    """
    if isinstance(platform_code_list, str):
        platform_code_list = [platform_code_list]
    if isinstance(parameter_list, str):
        parameter_list = [parameter_list]

    time_min_str, time_max_str =time_to_str(time_min, time_max)

    # Create the filename
    fig_name = f'line-{(",").join(platform_code_list)}' + \
        f'-{(",").join(parameter_list)}-dmin{depth_min}' + \
        f'-dmax{depth_max}-tmin{time_min_str}-tmax{time_max_str}-qc{qc}' + \
        f'-template{template}'

    if not os.path.exists(f'{fig_folder}/{fig_name}.html'):

        create_fig_folder()
        
        if multithread:
            f = threading.Thread(
                target=thread_line,
                args=(platform_code_list, parameter_list, fig_name, depth_min,
                      depth_max, time_min, time_max, qc, template, True))
            f.start()

            response = {
                'status': True,
                'message': 'Working, please wait some minuts before ' + \
                    'access to the link from result[0].',
                'result': [f'{fig_url}/{fig_name}.html']}
            status_code = 201

        else:
            path_fig = thread_line(platform_code_list, parameter_list, fig_name,
                                   depth_min, depth_max, time_min, time_max, qc,
                                   template)

            if path_fig:
                response = {
                    'status': True,
                    'message': 'Link to the figure in result[0]',
                    'result': [f'{fig_url}/{fig_name}.html']}
                status_code = 201
            else:
                abort(404, 'Data not found')
    else:
        response = {
            'status': True,
            'message': 'Link to the figure in result[0]',
            'result': [f'{fig_url}/{fig_name}.html']}
        status_code = 201

    return response, status_code


def thread_area(platform_code_list, parameter_list, fig_name, depth_min=None,
                depth_max=None, time_min=None, time_max=None, qc=None,
                template=None, detached=False):
    """
    It creates an area figure, the x axis is the time and the y axis is the
    averave of values from the input parameter of the platform_code.
    Save the figure in the {fig_folder}/{fig_name}.html

    Parameters
    ----------
        platform_code_list: str or list of str
            Platform code
        parameter_list: str or list of str
            Parameter acronym
        fig_name: str
            Name of the figure
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
        template: str
            Options: 'ggplot2', 'seaborn', 'simple_white', 'plotly',
            'plotly_white', 'plotly_dark', 'presentation', 'xgridoff',
            'ygridoff' and 'gridon'
        detached: bool
            If detached is True, the function makes an html with the message
            'no data found'.
    
    Returns
    -------
        figure_path: str - bool
            Location of the figure (html file). If there is no data or a db
            connection error, it returns False
    """
    rule = get_rule(platform_code_list, parameter_list, depth_min, depth_max,
                    time_min, time_max, qc)  # rule is False if there is a db connection
                                             # error

    if rule:
        df = get_df(platform_code_list, parameter_list, rule, depth_min,
                    depth_max, time_min, time_max, qc)

        figure_path = f'{fig_folder}/{fig_name}.html'

        if df.empty:
            figure_path = False
        else:
            fig = px.area(df, x='time', y='value', color='depth',
                          line_group='platform_code', template=template,
                          line_shape='spline', symbol='parameter')

            plotly.io.write_html(fig, figure_path, config=config_fig, 
                                 include_plotlyjs='cdn')
    else:
        figure_path = False

    if figure_path == False and detached == True:
        with open(f'{fig_folder}/{fig_name}.html', 'w') as fp:
            fp.write('No data found')

    return figure_path


def get_area(platform_code_list, parameter_list, depth_min=None, depth_max=None,
             time_min=None, time_max=None, qc=None, template=None,
             multithread=True):
    """
    Make an area figure using Plotly. The trace contains averages
    values of the input parameter. 

    Parameters
    ----------
        platform_code_list: str or list of str
            Platform code
        parameter_list: str or list of str
            Variable to plot in the y axis.
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
        template: str
            Options: 'ggplot2', 'seaborn', 'simple_white', 'plotly',
            'plotly_white', 'plotly_dark', 'presentation', 'xgridoff',
            'ygridoff' and 'gridon'
        multithread: bool
            Getting the data and making the plot takes a while.
            This argument makes the figure with a secondary thread to avoid
            blocking the main program.
    
    Returns
    -------
        (response, status_code): (dict, int)
            The response is a dictionary with the keys -> status, message and
            result.
                The status is a bool that says if the operation was successful.
                The message is a str with comments for the user.
                The result contains a list with of the figure.
            The status_code is always 201 (created) if multithread = True,
            otherwhise status_code can be 404 if data is not found.
    """
    if isinstance(platform_code_list, str):
        platform_code_list = [platform_code_list]
    if isinstance(parameter_list, str):
        parameter_list = [parameter_list]

    time_min_str, time_max_str =time_to_str(time_min, time_max)
    # Create the filename
    fig_name = f'area-{(",").join(platform_code_list)}' + \
        f'-{(",").join(parameter_list)}' + \
        f'-dmin{depth_min}-dmax{depth_max}-tmin{time_min_str}' + \
        f'-tmax{time_max_str}-qc{qc}-template{template}'

    if not os.path.exists(f'{fig_folder}/{fig_name}.html'):

        create_fig_folder()

        if multithread:
            f = threading.Thread(
                target=thread_area,
                args=(platform_code_list, parameter_list, fig_name, depth_min,
                      depth_max, time_min, time_max, qc, template, True))
            f.start()

            response = {
                'status': True,
                'message': 'Working, please wait some minuts before ' + \
                    'access to the link from result[0].',
                'result': [f'{fig_url}/{fig_name}.html']}
            status_code = 201
        else:
            path_fig = thread_area(platform_code_list, parameter_list, fig_name,
                                   depth_min, depth_max, time_min, time_max, qc,
                                   template)

            if path_fig:
                response = {
                    'status': True,
                    'message': 'Link to the figure in result[0]',
                    'result': [f'{fig_url}/{fig_name}.html']}
                status_code = 201
            else:
                abort(404, 'Data not found')

    else:
        response = {
            'status': True,
            'message': 'Link to the figure in result[0]',
            'result': [f'{fig_url}/{fig_name}.html']}
        status_code = 201

    return response, status_code


def thread_parameter_availability(parameter, platform_code_list, fig_name,
                                  depth_min=None, depth_max=None, time_min=None,
                                  time_max=None, qc=None, template=None,
                                  detached=False):
    """
    It creates an gantt figure, the x axis is the time and the y axis
    represents the aviability of the input parameter from the
    input platform_code list.
    Save the figure in the {fig_folder}/{fig_name}.html

    Parameters
    ----------
        parameter: str
            Parameter acronym
        platform_code_list: list of str
            List of platform Platform code
        fig_name: str
            Name of the figure
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
        template: str
            Options: 'ggplot2', 'seaborn', 'simple_white', 'plotly',
            'plotly_white', 'plotly_dark', 'presentation', 'xgridoff',
            'ygridoff' and 'gridon'.
        detached: bool
            If detached is True, the function makes an html with the message
            'no data found'.
    
    Returns
    -------
        figure_path: str - bool
            Location of the figure (html file). If there is no data or a db
            connection error, it returns False
    """
    rule = get_rule(platform_code_list, parameter, depth_min, depth_max,
                    time_min, time_max, qc)

    if rule:

        figure_path = f'{fig_url}/{fig_name}.html'

        # Create DataFrame
        df_content = []
        for platform_code in platform_code_list:
            df_parameter = get_df(platform_code, parameter, rule, depth_min,
                                depth_max, time_min, time_max, qc)

            try:
                df_parameter['time'] = pd.to_datetime(df_parameter['time'])
                df_parameter.set_index('time', inplace=True)
            except:
                # df_parameter is empty
                continue

            df_parameter = df_parameter.resample(rule).mean()

            ts = df_parameter['value'].isnull()
            intervals = []
            in_interval = False
            end = None

            for index, value in ts.items():
                end = index.strftime('%Y-%m-%d %H:%M:%S')
                if in_interval is False and value is False:
                    in_interval = True
                    start = index.strftime('%Y-%m-%d %H:%M:%S')
                elif in_interval is True and value is True:
                    in_interval = False
                    intervals.append((start, end))
                    if in_interval is True:
                        intervals.append((start, end))
            if not intervals:
                df_parameter.reset_index(inplace=True)
                start = df_parameter.iloc[0]['time'].strftime('%Y-%m-%d %H:%M:%S')
                end = df_parameter.iloc[-1]['time'].strftime('%Y-%m-%d %H:%M:%S')
                intervals.append((start, end))

            # Make the dictionary
            for start, end in intervals:
                df_content.append(
                    dict(
                        Task=f'{platform_code}',
                        Start=start,
                        Finish=end,
                        Resource=f'{platform_code}'))

        # # Make fig
        df = pd.DataFrame(df_content)

        if df.empty:
            figure_path = False
        else:
            fig = px.timeline(
                df,
                x_start='Start',
                x_end='Finish',
                y='Task',
                color='Resource',
                title=f'Data availability for {parameter}',
                labels={'Task': 'Platform codes'},
                template=template)

            fig.update(layout_showlegend=False)

            plotly.io.write_html(fig, f'{fig_folder}/{fig_name}.html',
                                 config=config_fig, include_plotlyjs='cdn')
    else:
        figure_path = False

    if figure_path == False and detached == True:
        with open(f'{fig_folder}/{fig_name}.html', 'w') as fp:
            fp.write('No data found')
    return figure_path


def get_parameter_availability(parameter, depth_min=None, depth_max=None,
                               time_min=None, time_max=None, qc=None,
                               template=None, multithread=True):
    """
    Make an parameter aviability (gantt) figure using Plotly.

    Parameters
    ----------
        parameter: str
            Variable to plot in the y axis.
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
        template: str
            Options: 'ggplot2', 'seaborn', 'simple_white', 'plotly',
            'plotly_white', 'plotly_dark', 'presentation', 'xgridoff',
            'ygridoff' and 'gridon'.
        multithread: bool
            Getting the data and making the plot takes a while.
            This argument makes the figure with a secondary thread to avoid
            blocking the main program.
    
    Returns
    -------
        (response, status_code): (dict, int)
            The response is a dictionary with the keys -> status, message and
            result.
                The status is a bool that says if the operation was successful.
                The message is a str with comments for the user.
                The result contains a list with of the figure.
            The status_code is always 201 (created) if multithread = True,
            otherwhise status_code can be 404 if data is not found or 503 to
            indicate db connection errors.
    """
    time_min_str, time_max_str = time_to_str(time_min, time_max)
    # Create the filename
    fig_name = f'parameter_availability-{parameter}-dmin{depth_min}-' + \
        f'dmax{depth_max}-tmin{time_min_str}-tmax{time_max_str}-qc{qc}' + \
        f'template{template}'

    if not os.path.exists(f'{fig_folder}/{fig_name}.html'):

        create_fig_folder()

        # Get all metadata ids (platform_code)
        response, status_code = get_metadata(parameter=parameter)

        if status_code != 200:
            return response, status_code
        platform_code_list = response['result']

        if platform_code_list:
            if multithread:
                j = threading.Thread(
                    target=thread_parameter_availability,
                    args=(parameter, platform_code_list, fig_name, depth_min,
                          depth_max, time_min, time_max, qc, template, True))
                j.start()

                response = {
                    'status': True,
                    'message': 'Working, please wait some minuts before ' + \
                        'access to the link from result[0].',
                    'result': [f'{fig_url}/{fig_name}.html']}
                status_code = 201
            else:
                path_fig = thread_parameter_availability(parameter,
                                                         platform_code_list,
                                                         fig_name, depth_min,
                                                         depth_max, time_min,
                                                         time_max, qc, template)

                if path_fig:
                    response = {
                        'status': True,
                        'message': 'Link to the figure in result[0]',
                        'result': [f'{fig_url}/{fig_name}.html']}
                    status_code = 201
                else:
                    abort(404, 'Data not found')
        else:
            abort(404, 'Data not found')
    else:
        response = {
            'status': True,
            'message': 'Link to the figure in result[0]',
            'result': [f'{fig_url}/{fig_name}.html']}
        status_code = 201

    return response, status_code


def thread_platform_availability(platform_code, fig_name, depth_min=None,
                                 depth_max=None, time_min=None, time_max=None,
                                 qc=None, template=None, detached=False):
    """
    It creates an gantt figure, the x axis is the time and the y axis
    represents the aviability of the parameter of the input platform_code.
    Save the figure in the {fig_folder}/{fig_name}.html

    Parameters
    ----------
        platform_code: str
            Platform code
        fig_name: str
            Name of the figure
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
        template: str
            Options: 'ggplot2', 'seaborn', 'simple_white', 'plotly',
            'plotly_white', 'plotly_dark', 'presentation', 'xgridoff',
            'ygridoff' and 'gridon'.
        detached: bool
            If detached is True, the function makes an html with the message
            'no data found'.
    
    Returns
    -------
        figure_path: str - bool
            Location of the figure (html file). If there is no data or a db
            connection error, it returns False
    """

    parameters = []
    response_parameters, status_code = get_parameter(platform_code=platform_code,
                                                     depth_min=depth_min,
                                                     depth_max=depth_max,
                                                     time_min=time_min,
                                                     time_max=time_max, qc=qc,
                                                     rule='M')
    if status_code != 200:
        return False

    for response_parameter in response_parameters['result']:
        parameters.append(response_parameter['key'])

    if not parameters:
        return False

    # Get good rule
    search_string = '{"platform_code":' + f'"{platform_code}"' + \
        ',"parameter":' + f'"{parameters[0]}"' + '}'
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

    rule = good_rule(search_string)

    if rule:

        figure_path = f'{fig_url}/{fig_name}.html'

        df_content = []
        for parameter in parameters:

            df_parameter = get_df(platform_code, parameter, rule, depth_min,
                                depth_max, time_min, time_max, qc)

            try:
                df_parameter['time'] = pd.to_datetime(df_parameter['time'])
                df_parameter.set_index('time', inplace=True)
            except KeyError:
                # Empty df
                continue

            if rule != 'R':
                df_parameter = df_parameter.resample(rule).mean()
            else:
                df_parameter = df_parameter.resample('H').mean()

            ts = df_parameter['value'].isnull()
            intervals = []
            in_interval = False
            end = None

            for index, value in ts.items():
                end = index.strftime('%Y-%m-%d %H:%M:%S')
                if in_interval is False and value is False:
                    in_interval = True
                    start = index.strftime('%Y-%m-%d %H:%M:%S')
                elif in_interval is True and value is True:
                    in_interval = False
                    intervals.append((start, end))
                    if in_interval is True:
                        intervals.append((start, end))
            if not intervals:
                df_parameter.reset_index(inplace=True)
                start = df_parameter.iloc[0]['time'].strftime('%Y-%m-%d %H:%M:%S')
                end = df_parameter.iloc[-1]['time'].strftime('%Y-%m-%d %H:%M:%S')
                intervals.append((start, end))

            # Make the dictionary
            for start, end in intervals:
                df_content.append(
                    dict(
                        Task=f'{parameter}',
                        Start=start,
                        Finish=end,
                        Resource=f'{parameter}'))

        # # Make fig
        df = pd.DataFrame(df_content)

        if df.empty:
            figure_path = False
        else:
            fig = px.timeline(
                df,
                x_start='Start',
                x_end='Finish',
                y='Task',
                color='Resource',
                # title=f'Data availability from {platform_code}',
                labels={'Task': 'Parameters'},
                template=template)

            fig.update(layout_showlegend=False)
            fig.update_layout(margin=dict(l=0, r=0, t=0, b=0))

            plotly.io.write_html(fig, f'{fig_folder}/{fig_name}.html',
                                config=config_fig, include_plotlyjs='cdn')
    else:
        figure_path = False

    if figure_path == False and detached == True:
        with open(f'{fig_folder}/{fig_name}.html', 'w') as fp:
            fp.write('No data found')
    return figure_path


def get_platform_availability(platform_code, depth_min=None, depth_max=None,
                              time_min=None, time_max=None, qc=None,
                              template=None, multithread=True):
    """
    Make an platform  aviability (gantt) figure using Plotly.

    Parameters
    ----------
        platform_code: str
            Variable to plot in the y axis.
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
        template: str
            Options: 'ggplot2', 'seaborn', 'simple_white', 'plotly',
            'plotly_white', 'plotly_dark', 'presentation', 'xgridoff',
            'ygridoff' and 'gridon'.
        multithread: bool
            Getting the data and making the plot takes a while.
            This argument makes the figure with a secondary thread to avoid
            blocking the main program.

    Returns
    -------
        (response, status_code): (dict, int)
            The response is a dictionary with the keys -> status, message and
            result.
                The status is a bool that says if the operation was successful.
                The message is a str with comments for the user.
                The result contains a list with of the figure.
            The status_code is always 201 (created) if multithread = True,
            otherwhise status_code can be 404 if data is not found or 503 to
            indicate db connection errors.
    """
    time_min_str, time_max_str = time_to_str(time_min, time_max)
    # Create the filename
    fig_name = f'platform_availability-{platform_code}-dmin{depth_min}' + \
        f'-dmax{depth_max}-tmin{time_min_str}-tmax{time_max_str}-qc{qc}' + \
        f'-template{template}'

    if not os.path.exists(f'{fig_folder}/{fig_name}.html'):

        create_fig_folder()

        if multithread:
            j = threading.Thread(
                    target=thread_platform_availability,
                    args=(platform_code, fig_name, depth_min, depth_max,
                          time_min, time_max, qc, True))
            j.start()

            response = {
                'status': True,
                'message': 'Working, please wait some minuts before access ' + \
                    f'to the result link. {platform_code} availability',
                'result': [f'{fig_url}/{fig_name}.html']
            }
            status_code = 201
        else:
            path_fig =  thread_platform_availability(platform_code, fig_name,
                                                     depth_min, depth_max,
                                                     time_min, time_max, qc,
                                                     template)
            if path_fig:
                response = {
                    'status': True,
                    'message': f'{platform_code} availability',
                    'result': [path_fig]}
                status_code = 201
            else:
                abort(404, 'Data not found')
    else:
        response = {
            'status': True,
            'message': f'{platform_code} availability',
            'result': [f'{fig_url}/{fig_name}.html']
        }
        status_code = 201

    return response, status_code


def get_parameter_pie(rule, platform_code=None, depth_min=None, depth_max=None,
                      time_min=None, time_max=None, qc=None, template=None):
    """
    Make an parameter aviability (Pie Chart) figure using Plotly.

    Parameters
    ----------
        rule: str
            Index rule
        platform_code: str
            Platform Code
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
        template: str
            Options: 'ggplot2', 'seaborn', 'simple_white', 'plotly',
            'plotly_white', 'plotly_dark', 'presentation', 'xgridoff',
            'ygridoff' and 'gridon'.
    
    Returns
    -------
        (response, status_code): (dict, int)
            The response is a dictionary with the keys -> status, message and
            result.
                The status is a bool that says if the operation was successful.
                The message is a str with comments for the user.
                The result contains a list with of the figure.
            The status_code is always 201 (created) if multithread = True,
            otherwhise status_code can be 404 if data is not found or 503 to
            indicate db connection errors.
    """
    time_min_str, time_max_str = time_to_str(time_min, time_max)

    fig_name = f'parameter_pie-r{rule}-plat{platform_code}-dmin{depth_min}' + \
        f'-dmax{depth_max}-tmin{time_min_str}-tmax{time_max_str}-qc{qc}' + \
        f'-template{template}'


    if not os.path.exists(f'{fig_folder}/{fig_name}.html'):

        create_fig_folder()

        response, status_code = get_parameter(platform_code, depth_min,
                                              depth_max, time_min, time_max, qc,
                                              rule)
        if status_code != 200:
            return response, status_code

        parameter_list = response['result']
        
        if parameter_list:
            # Create DataFrame
            df = pd.DataFrame(parameter_list)
            fig = px.pie(df, values='doc_count', names='key', template=template,
                         labels={'key': 'Parameter',
                                 'doc_count': 'Measurements'})
            fig.update_layout(margin=dict(l=0, r=0, t=0, b=0))
            plotly.io.write_html(fig, f'{fig_folder}/{fig_name}.html',
                                 config=config_fig, include_plotlyjs='cdn')
            response = {
                'status': True,
                'message': 'Link to the figure in result[0]',
                'result': [f'{fig_url}/{fig_name}.html']}
            status_code = 201
        else:
            abort(404, 'Data not found')
    else:
        response = {
            'status': True,
            'message': 'Link to the figure in result[0]',
            'result': [f'{fig_url}/{fig_name}.html']}
        status_code = 201

    return response, status_code


def get_platform_pie(rule, parameter=None, depth_min=None, depth_max=None,
                     time_min=None, time_max=None, qc=None, template=None):
    """
    Make an platform data number (Pie Chart) figure using Plotly.

    Parameters
    ----------
        rule: str
            Index rule
        parameter: str
            Parameter acronym
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
        template: str
            Options: 'ggplot2', 'seaborn', 'simple_white', 'plotly',
            'plotly_white', 'plotly_dark', 'presentation', 'xgridoff',
            'ygridoff' and 'gridon'.
    
    Returns
    -------
        (response, status_code): (dict, int)
            The response is a dictionary with the keys -> status, message and
            result.
                The status is a bool that says if the operation was successful.
                The message is a str with comments for the user.
                The result contains a list with of the figure.
            The status_code is always 201 (created) if multithread = True,
            otherwhise status_code can be 404 if data is not found or 503 to
            indicate db connection errors.
    """
    time_min_str, time_max_str = time_to_str(time_min, time_max)

    fig_name = f'platform_pie-r{rule}-param{parameter}-dmin{depth_min}' + \
        f'-dmax{depth_max}-tmin{time_min_str}-tmax{time_max_str}-qc{qc}' + \
        f'-template{template}'

    if not os.path.exists(f'{fig_folder}/{fig_name}.html'):

        create_fig_folder()

        # Get metadata ids
        response, status_code = get_metadata()

        if status_code != 200:
            return response, status_code

        platform_code_list = response['result']

        data_content = []
        for platform_code in platform_code_list:

            response, status_code = get_data_count(rule,
                                                   platform_code=platform_code,
                                                   parameter=parameter,
                                                   depth_min=depth_min,
                                                   depth_max=depth_max,
                                                   time_min=time_min,
                                                   time_max=time_max, qc=qc)
            if status_code != 200:
                continue

            count = int(response['result'][0])
            if count > 0:
                data_content.append(
                    {'Platform Code': platform_code, 'Measurements': count})

        if data_content:
            # Create DataFrame
            df = pd.DataFrame(data_content)
            fig = px.pie(df, values='Measurements', names='Platform Code',
                         template=template)

            plotly.io.write_html(fig, f'{fig_folder}/{fig_name}.html',
                                 config=config_fig, include_plotlyjs='cdn')

            response = {
                'status': True,
                'message': 'Platform pie',
                'result': [f'{fig_url}/{fig_name}.html']
            }
            status_code = 201
        else:
            abort(404, 'Data not found')
        
    else:
        response = {
            'status': True,
            'message': 'Link to the figure in result[0]',
            'result': [f'{fig_url}/{fig_name}.html']}
        status_code = 201
    
    return response, status_code


def get_map(rule, platform_code=None, parameter=None, depth_min=None,
            depth_max=None, time_min=None, time_max=None, qc=None,
            template=None):
    """
    Make a map with the points where we have data that match with the input
    parameters.append()

    Parameters
    ----------
        rule: str
            Index rule
        platform_code: str
            Platform code
        parameter: str
            Parameter acronym
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
        template: str
            Options: 'ggplot2', 'seaborn', 'simple_white', 'plotly',
            'plotly_white', 'plotly_dark', 'presentation', 'xgridoff',
            'ygridoff' and 'gridon'
    
    Returns
    -------
        (response, status_code): (dict, int)
            The response is a dictionary with the keys -> status, message and
            result.
                The status is a bool that says if the operation was successful.
                The message is a str with comments for the user.
                The result contains a list with of the figure.
            The status_code is always 201 (created) if multithread = True,
            otherwhise status_code can be 404 if data is not found or 503 to
            indicate db connection errors.
    """
    time_min_str, time_max_str = time_to_str(time_min, time_max)

    fig_name = f'map-r{rule}-plat{platform_code}-param{parameter}' + \
        f'-dmin{depth_min}-dmax{depth_max}-tmin{time_min_str}' + \
        f'-tmax{time_max_str}-qc{qc}-template{template}'

    if not os.path.exists(f'{fig_folder}/{fig_name}.html'):

        # Check if folder exist
        if not os.path.exists(fig_folder):
            os.makedirs(fig_folder)

        if platform_code:
            platform_codes = [platform_code]
        else:
            # Get metadata list
            response, status_code = get_metadata()
            if status_code != 200:
                return response, status_code
            
            platform_codes = response['result']

        if not platform_codes:
            abort(404, 'Data not found')

        latitudes = []
        longitudes = []
        parameters = []
        start_dates = []
        end_dates = []
        for platform in platform_codes:

            response, status_code = get_data_count(rule, platform_code=platform,
                                                   parameter=parameter,
                                                   depth_min=depth_min,
                                                   depth_max=depth_max,
                                                   time_min=time_min,
                                                   time_max=time_max, qc=qc)
            if status_code != 200:
                continue
            
            count = int(response['result'][0])
            if count > 0:
                # Get metadata information
                response, status_code = get_metadata_id(platform)
                if status_code != 200:
                    return response, status_code

                lat = float(
                    response['result'][0][platform].get(
                        'last_latitude_observation'))
                lon = float(
                    response['result'][0][platform].get(
                        'last_longitude_observation'))
                latitudes.append(lat)
                longitudes.append(lon)
                parameters.append(
                    f'{" ,".join(response["result"][0][platform].get("parameters"))}')
                start_dates.append(
                    f'{response["result"][0][platform].get("start_date_observation")}')
                end_dates.append(
                    f'{response["result"][0][platform].get("end_date_observation")}')

        geo_df = pd.DataFrame(
            list(zip(
                latitudes, longitudes, platform_codes, parameters, start_dates,
                end_dates)),
            columns =['lat', 'lon', 'platform_code', 'parameters', 'start_date',
                      'end_date'])

        px.set_mapbox_access_token(mapbox_access_token)
        fig = px.scatter_mapbox(geo_df,
                                lat=geo_df['lat'],
                                lon=geo_df['lon'],
                                hover_name='platform_code',
                                zoom=1, template=template)
        fig.update_layout(margin=dict(l=0, r=0, t=0, b=0))

        plotly.io.write_html(fig, f'{fig_folder}/{fig_name}.html',
                             include_plotlyjs='cdn')
    
    response = {
        'status': True,
        'message': 'Link to the figure in result[0]',
        'result': [f'{fig_url}/{fig_name}.html']
    }
    status_code = 201

    return response, status_code


def thread_scatter(platform_code, x, y, fig_name, color=None, marginal_x=None,
                   marginal_y=None, trendline=None, template=None,
                   depth_min=None, depth_max=None, time_min=None, time_max=None,
                   qc=None, detached=False):

    parameter_list = [x, y, color]
    rule = get_rule(platform_code, parameter_list, depth_min, depth_max,
                    time_min, time_max, qc)  # rule is False if there is a db
                                             # connection error

    if rule:

        figure_path = f'{fig_folder}/{fig_name}.html'
        # Get x
        df_x = get_df(platform_code, x, rule, depth_min, depth_max,
                      time_min, time_max, qc)
        df_x.set_index(['depth', 'time'], inplace=True)
        df_x.rename(columns={'value': x}, inplace=True)

        # Get y
        df_y = get_df(platform_code, y, rule, depth_min, depth_max,
                      time_min, time_max, qc)
        df_y.set_index(['depth', 'time'], inplace=True)
        df_y.rename(columns={'value': y}, inplace=True)

        df = df_x.join(df_y, how='left', lsuffix=f'_{x}', rsuffix=f'_{y}')
        df.reset_index(inplace=True)

        fig = px.scatter(df, x=x, y=y, color=color, marginal_x=marginal_x,
                         marginal_y=marginal_y, trendline=trendline,
                         template=template)

        plotly.io.write_html(fig, f'{fig_folder}/{fig_name}.html',
                             config=config_fig, include_plotlyjs='cdn')

    else:
        figure_path = False

    if figure_path == False and detached == True:
        with open(f'{fig_folder}/{fig_name}.html', 'w') as fp:
            fp.write('No data found')
    return figure_path


def get_scatter(platform_code, x, y, color=None, marginal_x=None,
                marginal_y=None, trendline=None, template=None, depth_min=None,
                depth_max=None, time_min=None, time_max=None, qc=None,
                multithread=True):
    """
    Make a scatter figure using Plotly.

    Parameters
    ----------
        platform_code: str
            Platform code
        x: str
            Variable to plot in the x axis.
        y: str
            Variable to plot in the y axis.
        color: str
            Variable that defines the color of the dots.
        marginal_x: str
            Type of chart to be included in the x axis.
        marginal_y: str
            Type of chart to be included in the y axis.
        trendline: str
            Type of trendline.
        template: str
            Type of template to use.
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
        multithread: bool
            Getting the data and making the plot takes a while.
            This argument makes the figure with a secondary thread to avoid
            blocking the main program.
    
    Returns
    -------
        (response, status_code): (dict, int)
            The response is a dictionary with the keys -> status, message and
            result.
                The status is a bool that says if the operation was successful.
                The message is a str with comments for the user.
                The result contains a list with of the figure.
            The status_code is always 201 (created) if multithread = True,
            otherwhise status_code can be 404 if data is not found.
    """
    time_min_str, time_max_str = time_to_str(time_min, time_max)

    # Create the filename
    fig_name = f'scatter-{platform_code}-X{x}-Y{y}-C{color}-MX{marginal_x}' + \
        f'-MY-{marginal_y}-TL-{trendline}-TM-{template}-dmin{depth_min}' + \
        f'-dmax{depth_max}-tmin{time_min_str}-tmax{time_max_str}-qc{qc}'

    if not os.path.exists(f'{fig_folder}/{fig_name}.html'):

        create_fig_folder()

        if multithread:
            f = threading.Thread(
                target=thread_scatter,
                args=(platform_code, x, y, fig_name, color, marginal_x,
                      marginal_y, trendline, template, depth_min, depth_max,
                      time_min, time_max, qc, True))
            f.start()

            response = {
                'status': True,
                'message': 'Working, please wait some minuts before access ' + \
                    'to the result link.',
                'result': [f'{fig_url}/{fig_name}.html']}
            status_code = 201

        else:
            path_fig = thread_scatter(platform_code, x, y, fig_name, color,
                                      marginal_x, marginal_y, trendline,
                                      template, depth_min, depth_max, time_min,
                                      time_max, qc, False)

            if path_fig:
                response = {
                    'status': True,
                    'message': f'Scatter plot from {platform_code}',
                    'result': [f'{fig_url}/{fig_name}.html']}
                status_code = 201
            else:
                abort(404, 'Data not found')
    else:
        response = {
            'status': True,
            'message': f'Scatter plot from {platform_code}',
            'result': [f'{fig_url}/{fig_name}.html']}
        status_code = 201

    return response, status_code
