import os
import glob

from config import fig_folder, df_folder


def delete_scatter(platform_code):
    """
    Delete all scatter plots
    
    Parameters
    ----------
        platform_code: str
            Platform code

    Returns
    -------
        (response, status_code): (dict, int)
            The response is a dictionary with the keys -> status, message and
            result.
                The status is a bool that says if the operation was successful.
                The message is a str with comments for the user.
                The result is an empty list.
            The status_code is always 204 (deleted).

    """
    # Delete figures
    for filename in glob.glob(f'{fig_folder}/scatter-{platform_code}*'):
        os.remove(filename)
    # Delete pkls
    for filename in glob.glob(f'{df_folder}/{platform_code}*'):
        os.remove(filename)

    response = {
        'status': True,
        'message': f'Scatter plot from {platform_code} deleted',
        'result': []
    }
    status_code = 204

    return response, status_code


def delete_platform_pie():
    """
    Delete all platform pie plots

    Returns
    -------
        (response, status_code): (dict, int)
            The response is a dictionary with the keys -> status, message and
            result.
                The status is a bool that says if the operation was successful.
                The message is a str with comments for the user.
                The result is an empty list.
            The status_code is always 204 (deleted).

    """
    # Delete figures
    for filename in glob.glob(f'{fig_folder}/platform_pie*'):
        os.remove(filename)

    response =  {
        'status': True,
        'message': f'Platform pie plots deleted',
        'result': []
    }
    status_code = 204

    return response, status_code


def delete_parameter_availability(parameter):
    """
    Delete all parameter aviability plots

    Parameters
    ----------
        parameter: str
            Parameter acronym

    Returns
    -------
        (response, status_code): (dict, int)
            The response is a dictionary with the keys -> status, message and
            result.
                The status is a bool that says if the operation was successful.
                The message is a str with comments for the user.
                The result is an empty list.
            The status_code is always 204 (deleted).
    """
    # Delete all metadata_map figures
    # Loop Through the folder projects all files and deleting them one by one
    for file_map in glob.glob(
        f'{fig_folder}/parameter_availability-{parameter}*'):
        os.remove(file_map)
    for file_pkl in glob.glob(f'{df_folder}/*{parameter}*'):
        os.remove(file_pkl)

    response = {
        'status': True,
        'message': f'The plot of data availability from {parameter} is deleted',
        'result': []
    }
    status_code = 204

    return response, status_code


def delete_platform_availability(platform_code):
    """
    Delete all platform avialable availability plots

    Parameters
    ----------
        parameter: str
            Parameter acronym

    Returns
    -------
        (response, status_code): (dict, int)
            The response is a dictionary with the keys -> status, message and
            result.
                The status is a bool that says if the operation was successful.
                The message is a str with comments for the user.
                The result is an empty list.
            The status_code is always 204 (deleted).
    """
    # Delete all figures
    # Loop Through the folder projects all files and deleting them one by one
    for file_map in glob.glob(
        f'{fig_folder}/platform_availability-{platform_code}*'):
        os.remove(file_map)
    for file_pkl in glob.glob(f'{df_folder}/{platform_code}*'):
        os.remove(file_pkl)

    response = {
        'status': True,
        'message': f'The plot of data availability from {platform_code} is deleted',
        'result': []
    }
    status_code = 204

    return response, status_code


def delete_map():
    """
    Delete all maps

    Returns
    -------
        (response, status_code): (dict, int)
            The response is a dictionary with the keys -> status, message and
            result.
                The status is a bool that says if the operation was successful.
                The message is a str with comments for the user.
                The result is an empty list.
            The status_code is always 204 (deleted).
    """
    # Delete all metadata_map figures
    # Loop Through the folder projects all files and deleting them one by one
    for file_map in glob.glob(f'{fig_folder}/metadata_map*'):
        os.remove(file_map)

    response = {
        'status': True,
        'message': 'All maps deleted',
        'result': []
    }
    status_code = 204

    return response, status_code


def delete_parameter_pie():
    """
    Delete all parameter pie plots

    Returns
    -------
        (response, status_code): (dict, int)
            The response is a dictionary with the keys -> status, message and
            result.
                The status is a bool that says if the operation was successful.
                The message is a str with comments for the user.
                The result is an empty list.
            The status_code is always 204 (deleted).
    """
    # Delete figures
    for filename in glob.glob(f'{fig_folder}/parameter_pie*'):
        os.remove(filename)

    response = {
        'status': True,
        'message': f'Parameter pie plots deleted',
        'result': []
    }
    status_code = 204

    return response, status_code


def delete_line(platform_code, parameter):
    """
    Delete all line plots

    Parameters
    ----------
        platform_code: str
            Platform code
        parameter: str
            Parameter acronym

    Returns
    -------
        (response, status_code): (dict, int)
            The response is a dictionary with the keys -> status, message and
            result.
                The status is a bool that says if the operation was successful.
                The message is a str with comments for the user.
                The result is an empty list.
            The status_code is always 204 (deleted).
    """
    for file_line in glob.glob(f'{fig_folder}/line-{platform_code}-{parameter}*'):
        os.remove(file_line)
    for file_pkl in glob.glob(f'{df_folder}/{platform_code}_{parameter}*'):
        os.remove(file_pkl)

    response = {
        'status': True,
        'message': f'Plots deleted',
        'result': []
    }
    status_code = 204

    return response, status_code


def delete_area(platform_code, parameter):
    """
    Delete all area plots

    Parameters
    ----------
        platform_code: str
            Platform code
        parameter: str
            Parameter acronym

    Returns
    -------
        (response, status_code): (dict, int)
            The response is a dictionary with the keys -> status, message and
            result.
                The status is a bool that says if the operation was successful.
                The message is a str with comments for the user.
                The result is an empty list.
            The status_code is always 204 (deleted).
    """
    for file_line in glob.glob(f'{fig_folder}/area-{platform_code}-{parameter}*'):
        os.remove(file_line)
    for file_pkl in glob.glob(f'{df_folder}/{platform_code}_{parameter}*'):
        os.remove(file_pkl)

    response = {
        'status': True,
        'message': f'Plots deleted',
        'result': []
    }
    status_code = 204

    return response, status_code
