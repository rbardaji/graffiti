from config import (data_index_r, data_index_h, data_index_2h, data_index_3h,
                    data_index_6h, data_index_8h, data_index_12h, data_index_d,
                    data_index_2d, data_index_3d, data_index_4d, data_index_5d,
                    data_index_6d, data_index_10d, data_index_15d, data_index_m,
                    data_index_m_min, data_index_m_max, data_index_15d_min,
                    data_index_15d_max, data_index_10d_min, data_index_10d_max,
                    data_index_6d_min, data_index_6d_max, data_index_5d_min,
                    data_index_5d_max, data_index_4d_min, data_index_4d_max, 
                    data_index_3d_min, data_index_3d_max, data_index_2d_min,
                    data_index_2d_max, data_index_d_min, data_index_d_max,
                    data_index_12h_min, data_index_12h_max, data_index_8h_min,
                    data_index_8h_max, data_index_6h_min, data_index_6h_max,
                    data_index_3h_min, data_index_3h_max, data_index_2h_min,
                    data_index_2h_max, data_index_h_min, data_index_h_max)


def index_name(rule, method='mean'):
    """
    It returns the mane of the index or the database table.

    Parameters
    ----------
        rule: str
            Options - M, 15D, 10D, 6D, 5D, 4D, 3D, 2D, D, 12H, 8H, 6H, 3H, 2H, H
        method: str
            Options - mean, min, max
    
    Returns
    -------
        data_index_name: str
            Name of the index
    """
    if rule == 'M':
        if method == 'mean':
            data_index_name = data_index_m
        elif method == 'min':
            data_index_name = data_index_m_min
        elif method == 'max':
            data_index_name = data_index_m_max
    elif rule == '15D':
        if method == 'mean':
            data_index_name =  data_index_15d
        elif method == 'min':
            data_index_name = data_index_15d_min
        elif method == 'max':
            data_index_name = data_index_15d_max
    elif rule == '10D':
        if method == 'mean':
            data_index_name =  data_index_10d
        elif method == 'min':
            data_index_name = data_index_10d_min
        elif method == 'max':
            data_index_name = data_index_10d_max
    elif rule == '6D':
        if method == 'mean':
            data_index_name =  data_index_6d
        elif method == 'min':
            data_index_name = data_index_6d_min
        elif method == 'max':
            data_index_name = data_index_6d_max
    elif rule == '5D':
        if method == 'mean':
            data_index_name =  data_index_5d
        elif method == 'min':
            data_index_name = data_index_5d_min
        elif method == 'max':
            data_index_name = data_index_5d_max
    elif rule == '4D':
        if method == 'mean':
            data_index_name =  data_index_4d
        elif method == 'min':
            data_index_name = data_index_4d_min
        elif method == 'max':
            data_index_name = data_index_4d_max
    elif rule == '3D':
        if method == 'mean':
            data_index_name =  data_index_3d
        elif method == 'min':
            data_index_name = data_index_3d_min
        elif method == 'max':
            data_index_name = data_index_3d_max
    elif rule == '2D':
        if method == 'mean':
            data_index_name =  data_index_2d
        elif method == 'min':
            data_index_name = data_index_2d_min
        elif method == 'max':
            data_index_name = data_index_2d_max
    elif rule == 'D':
        if method == 'mean':
            data_index_name =  data_index_d
        elif method == 'min':
            data_index_name = data_index_d_min
        elif method == 'max':
            data_index_name = data_index_d_max
    elif rule == '12H':
        if method == 'mean':
            data_index_name =  data_index_12h
        elif method == 'min':
            data_index_name = data_index_12h_min
        elif method == 'max':
            data_index_name = data_index_12h_max
    elif rule == '8H':
        if method == 'mean':
            data_index_name =  data_index_8h
        elif method == 'min':
            data_index_name = data_index_8h_min
        elif method == 'max':
            data_index_name = data_index_8h_max
    elif rule == '6H':
        if method == 'mean':
            data_index_name =  data_index_6h
        elif method == 'min':
            data_index_name = data_index_6h_min
        elif method == 'max':
            data_index_name = data_index_6h_max
    elif rule == '3H':
        if method == 'mean':
            data_index_name =  data_index_3h
        elif method == 'min':
            data_index_name = data_index_3h_min
        elif method == 'max':
            data_index_name = data_index_3h_max
    elif rule == '2H':
        if method == 'mean':
            data_index_name =  data_index_2h
        elif method == 'min':
            data_index_name = data_index_2h_min
        elif method == 'max':
            data_index_name = data_index_2h_max
    elif rule == 'H':
        if method == 'mean':
            data_index_name =  data_index_h
        elif method == 'min':
            data_index_name = data_index_h_min
        elif method == 'max':
            data_index_name = data_index_h_max
    else:
        data_index_name = data_index_r

    return data_index_name


def time_to_str(time_min:str, time_max:str):
    """
    Replace ':' to '-' from the time variables.

    Parameters
    ----------
        time_min: str
            Time variable with ':'.
        time_max: str
            Time variable with ':'.

    Returns
    -------
        time_min_str: str
            Time variable with '-'.
        time_max_str: str
            Time varaible with '-'.
    """
    if time_min:
        time_min_str = time_min.replace(":", "-")
    else:
        time_min_str = time_min
    if time_max:
        time_max_str = time_max.replace(":", "-")
    else:
        time_max_str = time_max
    
    return time_min_str, time_max_str