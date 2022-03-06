import math
import mooda as md
from os import listdir
from os.path import isfile, join
import pandas as pd
import datetime

from service.email_service import send_to_admin
from service.ingestion_service import (post_metadata, post_vocabulary,
                                       post_data_index)
from service.fig_service import (delete_scatter, delete_platform_pie,
                                 delete_platform_availability,
                                 delete_parameter_availability, delete_map,
                                 delete_parameter_pie)

from config import (auto_upload_folder, data_index_m, data_index_m_max,
                    data_index_m_min, data_index_15d,
                    data_index_15d_max, data_index_15d_min,
                    data_index_10d, data_index_10d_max, data_index_10d_min,
                    data_index_6d, data_index_6d_max,
                    data_index_6d_min, data_index_5d, data_index_5d_max,
                    data_index_5d_min, data_index_4d, data_index_4d_max,
                    data_index_4d_min, data_index_3d, data_index_3d_max,
                    data_index_3d_min, data_index_2d, data_index_2d_max,
                    data_index_2d_min, data_index_d, data_index_d_max,
                    data_index_d_min, data_index_12h, data_index_12h_max,
                    data_index_12h_min, data_index_8h, data_index_8h_max,
                    data_index_8h_min, data_index_6h, data_index_6h_max,
                    data_index_6h_min, data_index_3h, data_index_3h_max,
                    data_index_3h_min, data_index_2h, data_index_2h_max,
                    data_index_2h_min, data_index_h, data_index_h_max,
                    data_index_h_min, data_index_r)

ingestion_raw = False
ingestion_d = False
ingestion_3d = False
ingestion_16d = False

ingestion_m = True
ingestion_m_max = True
ingestion_m_min = True
# ingestion_m_std = True

ingestion_15d = True
ingestion_15d_max = True
ingestion_15d_min = True
# ingestion_15d_std = True

ingestion_10d = True
ingestion_10d_max = True
ingestion_10d_min = True
# ingestion_10d_std = False

ingestion_6d = True
ingestion_6d_max = True
ingestion_6d_min = True
# ingestion_6d_std = False

ingestion_5d = True
ingestion_5d_max = True
ingestion_5d_min = True
# ingestion_5d_std = False

ingestion_4d = True
ingestion_4d_max = True
ingestion_4d_min = True
# ingestion_4d_std = False

ingestion_3d = True
ingestion_3d_max = True
ingestion_3d_min = True
# ingestion_3d_std = False

ingestion_2d = True
ingestion_2d_max = True
ingestion_2d_min = True
# ingestion_2d_std = False

ingestion_d = True
ingestion_d_max = True
ingestion_d_min = True
# ingestion_d_std = False

ingestion_12h = True
ingestion_12h_max = True
ingestion_12h_min = True

ingestion_8h = True
ingestion_8h_max = True
ingestion_8h_min = True

ingestion_6h = True
ingestion_6h_max = True
ingestion_6h_min = True

ingestion_3h = True
ingestion_3h_max = True
ingestion_3h_min = True

ingestion_2h = True
ingestion_2h_max = True
ingestion_2h_min = True

ingestion_h = True
ingestion_h_max = True
ingestion_h_min = True

ingestion_r = True

def resample_wf(wf_in, rule, method='mean'):
    """
    Convenience method for frequency conversion and sampling of time series of the WaterFrame
    object. Warning: if WaterFrame.data contains MultiIndex, those indexes will disappear, obtaining a single 'TIME'
    index.

    Parameters
    ----------
        rule: str
            The offset string or object representing target conversion.
            You can find all of the resample options here:
            http://pandas.pydata.org/pandas-docs/stable/timeseries.html#offset-aliases
        method: "mean", "max", "min", optional, (method = "mean")
            Save the new value with the mean(), max() or min() function.
        inplace: bool
            If True, resample in place and return 'True', If False, return a new WaterFrame.

    Returns
    -------
        new_wf: WaterFrame
    """
    if isinstance(wf_in.data.index,
        pd.MultiIndex) and 'DEPTH' in wf_in.data.index.names:
    
        # pd.Grouper allows you to specify a "groupby instruction for a target
        # object".
        # Ref: https://stackoverflow.com/questions/15799162/resampling-within-a-pandas-multiindex
        level_values = wf_in.data.index.get_level_values

        if method == "mean":
            data = (
                wf_in.data.groupby(
                    [level_values(0)] +  # 0 is 'DEPTH'
                    [pd.Grouper(freq=rule, level='TIME')]).mean())
        elif method == "max":
            data = (
                wf_in.data.groupby(
                    [level_values(0)] +  # 0 is 'DEPTH'
                    [pd.Grouper(freq=rule, level='TIME')]).max())
        elif method == "min":
            data = (
                wf_in.data.groupby(
                    [level_values(0)] +  # 0 is 'DEPTH'
                    [pd.Grouper(freq=rule, level='TIME')]).min())
        elif method == "std":
            data = (
                wf_in.data.groupby(
                    [level_values(0)] +  # 0 is 'DEPTH'
                    [pd.Grouper(freq=rule, level='TIME')]).std(axis=1))
    else:
        # Legacy
        if method == "mean":
            data = wf_in.data.resample(rule, level='TIME').mean()
        elif method == "max":
            data = wf_in.data.resample(rule, level='TIME').max()
        elif method == "min":
            data = wf_in.data.resample(rule, level='TIME').min()
        elif method == "std":
            data = wf_in.data.resample(rule, level='TIME').std()

    # Change "_QC" values to 0
    for key in wf_in.data.keys():
        if "_QC" in key:
            data[key] = 0

    
    new_wf = wf_in.copy()
    new_wf.data = data
    return new_wf

def ingestion_wf(wf_in, index_name):
    data = wf_in.data
    metadata = wf_in.metadata

    for param in wf_in.parameters:

        print(metadata['platform_code'], param)
        # send_to_admin(f'START - Ingestion nc EmodNet {one_file} - {param}',
        #             f'Start ingestion nc EmodNet {one_file} - {param}')

        data_param = data[[param, f'{param}_QC', 'TIME_QC', 'DEPTH_QC']]

        for index, row in data_param.iterrows():
            if math.isnan(row[param]):
                continue
            ingestion_data = {
                'platform_code': metadata['platform_code'],
                'parameter': param,
                # 'time': str(index[1]), df['column_name'].dt.strftime('%Y-%m-%d')
                'time': index[1].strftime("%Y-%m-%dT%H:%M:%SZ"),
                'time_qc': str(row['TIME_QC']),
                'lat': metadata['last_latitude_observation'],
                'lat_qc': '0',
                'lon': metadata['last_longitude_observation'],
                'lon_qc': '0',
                'depth': index[0],
                'depth_qc': str(row['DEPTH_QC']),
                'value': row[param],
                'qc': row[f'{param}_QC']}

            response, status_code = post_data_index(ingestion_data, index_name)
            if status_code != 201:
                send_to_admin(
                    f'ERROR {status_code} - Ingestion nc EmodNet {one_file} - {param}',
                    f'{response.get("message")}')
        
        delete_parameter_availability(param)

onlyfiles = [
    f for f in listdir(auto_upload_folder) if isfile(join(auto_upload_folder, f))]

# Read ingestion log
lines = []
try:
    with open("ingestion_log.txt") as file:
        lines = file.readlines()
        stripped_lines = [j.strip() for j in lines]
        lines = stripped_lines
except FileNotFoundError:
    pass

for one_file in onlyfiles:

    print(one_file)

    if f'{one_file} R' in lines:
        # This file is already ingested
        continue

    file_path = auto_upload_folder + f'/{one_file}'

    wf = md.read_nc_emodnet(file_path)

    platform_code = wf.metadata['platform_code']
    metadata = wf.metadata
    metadata['parameters'] = wf.parameters

    if 'ESTOC' in metadata['platform_code']:
        try:
            del wf.vocabulary['TIME']['QC_indicator']
        except KeyError:
            pass
        try:
            del wf.vocabulary['TIME']['QC_procedure']
        except KeyError:
            pass
        wf.data['TIME_QC'] = 0
        wf.data['DEPTH_QC'] = 0
        wf.metadata[
            'last_latitude_observation'] = wf.metadata['geospatial_lat_max']
        wf.metadata[
            'last_longitude_observation'] = wf.metadata['geospatial_lon_max']

    wf_m = wf.copy()
    wf_m.resample('M')
    if ingestion_m and f'{one_file} M' not in lines:
        # Metadata and vocabulary ingestion, only this time
        post_metadata(platform_code, metadata)
        vocabulary = wf.vocabulary.copy()
        post_vocabulary(platform_code, vocabulary)

        # Data ingestion
        ingestion_wf(wf_m, data_index_m)
        # Write file into log
        with open("ingestion_log.txt", "a") as file:
            file.write(f"{one_file} M\n")
            # now = datetime.datetime.now()
            # file.write(f"{str(now)}\n")

    wf_m_max = wf.copy()
    wf_m_max.resample('M', 'max')
    if ingestion_m_max and f'{one_file} M_MAX' not in lines:
        ingestion_wf(wf_m_max, data_index_m_max)
        # Write file into log
        with open("ingestion_log.txt", "a") as file:
            file.write(f"{one_file} M_MAX\n")
            # now = datetime.datetime.now()
            # file.write(f"{str(now)}\n")

    wf_m_min = wf.copy()
    wf_m_min.resample('M', 'min')
    if ingestion_m_min and f'{one_file} M_MIN' not in lines:
        ingestion_wf(wf_m_min, data_index_m_min)
        with open("ingestion_log.txt", "a") as file:
            file.write(f"{one_file} M_MIN\n")
            # now = datetime.datetime.now()
            # file.write(f"{str(now)}\n")

    wf_15d = wf.copy()
    wf_15d.resample('15D')
    if ingestion_15d and f'{one_file} 15D' not in lines:
        ingestion_wf(wf_15d, data_index_15d)
        with open("ingestion_log.txt", "a") as file:
            file.write(f"{one_file} 15D\n")
            # now = datetime.datetime.now()
            # file.write(f"{str(now)}\n")

    wf_15d_max = wf.copy()
    wf_15d_max.resample('15D', 'max')
    if ingestion_15d_max and f'{one_file} 15D_MAX' not in lines:
        print('15D MAX -------------------')
        ingestion_wf(wf_15d_max, data_index_15d_max)
        with open("ingestion_log.txt", "a") as file:
            file.write(f"{one_file} 15D_MAX\n")
            # now = datetime.datetime.now()
            # file.write(f"{str(now)}\n")

    wf_15d_min = wf.copy()
    wf_15d_min.resample('15D', 'min')
    if ingestion_15d_min and f'{one_file} 15D_MIN' not in lines:
        print('15D MIN -------------------')
        ingestion_wf(wf_15d_min, data_index_15d_min)
        with open("ingestion_log.txt", "a") as file:
            file.write(f"{one_file} 15D_MIN\n")
            # now = datetime.datetime.now()
            # file.write(f"{str(now)}\n")

    wf_10d = wf.copy()
    wf_10d.resample('10D')
    if ingestion_10d and f'{one_file} 10D' not in lines:
        print('10D -------------------')
        ingestion_wf(wf_10d, data_index_10d)
        with open("ingestion_log.txt", "a") as file:
            file.write(f"{one_file} 10D\n")
            # now = datetime.datetime.now()
            # file.write(f"{str(now)}\n")

    wf_10d_max = wf.copy()
    wf_10d_max.resample('10D', 'max')
    if ingestion_10d_max and f'{one_file} 10D_MAX' not in lines:
        print('10D MAX -------------------')
        ingestion_wf(wf_10d_max, data_index_10d_max)
        with open("ingestion_log.txt", "a") as file:
            file.write(f"{one_file} 10D_MAX\n")
            # now = datetime.datetime.now()
            # file.write(f"{str(now)}\n")

    wf_10d_min = wf.copy()
    wf_10d_min.resample('10D', 'min')
    if ingestion_10d_min and f'{one_file} 10D_MIN' not in lines:
        print('10D MIN -------------------')
        ingestion_wf(wf_10d_min, data_index_10d_min)
        with open("ingestion_log.txt", "a") as file:
            file.write(f"{one_file} 10D_MIN\n")
            # now = datetime.datetime.now()
            # file.write(f"{str(now)}\n")

    wf_6d = wf.copy()
    wf_6d.resample('6D')
    if ingestion_6d and f'{one_file} 6D' not in lines:
        print('6D -------------------')
        ingestion_wf(wf_6d, data_index_6d)
        with open("ingestion_log.txt", "a") as file:
            file.write(f"{one_file} 6D\n")
            # now = datetime.datetime.now()
            # file.write(f"{str(now)}\n")

    wf_6d_max = wf.copy()
    wf_6d_max.resample('6D', 'max')
    if ingestion_6d_max and f'{one_file} 6D_MAX' not in lines:
        print('6D MAX -------------------')
        ingestion_wf(wf_6d_max, data_index_6d_max)
        with open("ingestion_log.txt", "a") as file:
            file.write(f"{one_file} 6D_MAX\n")
            # now = datetime.datetime.now()
            # file.write(f"{str(now)}\n")

    wf_6d_min = wf.copy()
    wf_6d_min.resample('6D', 'min')
    if ingestion_6d_min and f'{one_file} 6D_MIN' not in lines:
        print('6D MIN -------------------')
        ingestion_wf(wf_6d_min, data_index_6d_min)
        with open("ingestion_log.txt", "a") as file:
            file.write(f"{one_file} 6D_MIN\n")
            # now = datetime.datetime.now()
            # file.write(f"{str(now)}\n")

    wf_5d = wf.copy()
    wf_5d.resample('5D')
    if ingestion_5d and f'{one_file} 5D' not in lines:
        print('5D -------------------')
        ingestion_wf(wf_5d, data_index_5d)
        with open("ingestion_log.txt", "a") as file:
            file.write(f"{one_file} 5D\n")
            # now = datetime.datetime.now()
            # file.write(f"{str(now)}\n")

    wf_5d_max = wf.copy()
    wf_5d_max.resample('5D', 'max')
    if ingestion_5d_max and f'{one_file} 5D_MAX' not in lines:
        print('5D MAX -------------------')
        ingestion_wf(wf_5d_max, data_index_5d_max)
        with open("ingestion_log.txt", "a") as file:
            file.write(f"{one_file} 5D_MAX\n")
            # now = datetime.datetime.now()
            # file.write(f"{str(now)}\n")

    wf_5d_min = wf.copy()
    wf_5d_min.resample('5D', 'min')
    if ingestion_5d_min and f'{one_file} 5D_MIN' not in lines:
        print('5D MIN -------------------')
        ingestion_wf(wf_5d_min, data_index_5d_min)
        with open("ingestion_log.txt", "a") as file:
            file.write(f"{one_file} 5D_MIN\n")
            # now = datetime.datetime.now()
            # file.write(f"{str(now)}\n")

    wf_4d = wf.copy()
    wf_4d.resample('4D')
    if ingestion_4d and f'{one_file} 4D' not in lines:
        print('4D -------------------')
        ingestion_wf(wf_4d, data_index_4d)
        with open("ingestion_log.txt", "a") as file:
            file.write(f"{one_file} 4D\n")
            # now = datetime.datetime.now()
            # file.write(f"{str(now)}\n")

    wf_4d_max = wf.copy()
    wf_4d_max.resample('4D', 'max')
    if ingestion_4d_max and f'{one_file} 4D_MAX' not in lines:
        print('4D MAX -------------------')
        ingestion_wf(wf_4d_max, data_index_4d_max)
        with open("ingestion_log.txt", "a") as file:
            file.write(f"{one_file} 4D_MAX\n")
            # now = datetime.datetime.now()
            # file.write(f"{str(now)}\n")

    wf_4d_min = wf.copy()
    wf_4d_min.resample('4D', 'min')
    if ingestion_4d_min and f'{one_file} 4D_MIN' not in lines:
        print('4D MIN -------------------')
        ingestion_wf(wf_4d_min, data_index_4d_min)
        with open("ingestion_log.txt", "a") as file:
            file.write(f"{one_file} 4D_MIN\n")
            # now = datetime.datetime.now()
            # file.write(f"{str(now)}\n")

    wf_3d = wf.copy()
    wf_3d.resample('3D')
    if ingestion_3d and f'{one_file} 3D' not in lines:
        print('3D -------------------')
        ingestion_wf(wf_3d, data_index_3d)
        with open("ingestion_log.txt", "a") as file:
            file.write(f"{one_file} 3D\n")
            # now = datetime.datetime.now()
            # file.write(f"{str(now)}\n")

    wf_3d_max = wf.copy()
    wf_3d_max.resample('3D', 'max')
    if ingestion_3d_max and f'{one_file} 3D_MAX' not in lines:
        print('3D MAX -------------------')
        ingestion_wf(wf_3d_max, data_index_3d_max)
        with open("ingestion_log.txt", "a") as file:
            file.write(f"{one_file} 3D_MAX\n")
            # now = datetime.datetime.now()
            # file.write(f"{str(now)}\n")

    wf_3d_min = wf.copy()
    wf_3d_min.resample('3D', 'min')
    if ingestion_3d_min and f'{one_file} 3D_MIN' not in lines:
        print('3D MIN -------------------')
        ingestion_wf(wf_3d_min, data_index_3d_min)
        with open("ingestion_log.txt", "a") as file:
            file.write(f"{one_file} 3D_MIN\n")
            # now = datetime.datetime.now()
            # file.write(f"{str(now)}\n")

    wf_2d = wf.copy()
    wf_2d.resample('2D')
    if ingestion_2d and f'{one_file} 2D' not in lines:
        print('2D -------------------')
        ingestion_wf(wf_2d, data_index_2d)
        with open("ingestion_log.txt", "a") as file:
            file.write(f"{one_file} 2D\n")
            # now = datetime.datetime.now()
            # file.write(f"{str(now)}\n")

    wf_2d_max = wf.copy()
    wf_2d_max.resample('2D', 'max')
    if ingestion_2d_max and f'{one_file} 2D_MAX' not in lines:
        print('2D MAX -------------------')
        ingestion_wf(wf_2d_max, data_index_2d_max)
        with open("ingestion_log.txt", "a") as file:
            file.write(f"{one_file} 2D_MAX\n")
            # now = datetime.datetime.now()
            # file.write(f"{str(now)}\n")

    wf_2d_min = wf.copy()
    wf_2d_min.resample('2D', 'min')
    if ingestion_2d_min and f'{one_file} 2D_MIN' not in lines:
        print('2D MIN -------------------')
        ingestion_wf(wf_2d_min, data_index_2d_min)
        with open("ingestion_log.txt", "a") as file:
            file.write(f"{one_file} 2D_MIN\n")
            # now = datetime.datetime.now()
            # file.write(f"{str(now)}\n")

    wf_d = wf.copy()
    wf_d.resample('D')
    if ingestion_d and f'{one_file} D' not in lines:
        print('D -------------------')
        ingestion_wf(wf_d, data_index_d)
        with open("ingestion_log.txt", "a") as file:
            file.write(f"{one_file} D\n")
            # now = datetime.datetime.now()
            # file.write(f"{str(now)}\n")

    wf_d_max = wf.copy()
    wf_d_max.resample('D', 'max')
    if ingestion_d_max and f'{one_file} D_MAX' not in lines:
        print('D MAX -------------------')
        ingestion_wf(wf_d_max, data_index_d_max)
        with open("ingestion_log.txt", "a") as file:
            file.write(f"{one_file} D_MAX\n")
            # now = datetime.datetime.now()
            # file.write(f"{str(now)}\n")

    wf_d_min = wf.copy()
    wf_d_min.resample('D', 'min')
    if ingestion_d_min and f'{one_file} D_MIN' not in lines:
        print('D MIN -------------------')
        ingestion_wf(wf_d_min, data_index_d_min)
        with open("ingestion_log.txt", "a") as file:
            file.write(f"{one_file} D_MIN\n")
            # now = datetime.datetime.now()
            # file.write(f"{str(now)}\n")

    wf_12h = wf.copy()
    wf_12h.resample('12H')
    if ingestion_12h and f'{one_file} 12H' not in lines:
        print('12H -------------------')
        ingestion_wf(wf_12h, data_index_12h)
        with open("ingestion_log.txt", "a") as file:
            file.write(f"{one_file} 12H\n")
            # now = datetime.datetime.now()
            # file.write(f"{str(now)}\n")

    wf_12h_max = wf.copy()
    wf_12h_max.resample('12H', 'max')
    if ingestion_12h_max and f'{one_file} 12H_MAX' not in lines:
        print('12H MAX -------------------')
        ingestion_wf(wf_12h_max, data_index_12h_max)
        with open("ingestion_log.txt", "a") as file:
            file.write(f"{one_file} 12H_MAX\n")
            # now = datetime.datetime.now()
            # file.write(f"{str(now)}\n")

    wf_12h_min = wf.copy()
    wf_12h_min.resample('12H', 'min')
    if ingestion_12h_min and f'{one_file} 12H_MIN' not in lines:
        print('12H MIN -------------------')
        ingestion_wf(wf_12h_min, data_index_12h_min)
        with open("ingestion_log.txt", "a") as file:
            file.write(f"{one_file} 12H_MIN\n")
            # now = datetime.datetime.now()
            # file.write(f"{str(now)}\n")

    wf_8h = wf.copy()
    wf_8h.resample('8H')
    if ingestion_8h and f'{one_file} 8H' not in lines:
        print('8H -------------------')
        ingestion_wf(wf_8h, data_index_8h)
        with open("ingestion_log.txt", "a") as file:
            file.write(f"{one_file} 8H\n")
            # now = datetime.datetime.now()
            # file.write(f"{str(now)}\n")

    wf_8h_max = wf.copy()
    wf_8h_max.resample('8H', 'max')
    if ingestion_8h_max and f'{one_file} 8H_MAX' not in lines:
        print('8H MAX -------------------')
        ingestion_wf(wf_8h_max, data_index_8h_max)
        with open("ingestion_log.txt", "a") as file:
            file.write(f"{one_file} 8H_MAX\n")
            # now = datetime.datetime.now()
            # file.write(f"{str(now)}\n")

    wf_8h_min = wf.copy()
    wf_8h_min.resample('8H', 'min')
    if ingestion_8h_min and f'{one_file} 8H_MIN' not in lines:
        print('8H MIN -------------------')
        ingestion_wf(wf_8h_min, data_index_8h_min)
        with open("ingestion_log.txt", "a") as file:
            file.write(f"{one_file} 8H_MIN\n")
            # now = datetime.datetime.now()
            # file.write(f"{str(now)}\n")
    
    wf_6h = wf.copy()
    wf_6h.resample('6H')
    if ingestion_6h and f'{one_file} 6H' not in lines:
        print('6H -------------------')
        ingestion_wf(wf_6h, data_index_6h)
        with open("ingestion_log.txt", "a") as file:
            file.write(f"{one_file} 6H\n")
            # now = datetime.datetime.now()
            # file.write(f"{str(now)}\n")

    wf_6h_max = wf.copy()
    wf_6h_max.resample('6H', 'max')
    if ingestion_6h_max and f'{one_file} 6H_MAX' not in lines:
        print('6H MAX -------------------')
        ingestion_wf(wf_6h_max, data_index_6h_max)
        with open("ingestion_log.txt", "a") as file:
            file.write(f"{one_file} 6H_MAX\n")
            # now = datetime.datetime.now()
            # file.write(f"{str(now)}\n")

    wf_6h_min = wf.copy()
    wf_6h_min.resample('6H', 'min')
    if ingestion_6h_min and f'{one_file} 6H_MIN' not in lines:
        print('6H MIN -------------------')
        ingestion_wf(wf_6h_min, data_index_6h_min)
        with open("ingestion_log.txt", "a") as file:
            file.write(f"{one_file} 6H_MIN\n")
            # now = datetime.datetime.now()
            # file.write(f"{str(now)}\n")

    wf_3h = wf.copy()
    wf_3h.resample('3H')
    if ingestion_3h and f'{one_file} 3H' not in lines:
        print('3H -------------------')
        ingestion_wf(wf_3h, data_index_3h)
        with open("ingestion_log.txt", "a") as file:
            file.write(f"{one_file} 3H\n")
            # now = datetime.datetime.now()
            # file.write(f"{str(now)}\n")

    wf_3h_max = wf.copy()
    wf_3h_max.resample('3H', 'max')
    if ingestion_3h_max and f'{one_file} 3H_MAX' not in lines:
        print('3H MAX -------------------')
        ingestion_wf(wf_3h_max, data_index_3h_max)
        with open("ingestion_log.txt", "a") as file:
            file.write(f"{one_file} 3H_MAX\n")
            # now = datetime.datetime.now()
            # file.write(f"{str(now)}\n")

    wf_3h_min = wf.copy()
    wf_3h_min.resample('3H', 'min')
    if ingestion_3h_min and f'{one_file} 3H_MIN' not in lines:
        print('3H MIN -------------------')
        ingestion_wf(wf_3h_min, data_index_3h_min)
        with open("ingestion_log.txt", "a") as file:
            file.write(f"{one_file} 3H_MIN\n")
            # now = datetime.datetime.now()
            # file.write(f"{str(now)}\n")

    wf_2h = wf.copy()
    wf_2h.resample('2H')
    if ingestion_2h and f'{one_file} 2H' not in lines:
        print('2H -------------------')
        ingestion_wf(wf_2h, data_index_2h)
        with open("ingestion_log.txt", "a") as file:
            file.write(f"{one_file} 2H\n")
            # now = datetime.datetime.now()
            # file.write(f"{str(now)}\n")

    wf_2h_max = wf.copy()
    wf_2h_max.resample('2H', 'max')
    if ingestion_2h_max and f'{one_file} 2H_MAX' not in lines:
        print('2H MAX -------------------')
        ingestion_wf(wf_2h_max, data_index_2h_max)
        with open("ingestion_log.txt", "a") as file:
            file.write(f"{one_file} 2H_MAX\n")
            # now = datetime.datetime.now()
            # file.write(f"{str(now)}\n")

    wf_2h_min = wf.copy()
    wf_2h_min.resample('2H', 'min')
    if ingestion_2h_min and f'{one_file} 2H_MIN' not in lines:
        print('2H MIN -------------------')
        ingestion_wf(wf_2h_min, data_index_2h_min)
        with open("ingestion_log.txt", "a") as file:
            file.write(f"{one_file} 2H_MIN\n")
            # now = datetime.datetime.now()
            # file.write(f"{str(now)}\n")

    wf_h = wf.copy()
    wf_h.resample('H')
    if ingestion_h and f'{one_file} H' not in lines:
        print('H -------------------')
        ingestion_wf(wf_h, data_index_h)
        with open("ingestion_log.txt", "a") as file:
            file.write(f"{one_file} H\n")
            # now = datetime.datetime.now()
            # file.write(f"{str(now)}\n")

    wf_h_max = wf.copy()
    wf_h_max.resample('H', 'max')
    if ingestion_h_max and f'{one_file} H_MAX' not in lines:
        print('H MAX -------------------')
        ingestion_wf(wf_h_max, data_index_h_max)
        with open("ingestion_log.txt", "a") as file:
            file.write(f"{one_file} H_MAX\n")
            # now = datetime.datetime.now()
            # file.write(f"{str(now)}\n")

    wf_h_min = wf.copy()
    wf_h_min.resample('H', 'min')
    if ingestion_h_min and f'{one_file} H_MIN' not in lines:
        print('H MIN -------------------')
        ingestion_wf(wf_h_min, data_index_h_min)
        with open("ingestion_log.txt", "a") as file:
            file.write(f"{one_file} H_MIN\n")
            # now = datetime.datetime.now()
            # file.write(f"{str(now)}\n")

    if ingestion_r and f'{one_file} R' not in lines:
        print('R -------------------')
        try:
            ingestion_wf(wf, data_index_r)
            with open("ingestion_log.txt", "a") as file:
                file.write(f"{one_file} R\n")
                # now = datetime.datetime.now()
                # file.write(f"{str(now)}\n")
        except:
            print('ERROR')

    # Delete figures
    delete_scatter(platform_code)
    delete_platform_pie()
    delete_platform_availability(platform_code)
    delete_map()
    delete_parameter_pie()
