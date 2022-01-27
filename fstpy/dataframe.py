# -*- coding: utf-8 -*-
import copy
import datetime
import logging
from typing import Final

import dask.array as da
import numpy as np
import pandas as pd
import pytz

from .std_dec import (VCONVERT_RMNDATE_TO_DATETIME, VCREATE_DATA_TYPE_STR,
                      VCREATE_FORECAST_HOUR, VCREATE_GRID_IDENTIFIER,
                      VCREATE_IP_INFO, VGET_UNIT_AND_DESCRIPTION,
                      VPARSE_ETIKET)
from .std_vgrid import set_vertical_coordinate_type

# import rpnpy.librmn.all as rmn




class MissingColumnError(Exception):
    pass


def add_grid_column(df: pd.DataFrame) -> pd.DataFrame:
    """Adds the grid column to the dataframe. The grid column is a simple identifier composed of ip1+ip2 or ig1+ig2 depending on the type of record (>>,^^,^>) vs regular field. 
    Replaces original column(s) if present.

    :param df: input dataframe
    :type df: pd.DataFrame
    :return: modified dataframe with the 'grid' column added
    :rtype: pd.DataFrame
    """
    for col in ['nomvar', 'ip1', 'ip2', 'ig1', 'ig2']:
        if col not in df.columns:
            raise MissingColumnError(f'"{col}" is missing from DataFrame columns, cannot add grid column!')

    new_df = copy.deepcopy(df.drop('grid', axis=1, errors='ignore'))
    new_df['grid'] = VCREATE_GRID_IDENTIFIER(new_df.nomvar, new_df.ip1, new_df.ip2, new_df.ig1, new_df.ig2)
    return new_df

def get_path_and_key_from_array(darr:'da.core.Array'):
    if not isinstance(darr,da.core.Array):
        return None, None
    graph = darr.__dask_graph__()
    graph_list = list(graph.to_dict())
    path_and_key = graph_list[0][0]
    if ':' in path_and_key:
        path_and_key = path_and_key.split(':')
        return path_and_key[0], path_and_key[1]
    else:
        return None, None

VPARSE_TASK_LIST = np.vectorize(get_path_and_key_from_array, otypes=['object','object'])

def add_path_and_key_columns(df: pd.DataFrame):
    """Adds the path and key columns to the dataframe.
    Replaces original column(s) if present.

    :param df: dataframe
    :type df: pd.DataFrame
    :return: path and key for each row
    :rtype: pd.DataFrame
    """
    if 'd' not in df.columns:
        raise MissingColumnError(f'"d" is missing from DataFrame columns, cannot add path and key column!') 

    new_df = copy.deepcopy(df.drop(['path','key'], axis=1, errors='ignore'))
    new_df['path'], new_df['key'] = VPARSE_TASK_LIST(new_df.d)
    return new_df


# get modifier information from the second character of typvar
def parse_typvar(typvar: str):
    multiple_modifications = False
    zapped = False
    filtered = False
    interpolated = False
    unit_converted = False
    bounded = False
    missing_data = False
    ensemble_extra_info = False
    if len(typvar) == 2:
        typvar2 = typvar[1]
        if (typvar2 == 'M'):
            # Il n'y a pas de façon de savoir quelle modif a ete faite
            multiple_modifications = True
        elif (typvar2 == 'Z'):
            zapped = True
        elif (typvar2 == 'F'):
            filtered = True
        elif (typvar2 == 'I'):
            interpolated = True
        elif (typvar2 == 'U'):
            unit_converted = True
        elif (typvar2 == 'B'):
            bounded = True
        elif (typvar2 == '?'):
            missing_data = True
        elif (typvar2 == '!'):
            ensemble_extra_info = True
    return multiple_modifications, zapped, filtered, interpolated, unit_converted, bounded, missing_data, ensemble_extra_info

VPARSE_TYPVAR: Final = np.vectorize(parse_typvar, otypes=['bool', 'bool', 'bool', 'bool', 'bool', 'bool', 'bool', 'bool'])  


class InvalidTimezoneError(Exception):
    pass



def convert_date_to_timezone(date: datetime.datetime, timezone: str) -> datetime.datetime:
    """Converts an utc date into the provided timezone

    :param date: input utc date
    :type date: datetime.datetime
    :param timezone: timezone string to convert date to
    :type timezone: str
    :raises InvalidTimezoneError: raised if given timezone is not valid
    :return: converted date
    :rtype: datetime.datetime
    """
    if timezone not in pytz.all_timezones:
        raise InvalidTimezoneError(f'Invalid timezone! valid timezones are\n{pytz.all_timezones}')
    else:
        if not pd.isnull(date):
            utc_timezone = pytz.timezone("UTC")
            with_timezone = utc_timezone.localize(pd.to_datetime(date))
            return with_timezone.astimezone(pytz.timezone(timezone)).replace(tzinfo=None)
        else:
            return date

VCONVERT_DATE_TO_TIMEZONE: Final = np.vectorize(convert_date_to_timezone)  # ,otypes=['datetime64']

class IndvalidDateColumnError(Exception):
    pass

def add_timezone_column(df: pd.DataFrame, source_column: str, timezone:str) -> pd.DataFrame:
    """Adds a timezone adjusted column for provided date (date_of_validity or date_of_observation)
    :param df: input dataframe
    :type df: pd.DataFrame
    :param source_column: either date_of_validity or date_of_observation
    :type source_column: str
    :param timezone: timezone name (valid timezone can be obtained from pytz.all_timezones)
    :type timezone: str
    :raises IndvalidDateColumnError: raised if source_column not in 'date_of_validity' or 'date_of_observation'
    :raises MissingColumnError: raised if source_column is not in dataframe
    :return: a new date adjusted timezone column
    :rtype: pd.DataFrame
    """
    if source_column not in ['date_of_validity', 'date_of_observation']:
        raise IndvalidDateColumnError(f'"{source_column}" not in {["date_of_validity", "date_of_observation"]}!') 

    if source_column not in df.columns:
        raise MissingColumnError(f'"{source_column}" is missing from DataFrame columns, cannot add timezone column!') 

    new_column = ''.join([source_column,'_',timezone])
    new_column = new_column.replace('/','_')
    new_df = copy.deepcopy(df.drop([new_column], axis=1, errors='ignore'))
    new_df[new_column] = VCONVERT_DATE_TO_TIMEZONE(new_df[source_column],timezone)
    return new_df

def add_flag_values(df: pd.DataFrame) -> pd.DataFrame:
    """Adds the correct flag values derived from parsing the typvar.
    Replaces original column(s) if present.

    :param df: dataframe
    :type df: pd.DataFrame
    :return: flag values set according to second character of typvar if present
    :rtype: pd.DataFrame
    """
    if 'typvar' not in df.columns:
        raise MissingColumnError(f'"typvar" is missing from DataFrame columns, cannot add flags columns!') 

    new_df = copy.deepcopy(df.drop(['multiple_modifications', 'zapped', 'filtered', 'interpolated', 'unit_converted', 'bounded', 'missing_data', 'ensemble_extra_info'], axis=1, errors='ignore'))
    new_df['multiple_modifications'], new_df['zapped'], new_df['filtered'], new_df['interpolated'], new_df['unit_converted'], new_df['bounded'], new_df['missing_data'], new_df['ensemble_extra_info'] = VPARSE_TYPVAR(new_df.typvar)
    return new_df



def drop_duplicates(df: pd.DataFrame):
    """Removes duplicate rows from dataframe.

    :param df: original dataframe
    :type df: pd.DataFrame
    :return: dataframe without duplicate rows
    :rtype: pd.DataFrame
    """
    init_row_count = len(df.index)
    columns = ['nomvar', 'typvar', 'etiket', 'ni', 'nj', 'nk', 'dateo',
               'ip1', 'ip2', 'ip3', 'deet', 'npas', 'datyp', 'nbits',
               'grtyp', 'ig1', 'ig3', 'ig4', 'datev']

    df.drop_duplicates(subset=columns, keep='first',inplace=True)

    row_count = len(df.index)
    if init_row_count != row_count:
        logging.warning('Found duplicate rows in dataframe!')
    
    return df    



def add_shape_column(df: pd.DataFrame) -> pd.DataFrame:
    """Adds the shape column from the ni and nj to a dataframe.
    Replaces original column(s) if present.

    :param df: dataframe
    :type df: pd.DataFrame
    :return: dataframe with label,run,implementation and ensemble_member columns 
             added
    :rtype: pd.DataFrame
    """
    for col in ['ni', 'nj']:
        if col not in df.columns:
            raise MissingColumnError(f'"{col}" is missing from DataFrame columns, cannot add shape column!') 

    new_df = copy.deepcopy(df.drop('shape', axis=1, errors='ignore'))
    new_df['shape'] = pd.Series(zip(new_df.ni.to_numpy(), new_df.nj.to_numpy()),dtype='object').to_numpy()
    return new_df


def add_parsed_etiket_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Adds label,run,implementation and ensemble_member columns from the parsed etikets to a dataframe.
    Replaces original column(s) if present.

    :param df: dataframe
    :type df: pd.DataFrame
    :return: dataframe with label,run,implementation and ensemble_member columns 
             added
    :rtype: pd.DataFrame
    """
    if 'etiket' not in df.columns:
        raise MissingColumnError(f'"etiket" is missing from DataFrame columns, cannot add parsed etiket columns!') 

    new_df = copy.deepcopy(df.drop(['label', 'run', 'implementation', 'ensemble_member'], axis=1, errors='ignore'))
    new_df['label'], new_df['run'], new_df['implementation'], new_df['ensemble_member'] = VPARSE_ETIKET(new_df.etiket)
    return new_df


def add_unit_and_description_columns(df: pd.DataFrame):
    """Adds unit and description from the nomvars to a dataframe.
    Replaces original column(s) if present.

    :param df: dataframe
    :type df: pd.DataFrame
    :return: dataframe with unit and description columns added
    :rtype: pd.DataFrame
    """
    if 'nomvar' not in df.columns:
        raise MissingColumnError(f'"nomvar" is missing from DataFrame columns, cannot add unit and description columns!') 

    new_df = copy.deepcopy(df.drop(['unit', 'description'], axis=1, errors='ignore'))
    new_df['unit'], new_df['description'] = VGET_UNIT_AND_DESCRIPTION(new_df.nomvar)
    return new_df


def add_decoded_date_column(df: pd.DataFrame, attr: str = 'dateo'):
    """Adds the decoded dateo or datev column to the dataframe.
    Replaces original column(s) if present.

    :param df: dataframe
    :type df: pd.DataFrame
    :param attr: selected date to decode, defaults to 'dateo'
    :type attr: str, optional
    :return: either date_of_observation or date_of_validity column added to the 
             dataframe
    :rtype: pd.DataFrame
    """

    if attr == 'dateo':
        if 'dateo' not in df.columns:
            raise MissingColumnError(f'"dateo" is missing from DataFrame columns, cannot add date_of_observation column!') 

        new_df = copy.deepcopy(df.drop('date_of_observation', axis=1, errors='ignore'))
        new_df['date_of_observation'] = VCONVERT_RMNDATE_TO_DATETIME(new_df.dateo)
    else:
        if 'datev' not in df.columns:
            raise MissingColumnError(f'"datev" is missing from DataFrame columns, cannot add date_of_validity column!') 

        new_df = copy.deepcopy(df.drop('date_of_validity', axis=1, errors='ignore'))
        new_df['date_of_validity'] = VCONVERT_RMNDATE_TO_DATETIME(new_df.datev)
    return new_df    



def add_forecast_hour_column(df: pd.DataFrame):
    """Adds the forecast_hour column derived from the deet and npas columns.
    Replaces original column(s) if present.

    :param df: dataframe
    :type df: pd.DataFrame
    :return: forecast_hour column added to the dataframe
    :rtype: pd.DataFrame
    """
    for col in ['deet', 'npas']:
        if col not in df.columns:
            raise MissingColumnError(f'"{col}" is missing from DataFrame columns, cannot add forecast_hour column!') 

    new_df = copy.deepcopy(df.drop('forecast_hour', axis=1, errors='ignore'))
    new_df['forecast_hour'] = VCREATE_FORECAST_HOUR(new_df.deet, new_df.npas)
    return new_df
    


def add_data_type_str_column(df: pd.DataFrame) -> pd.DataFrame:
    """Adds the data type decoded to string value column to the dataframe.
    Replaces original column(s) if present.

    :param df: dataframe
    :type df: pd.DataFrame
    :return: data_type_str column added to the dataframe
    :rtype: pd.DataFrame
    """
    if 'datyp' not in df.columns:
            raise MissingColumnError(f'"datyp" is missing from DataFrame columns, cannot add data_type_str column!') 

    new_df = copy.deepcopy(df.drop('data_type_str', axis=1, errors='ignore'))
    new_df['data_type_str'] = VCREATE_DATA_TYPE_STR(new_df.datyp)
    return new_df
    


def add_ip_info_columns(df: pd.DataFrame):
    """Adds all relevant level info from the ip1 column values.
    Replaces original column(s) if present.

    :param df: dataframe
    :type df: pd.DataFrame
    :return: level, ip1_kind, ip1_pkind,surface and follow_topography columns 
             added to the dataframe.
    :rtype: pd.DataFrame
    """
    for col in ['nomvar', 'ip1', 'ip2', 'ip3']:
        if col not in df.columns:
            raise MissingColumnError(f'"{col}" is missing from DataFrame columns, cannot add ip info columns!') 

    new_df = copy.deepcopy(df.drop(['level', 'ip1_kind', 'ip1_pkind', 'ip2_dec', 'ip2_kind', 'ip2_pkind', 'ip3_dec', 'ip3_kind', 'ip3_pkind', 'surface', 'follow_topography', 'ascending', 'interval'], axis=1, errors='ignore'))
    new_df['level'], new_df['ip1_kind'], new_df['ip1_pkind'], new_df['ip2_dec'], new_df['ip2_kind'], new_df['ip2_pkind'], new_df['ip3_dec'], new_df['ip3_kind'], new_df['ip3_pkind'], new_df['surface'], new_df['follow_topography'], new_df['ascending'], new_df['interval'] = VCREATE_IP_INFO(new_df.nomvar, new_df.ip1, new_df.ip2, new_df.ip3)
    return new_df



def add_columns(df: pd.DataFrame, columns: 'str|list[str]' = ['flags', 'etiket', 'unit', 'dateo', 'datev', 'forecast_hour', 'datyp', 'ip_info']):
    """If valid columns are provided, they will be added. 
       These include ['flags','etiket','unit','dateo','datev','forecast_hour', 'datyp','ip_info']
       Replaces original column(s) if present.   

    :param df: dataframe to modify (meta data needs to be present in dataframe)
    :type df: pd.DataFrame
    :param decode: if decode is True, add the specified columns
    :type decode: bool
    :param columns: [description], defaults to  ['flags','etiket','unit','dateo','datev','forecast_hour', 'datyp','ip_info']
    :type columns: list[str], optional
    """
    cols = ['flags', 'etiket', 'unit', 'dateo', 'datev', 'forecast_hour', 'datyp', 'ip_info']
    if isinstance(columns,str):
        columns = [columns]
    
    for col in columns:
        if col not in cols:
            logging.warning(f'{col} not found in {cols}')

    if 'etiket' in columns:
        df = add_parsed_etiket_columns(df)

    if 'unit' in columns:
        df = add_unit_and_description_columns(df)

    if 'dateo' in columns:
        df = add_decoded_date_column(df, 'dateo')

    if 'datev' in columns:
        df = add_decoded_date_column(df, 'datev')

    if 'forecast_hour' in columns:
        df = add_forecast_hour_column(df)

    if 'datyp' in columns:
        df = add_data_type_str_column(df)

    if ('ip_info' in columns):
        df = add_ip_info_columns(df)
        df = set_vertical_coordinate_type(df)

    if 'flags' in columns:
        df = add_flag_values(df)

    return df    

    


def reorder_columns(df):
    ordered = ['nomvar', 'typvar', 'etiket', 'ni', 'nj', 'nk', 'dateo', 'ip1', 'ip2',
               'ip3', 'deet', 'npas', 'datyp', 'nbits', 'grtyp', 'ig1', 'ig2', 'ig3', 'ig4']
    if df.empty:
        return 
    all_columns = set(df.columns)
    # all_columns = set(df.columns.to_list())

    extra_columns = all_columns.difference(set(ordered))
    if len(extra_columns) > 0:
        ordered.extend(list(extra_columns))

    df = df[ordered]


def get_meta_fields_exists(grid):
    toctoc = grid.loc[grid.nomvar == "!!"]
    vcode = []
    if not toctoc.empty:
        for row in toctoc.itertuples():
            vcode.append(row.ig1)
        toctoc = True
    else:
        vcode.append(-1)
        toctoc = False
    p0 = meta_exists(grid, "P0")
    e1 = meta_exists(grid, "E1")
    pt = meta_exists(grid, "PT")
    hy = meta_exists(grid, "HY")
    sf = meta_exists(grid, "!!SF")
    return toctoc, p0, e1, pt, hy, sf, vcode


def meta_exists(grid, nomvar) -> bool:
    df = grid.loc[grid.nomvar == nomvar]
    return not df.empty

def create_empty_dataframe(num_rows):
    record = {
        'nomvar': ' ',
        'typvar': 'P',
        'etiket': ' ',
        'ni': 1,
        'nj': 1,
        'nk': 1,
        'dateo': 0,
        'ip1': 0,
        'ip2': 0,
        'ip3': 0,
        'deet': 0,
        'npas': 0,
        'datyp': 133,
        'nbits': 16,
        'grtyp': 'G',
        'ig1': 0,
        'ig2': 0,
        'ig3': 0,
        'ig4': 0,
        'datev': 0,
        'd':None
        }
    df =  pd.DataFrame([record for _ in range(num_rows)])
    return df

