# -*- coding: utf-8 -*-convertIp

from typing import Final
import copy
import warnings
import dask.array as da
import datetime
import logging
import numpy as np
import pandas as pd
import pytz
import rpnpy.librmn.all as rmn
from rpnpy.rpndate import RPNDate

from fstpy.std_enc import create_encoded_standard_etiket
from fstpy.std_dec import VCREATE_DATA_TYPE_STR, VCREATE_FORECAST_HOUR,                                  \
                          VCREATE_GRID_IDENTIFIER, VCONVERT_RMNDATE_TO_DATETIME,                         \
                          VCREATE_IP_INFO, VGET_UNIT_AND_DESCRIPTION, VGET_UNIT,                         \
                          VGET_DESCRIPTION, VPARSE_ETIKET
from fstpy.std_vgrid import set_vertical_coordinate_type
from fstpy.utils import vectorize

class MissingColumnError(Exception):
    pass

class UnexpectedValueError(Exception):
    pass

def add_grid_column(df: pd.DataFrame) -> pd.DataFrame:
    """Adds the grid column to the dataframe. The grid column is a simple identifier composed of ip1+ip2 or ig1+ig2 depending on the type of record (>>,^^,^>) vs regular field. 
    Replaces original column(s) if present.

    :param df: input dataframe
    :type df: pd.DataFrame
    :return: modified dataframe with the 'grid' column added
    :rtype: pd.DataFrame
    """
    if df.empty:
        return df
    for col in ['nomvar', 'ip1', 'ip2', 'ig1', 'ig2']:
        if col not in df.columns:
            raise MissingColumnError(f'"{col}" is missing from DataFrame columns, cannot add grid column!')

    for col in ['nomvar', 'ip1', 'ip2', 'ig1', 'ig2']:
        if df[col].isna().any():
            raise MissingColumnError(f'A "{col}" value is missing from {col} DataFrame column, cannot add grid column!')

    new_df = copy.deepcopy(df)
    if 'grid' not in new_df.columns:
        new_df['grid'] = VCREATE_GRID_IDENTIFIER(new_df.nomvar, new_df.ip1, new_df.ip2, new_df.ig1, new_df.ig2)
    else:
        mask = new_df.grid.isna()
        if mask.any():
            grid = VCREATE_GRID_IDENTIFIER(new_df.loc[mask, 'nomvar'],
                                           new_df.loc[mask, 'ip1'], new_df.loc[mask,'ip2'],
                                           new_df.loc[mask, 'ig1'], new_df.loc[mask,'ig2'])
            new_df.loc[mask, 'grid'] = grid

    return new_df

def get_path_and_key_from_array(darr:'da.core.Array'):
    """Gets the path and key tuple from the dask array

    :param darr: dask array to get info from
    :type darr: da.core.Array
    :return: tuple of path and key
    :rtype: Tuple(str,int)
    """
    if not isinstance(darr,da.core.Array):
        return None, None
    graph = darr.__dask_graph__()
    graph_list = list(graph.to_dict())
    path_and_key = graph_list[0][0]
    if ':' in path_and_key:
        path_and_key = path_and_key.split(':')
        return path_and_key[0], int(path_and_key[1])
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
    if df.empty:
        return df

    if 'd' not in df.columns:
        raise MissingColumnError(f'"d" is missing from DataFrame columns, cannot add path and key column!') 

    if df.d.isna().any():
        raise MissingColumnError(f'A "d" value is missing from d DataFrame column, cannot add path and key column!') 

    new_df = copy.deepcopy(df)
    if ('path' not in new_df.columns) or ('key' not in new_df.columns):
        new_df['path'], new_df['key'] = VPARSE_TASK_LIST(new_df.d)
    else:
        if not new_df.loc[new_df.path.isna()].empty:
            paths, _ = VPARSE_TASK_LIST(new_df.loc[new_df.path.isna()].d)
            new_df.loc[new_df.path.isna(),'path'] = paths
        if not new_df.loc[new_df.key.isna()].empty:
            _, keys = VPARSE_TASK_LIST(new_df.loc[new_df.key.isna()].d)
            new_df.loc[new_df.key.isna(),'key'] = keys
    return new_df

# get modifier information from the second character of typvar
def parse_typvar(typvar: str):
    """Get the modifier information from the second character of typvar

    :param typvar: 2 character string
    :type typvar: str
    :return: a series of bools corresponding to the second letter interpretation
    :rtype: list(bool)
    """
    multiple_modifications = False
    zapped                 = False
    filtered               = False
    interpolated           = False
    unit_converted         = False
    bounded                = False
    missing_data           = False
    masked                 = False
    masks                  = False
    ensemble_extra_info    = False
    if len(typvar) == 2:
        typvar2 = typvar[1]
        if (typvar2 == 'M'):
            # Il n'y a pas de façon de savoir quelle modif a ete faite
            multiple_modifications = True
        elif (typvar2 == '@'):
            if typvar[0] == '@':
                masks = True
            else:
                masked = True
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
        elif (typvar2 == 'H'):
            missing_data = True
        elif (typvar2 == '!'):
            ensemble_extra_info = True
    return multiple_modifications, zapped, filtered, interpolated, unit_converted, bounded, missing_data, ensemble_extra_info, masks, masked

VPARSE_TYPVAR: Final = vectorize(parse_typvar, otypes=['bool', 'bool', 'bool', 'bool', 'bool', 'bool', 'bool', 'bool', 'bool', 'bool'])  


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

VCONVERT_DATE_TO_TIMEZONE: Final = vectorize(convert_date_to_timezone)  # ,otypes=['datetime64']

class InvalidDateColumnError(Exception):
    pass

def add_timezone_column(df: pd.DataFrame, source_column: str, timezone:str) -> pd.DataFrame:
    """Adds a timezone adjusted column for provided date (date_of_validity or date_of_observation)
    :param df: input dataframe
    :type df: pd.DataFrame
    :param source_column: either date_of_validity or date_of_observation
    :type source_column: str
    :param timezone: timezone name (valid timezone can be obtained from pytz.all_timezones)
    :type timezone: str
    :raises InvalidDateColumnError: raised if source_column not in 'date_of_validity' or 'date_of_observation'
    :raises MissingColumnError: raised if source_column is not in dataframe
    :return: a new date adjusted timezone column
    :rtype: pd.DataFrame
    """
    if df.empty:
        return df
    if source_column not in ['date_of_validity', 'date_of_observation']:
        raise InvalidDateColumnError(f'"{source_column}" not in {["date_of_validity", "date_of_observation"]}!') 

    if source_column not in df.columns:
        raise MissingColumnError(f'"{source_column}" is missing from DataFrame columns, cannot add timezone column!') 

    new_column = ''.join([source_column,'_',timezone])
    new_column = new_column.replace('/','_')

    new_df = copy.deepcopy(df)
    if new_column not in new_df.columns:
        new_df[new_column] = VCONVERT_DATE_TO_TIMEZONE(new_df[source_column],timezone)
    else:
        if not new_df.loc[new_df[new_column].isna()].empty:
            new_df.loc[new_df[new_column].isna(),new_column] = VCONVERT_DATE_TO_TIMEZONE(new_df.loc[new_df[new_column].isna()][source_column],timezone)

    return new_df

def add_flag_values(df: pd.DataFrame) -> pd.DataFrame:
    """Adds the correct flag values derived from parsing the typvar.
    Replaces original column(s) if present.

    :param df: dataframe
    :type df: pd.DataFrame
    :return: flag values set according to second character of typvar if present
    :rtype: pd.DataFrame
    """
    if df.empty:
        return df
    if 'typvar' not in df.columns:
        raise MissingColumnError(f'"typvar" is missing from DataFrame columns, cannot add flags columns!') 

    if df.typvar.isna().any():
        raise MissingColumnError(f'A "typvar" value is missing from typvar DataFrame column, cannot add flags columns!') 

    new_df = copy.deepcopy(df)
    required_cols = ['masks', 'masked', 'multiple_modifications', 'zapped', 'filtered', 'interpolated', 'unit_converted', 'bounded', 'missing_data', 'ensemble_extra_info']
    if all([(col not in new_df.columns) for col in required_cols]):
        (new_df['multiple_modifications'], 
         new_df['zapped'], 
         new_df['filtered'], 
         new_df['interpolated'], 
         new_df['unit_converted'], 
         new_df['bounded'], 
         new_df['missing_data'], 
         new_df['ensemble_extra_info'], 
         new_df['masks'], 
         new_df['masked']) = VPARSE_TYPVAR(new_df.typvar)
    else: 
        # Suppression d'un future warning de pandas; dans notre cas, on veut conserver le meme comportement
        # meme avec le nouveau comportement a venir. On encapsule la suppression du warning pour ce cas seulement.
        with warnings.catch_warnings():
            warnings.filterwarnings('ignore', category=FutureWarning)

            if any([(col not in new_df.columns) for col in required_cols]):
                missing_cols = [x for x in required_cols if x not in new_df.columns]
                for col in missing_cols:
                    new_df[col] = None

            if not new_df.loc[new_df.masked.isna()].empty:
                _, _, _, _, _, _, _, _, _,  masked                        = VPARSE_TYPVAR(new_df.loc[new_df.masked.isna()].typvar)
                new_df.loc[new_df.multiple_modifications.isna(),'masked'] = masked

            if not new_df.loc[new_df.masks.isna()].empty:
                _, _, _, _, _, _, _, _, masks, _                         = VPARSE_TYPVAR(new_df.loc[new_df.masks.isna()].typvar)
                new_df.loc[new_df.multiple_modifications.isna(),'masks'] = masks    

            if not new_df.loc[new_df.multiple_modifications.isna()].empty:
                multiple_modifications, *_                                                = VPARSE_TYPVAR(new_df.loc[new_df.multiple_modifications.isna()].typvar)
                new_df.loc[new_df.multiple_modifications.isna(),'multiple_modifications'] = multiple_modifications

            if not new_df.loc[new_df.zapped.isna()].empty:
                _, zapped, *_                             = VPARSE_TYPVAR(new_df.loc[new_df.zapped.isna()].typvar)
                new_df.loc[new_df.zapped.isna(),'zapped'] = zapped

            if not new_df.loc[new_df.filtered.isna()].empty:
                _, _, filtered, *_                            = VPARSE_TYPVAR(new_df.loc[new_df.filtered.isna()].typvar)
                new_df.loc[new_df.filtered.isna(),'filtered'] = filtered

            if not new_df.loc[new_df.interpolated.isna()].empty:
                _, _, _, interpolated, *_                             = VPARSE_TYPVAR(new_df.loc[new_df.interpolated.isna()].typvar)
                new_df.loc[new_df.interpolated.isna(),'interpolated'] = interpolated

            if not new_df.loc[new_df.unit_converted.isna()].empty:
                _, _, _, _, unit_converted, *_                            = VPARSE_TYPVAR(new_df.loc[new_df.unit_converted.isna()].typvar)
                new_df.loc[new_df.unit_converted.isna(),'unit_converted'] = unit_converted

            if not new_df.loc[new_df.bounded.isna()].empty:
                _, _, _, _, _, bounded, *_                  = VPARSE_TYPVAR(new_df.loc[new_df.bounded.isna()].typvar)
                new_df.loc[new_df.bounded.isna(),'bounded'] = bounded

            if not new_df.loc[new_df.missing_data.isna()].empty:
                _, _, _, _, _, _, missing_data, *_                    = VPARSE_TYPVAR(new_df.loc[new_df.missing_data.isna()].typvar)
                new_df.loc[new_df.missing_data.isna(),'missing_data'] = missing_data

            if not new_df.loc[new_df.ensemble_extra_info.isna()].empty:
                _, _, _, _, _, _, _, ensemble_extra_info, *_                        = VPARSE_TYPVAR(new_df.loc[new_df.ensemble_extra_info.isna()].typvar)
                new_df.loc[new_df.ensemble_extra_info.isna(),'ensemble_extra_info'] = ensemble_extra_info

    return new_df

def reduce_flag_values(df: pd.DataFrame) -> pd.DataFrame:
    """Combine the flag values to the correct typvar.
    Replaces original column(s) if present.

    :param df: dataframe
    :type df: pd.DataFrame
    :return: typvar set according to the different flag values
    :rtype: pd.DataFrame
    """
    if df.empty:
        return df
    if 'typvar' not in df.columns:
        raise MissingColumnError(f'"typvar" is missing from DataFrame columns!') 

    required_cols = ['multiple_modifications', 'zapped', 'filtered', 'interpolated', 'unit_converted', 'bounded', 'missing_data', 'ensemble_extra_info', 'masked', 'masks']

    all_cols = df.columns.tolist()
    missing_elements = [x for x in required_cols if x not in all_cols]
        
    if missing_elements:
        df = add_flag_values(df)
    # S'assurer que meme si les colonnes sont presentes, elles ont bien ete initialisees(True ou False) et 
    # ne contiennent pas de NaN
    elif df[required_cols].isnull().any().any():
        df = add_flag_values(df)

    df.loc[(df[['multiple_modifications', 'zapped', 'filtered', 'interpolated', 'unit_converted', 'bounded']] == True).sum(axis=1) > 1, 'second_char'] = "M"
    
    conditions = [
        (df['masked']&df['ensemble_extra_info']),
        (df['masked']),
        (df['masks']),
        (df['missing_data']),
        (df['ensemble_extra_info']),
        (df['second_char'] == "M"),
        (df['multiple_modifications']),
        (df['zapped']),
        (df['bounded']),
        (df['filtered']),
        (df['interpolated']),
        (df['unit_converted'])
    ]
    cond_array = np.array(conditions, dtype=bool)

    values = ['*', '@', '#','H', '!', 'M', 'M', 'Z', 'B', 'F', 'I', 'U']

    df['second_char'] = np.select(cond_array, values, default='')

    df['typvar']                                 = df['typvar'].str[0] + df['second_char']
    df.loc[(df['second_char'] == '*'), 'typvar'] = "!@"
    df.loc[(df['second_char'] == '#'), 'typvar'] = "@@"

    df.drop(labels=['second_char'], axis=1, inplace=True)

    return df

def drop_duplicates(df: pd.DataFrame):
    """Removes duplicate rows from dataframe.

    :param df: original dataframe
    :type df: pd.DataFrame
    :return: dataframe without duplicate rows
    :rtype: pd.DataFrame
    """
    columns = ['nomvar', 'typvar', 'etiket', 'ni', 'nj', 'nk', 'dateo',
               'ip1', 'ip2', 'ip3', 'deet', 'npas', 'datyp', 'nbits',
               'grtyp', 'ig1', 'ig3', 'ig4', 'datev']

    clean_df = df.drop_duplicates(subset=columns, keep='first')

    if len(df.index) != len(clean_df.index):
        logging.warning('FSTPY: Found duplicate rows in dataframe!')
        # Identify and log the duplicate rows
        duplicates = df[df.duplicated(subset=columns, keep=False)]
        logging.info('Duplicate rows:\n{}'.format(duplicates.drop(columns='d', errors='ignore')))
    
    return clean_df

def add_shape_column(df: pd.DataFrame) -> pd.DataFrame:
    """Adds the shape column from the ni and nj to a dataframe.
    Replaces original column(s) if present.

    :param df: dataframe
    :type df: pd.DataFrame
    :return: dataframe with label,run,implementation and ensemble_member columns 
             added
    :rtype: pd.DataFrame
    """
    if df.empty:
        return df
    for col in ['ni', 'nj']:
        if col not in df.columns:
            raise MissingColumnError(f'"{col}" is missing from DataFrame columns, cannot add shape column!') 

    for col in ['ni', 'nj']:
        if df[col].isna().any():
            raise MissingColumnError(f'A "{col}" value is missing from {col} DataFrame column, cannot add shape column!') 

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
    if df.empty:
        return df
    if 'etiket' not in df.columns:
        raise MissingColumnError(f'"etiket" is missing from DataFrame columns, cannot add parsed etiket columns!') 

    if df.etiket.isna().any():
        raise MissingColumnError(f'A "etiket" value is missing from nomvar DataFrame column, cannot add parsed etiket columns!') 

    new_df = copy.deepcopy(df)
    required_cols = ['label', 'run', 'implementation', 'ensemble_member', 'etiket_format']
    if all([(col not in new_df.columns) for col in required_cols]):
        new_df['label'], new_df['run'], new_df['implementation'], new_df['ensemble_member'], new_df['etiket_format'] = VPARSE_ETIKET(new_df.etiket)
    else:
        if any([(col not in new_df.columns) for col in required_cols]):
            missing_cols = [x for x in required_cols if x not in new_df.columns]
            for col in missing_cols:
                new_df[col] = None

        mask = new_df.etiket_format.isna()
        if mask.any():             
            _,_,_,_, etiket_format = VPARSE_ETIKET(new_df.loc[mask, 'etiket'])
            new_df.loc[mask, 'etiket_format'] = etiket_format
        
        mask = new_df.label.isna()
        if mask.any():      
            label, *_ = VPARSE_ETIKET(new_df.loc[mask, 'etiket'])
            new_df.loc[mask,'label'] = label

        mask = new_df.run.isna()
        if mask.any(): 
            _, run, *_ = VPARSE_ETIKET(new_df.loc[mask, 'etiket'])
            new_df.loc[mask,'run'] = run

        mask = new_df.implementation.isna()
        if mask.any(): 
            _, _, implementation, *_ = VPARSE_ETIKET(new_df.loc[mask, 'etiket'])
            new_df.loc[mask,'implementation'] = implementation

        mask = new_df.ensemble_member.isna()
        if mask.any(): 
            _, _, _, ensemble_member, _ = VPARSE_ETIKET(new_df.loc[mask, 'etiket'])
            new_df.loc[mask,'ensemble_member'] = ensemble_member
            
    return new_df

def reduce_parsed_etiket_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Removes label,run,implementation and ensemble_member columns from the dataframe.
    Updates the etiket column with the latest information

    :param df: dataframe
    :type df: pd.DataFrame
    :return: dataframe without label,run,implementation and ensemble_member columns 
    :rtype: pd.DataFrame
    """
    if df.empty:
        return df

    if 'etiket' not in df.columns:
        raise MissingColumnError(f'"etiket" is missing from DataFrame columns, cannot reduce parsed etiket columns!') 

    if df.etiket.isna().any():
        raise MissingColumnError(f'A "etiket" value is missing from nomvar DataFrame column, cannot add parsed etiket columns!') 

    required_cols = ['label', 'run', 'implementation', 'ensemble_member', 'etiket_format']
    all_cols = df.columns.tolist()
    missing_elements = [x for x in required_cols if x not in all_cols]
    
    # Toutes les colonnes sont absentes, donc n'ont pas ete ajoutees (etiket non parsee)  Rien a faire!
    if len(missing_elements) == len(required_cols):
        return df
    
    # Ajoute les colonnes manquantes avec l'info de l'etiket, sans ecraser l'info des colonnes qui existent deja.
    if missing_elements:
        df = add_parsed_etiket_columns(df)

    df['etiket'] =  df.apply(lambda row: create_encoded_standard_etiket(
                                                                row['label'], 
                                                                row['run'], 
                                                                row['implementation'], 
                                                                row['ensemble_member'], 
                                                                row['etiket_format'],
                                                                ignore_extended=False,
                                                                override_pds_label=False,
                                                                ), axis=1)

    return df

def add_unit_and_description_columns(df: pd.DataFrame):
    """Adds unit and description from the nomvars to a dataframe.
    Replaces original column(s) if present.

    :param df: dataframe
    :type df: pd.DataFrame
    :return: dataframe with unit and description columns added
    :rtype: pd.DataFrame
    """
    if df.empty:
        return df
    if 'nomvar' not in df.columns:
        raise MissingColumnError(f'"nomvar" is missing from DataFrame columns, cannot add unit and description columns!') 

    if df.nomvar.isna().any():
        raise MissingColumnError(f'A "nomvar" value is missing from nomvar DataFrame column, cannot add unit and description columns!') 

    new_df = copy.deepcopy(df)

    if 'unit' not in new_df.columns and 'description' not in new_df.columns:
        new_df['unit'], new_df['description'] = VGET_UNIT_AND_DESCRIPTION(new_df.nomvar)
    
    elif 'unit' not in new_df.columns:
        new_df['unit'], _ = VGET_UNIT_AND_DESCRIPTION(new_df.nomvar)

        mask = new_df.description.isna()
        if mask.any():
            _, description = VGET_UNIT_AND_DESCRIPTION(new_df.loc[mask, 'nomvar'])
            new_df.loc[mask, 'description'] = description

    elif 'description' not in new_df.columns:
        _, new_df['description'] = VGET_UNIT_AND_DESCRIPTION(new_df.nomvar)

        mask = new_df.unit.isna()
        if mask.any():    
            unit, _ = VGET_UNIT_AND_DESCRIPTION(new_df.loc[mask, 'nomvar'])
            new_df.loc[mask,'unit'] = unit  

    else:
        mask = new_df.unit.isna()
        if mask.any():    
            unit, _ = VGET_UNIT_AND_DESCRIPTION(new_df.loc[mask, 'nomvar'])
            new_df.loc[mask,'unit'] = unit         

        mask = new_df.description.isna()
        if mask.any():
            _, description = VGET_UNIT_AND_DESCRIPTION(new_df.loc[mask, 'nomvar'])
            new_df.loc[mask, 'description'] = description
            
    return new_df

def add_unit_column(df: pd.DataFrame):
    """Adds unit from the nomvars to a dataframe.
    Replaces original column if present.

    :param df: dataframe
    :type df: pd.DataFrame
    :return: dataframe with unit columns added
    :rtype: pd.DataFrame
    """
    if df.empty:
        return df
    if 'nomvar' not in df.columns:
        raise MissingColumnError(f'"nomvar" is missing from DataFrame columns, cannot add unit columns!') 

    if df.nomvar.isna().any():
        raise MissingColumnError(f'A "nomvar" value is missing from nomvar DataFrame column, cannot add unit columns!') 

    new_df = copy.deepcopy(df)

    if 'unit' not in new_df.columns:
        new_df['unit']= VGET_UNIT(new_df.nomvar)

    else:
        mask = new_df.unit.isna()
        if mask.any():    
            unit = VGET_UNIT(new_df.loc[mask, 'nomvar'])
            new_df.loc[mask,'unit'] = unit         
            
    return new_df

def add_description_column(df: pd.DataFrame):
    """Adds description from the nomvars to a dataframe.
    Replaces original column(s) if present.

    :param df: dataframe
    :type df: pd.DataFrame
    :return: dataframe with description columns added
    :rtype: pd.DataFrame
    """
    if df.empty:
        return df
    if 'nomvar' not in df.columns:
        raise MissingColumnError(f'"nomvar" is missing from DataFrame columns, cannot add description columns!') 

    if df.nomvar.isna().any():
        raise MissingColumnError(f'A "nomvar" value is missing from nomvar DataFrame column, cannot add description columns!') 

    new_df = copy.deepcopy(df)

    if 'description' not in new_df.columns:
        new_df['description'] = VGET_DESCRIPTION(new_df.nomvar)

    else:
        mask = new_df.description.isna()
        if mask.any():
            description = VGET_DESCRIPTION(new_df.loc[mask, 'nomvar'])
            new_df.loc[mask, 'description'] = description
            
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
    if df.empty:
        return df
    if attr == 'dateo':
        if 'dateo' not in df.columns:
            raise MissingColumnError(f'"dateo" is missing from DataFrame columns, cannot add date_of_observation column!') 

        if df.dateo.isna().any():
            raise MissingColumnError(f'A "dateo" value is missing from dateo DataFrame column, cannot add date_of_observation column!') 

        new_df = copy.deepcopy(df)

        if 'date_of_observation' not in new_df.columns:
            new_df['date_of_observation'] = VCONVERT_RMNDATE_TO_DATETIME(new_df.dateo)
        else:
            filtered_df = new_df.loc[(new_df.date_of_observation.isna()) & (new_df.dateo != 0)]

            if not filtered_df.empty:
                new_df.loc[(new_df.date_of_observation.isna()) & (new_df.dateo !=  0),'date_of_observation'] = VCONVERT_RMNDATE_TO_DATETIME(filtered_df.dateo)

    else:
        if 'datev' not in df.columns:
            raise MissingColumnError(f'"datev" is missing from DataFrame columns, cannot add date_of_validity column!') 

        if df.datev.isna().any():
            raise MissingColumnError(f'A "datev" value is missing from datev DataFrame column, cannot add date_of_validity column!') 

        new_df = copy.deepcopy(df)

        if 'date_of_validity' not in new_df.columns:
            new_df['date_of_validity'] = VCONVERT_RMNDATE_TO_DATETIME(new_df.datev)
        else:
            filtered_df = new_df.loc[(new_df.date_of_validity.isna()) & (new_df.datev != 0)]

            if not filtered_df.empty:
                new_df.loc[(new_df.date_of_validity.isna()) & (new_df.datev !=  0),'date_of_validity'] = VCONVERT_RMNDATE_TO_DATETIME(filtered_df.datev)
       
    return new_df

def reduce_decoded_date_column(df: pd.DataFrame):
    """Ajustement des valeurs de dateo et de datev en fonction de la colonne 
       decodee date_of_origin. La valeur de datev est ajustee telle que
       datev = dateo + deet * npas 

    :param df: dataframe
    :type df: pd.DataFrame
    :return: dataframe auquel ont ete supprime les colonnes date_of_origin/validity
    :rtype: pd.DataFrame
    """
    if df.empty:
        return df
    
    if 'dateo' not in df.columns:
        raise MissingColumnError(f'"dateo" is missing from DataFrame columns!') 
    if 'datev' not in df.columns:
        raise MissingColumnError(f'"datev" is missing from DataFrame columns!') 
   
    if 'date_of_observation' in df.columns:
        df['dateo'] = df.apply(lambda row: RPNDate(row['date_of_observation']).dateo if pd.notnull(row['date_of_observation']) else row['dateo'], axis=1).astype(int)

    df['datev'] = VUPD_DATEV_FOR_COHERENCE(df.dateo, df.deet, df.npas)

    return df

def add_forecast_hour_column(df: pd.DataFrame):
    """Adds the forecast_hour column derived from the deet and npas columns.
    Replaces original column(s) if present.

    :param df: dataframe
    :type df: pd.DataFrame
    :return: forecast_hour column added to the dataframe
    :rtype: pd.DataFrame
    """
    if df.empty:
        return df
    for col in ['deet', 'npas']:
        if col not in df.columns:
            raise MissingColumnError(f'"{col}" is missing from DataFrame columns, cannot add forecast_hour column!') 

    for col in ['deet', 'npas']:
        if df[col].isna().any():
            raise MissingColumnError(f'A "{col}" value is missing from {col} DataFrame column, cannot add forecast_hour column!') 

    new_df = copy.deepcopy(df)

    if 'forecast_hour' not in new_df.columns:
        new_df['forecast_hour'] = VCREATE_FORECAST_HOUR(new_df.deet, new_df.npas)
    else:
        mask = new_df.forecast_hour.isna()
        if mask.any():     
            forecast_hour = VCREATE_FORECAST_HOUR(new_df.loc[mask,'deet'], new_df.loc[mask,'npas'])
            new_df.loc[mask, 'forecast_hour'] = forecast_hour

    return new_df

def reduce_forecast_hour_column(df: pd.DataFrame):
    """Update the npas column if forecast_hour is not equal to deet * npas.
       If deet = 0, we keep the original value for npas.
       Do not remove the forecast_hour column because will be used for the 
       ip_info columns reduction.

    :param df: dataframe
    :type df : pd.DataFrame
    :return  : dataframe with npas information updated
    :rtype   : pd.DataFrame
    """
    if df.empty:
        return df

    if 'deet' not in df.columns:
        raise MissingColumnError(f'"deet" is missing from DataFrame columns!') 
    
    if 'npas' not in df.columns:
        raise MissingColumnError(f'"npas" is missing from DataFrame columns!') 

    if 'forecast_hour' in df.columns:
        filtered_df = df.loc[(df.deet != 0) & (df.deet * df.npas != (df.forecast_hour/pd.Timedelta(seconds=1)).astype(int)) ]

        if not filtered_df.empty:
            # Suppression d'un future warning de pandas; dans notre cas, on veut conserver le meme comportement
            # meme avec le nouveau comportement a venir. On encapsule la suppression du warning pour ce cas seulement.
            with warnings.catch_warnings():
                warnings.filterwarnings('ignore', category=FutureWarning)
                df.loc[(df.deet != 0) & (df.deet * df.npas != (df.forecast_hour/pd.Timedelta(seconds=1)).astype(int)), 'npas'] = \
                    VUPDATE_NPAS_FROM_FORECAST_HOUR(filtered_df.deet, filtered_df.forecast_hour/pd.Timedelta(seconds=1))
    return df

def update_npas_from_forecast_hour(deet: int, forecast_hour: float) -> int:
    """ Update the npas column if forecast_hour is not equal to deet * npas.
        This function should never receive a value of deet = 0

    :param deet: valeur du deet
    :type deet : int
    :param forecast_hour: forecast_hour
    :type forecast_hour : float
    :return  : npas calcule a partir de forecast_hour or ERROR if deet = 0
    :rtype   : int
    """   
    if deet != 0:
        npas = int(forecast_hour / deet)
        return(npas)
    else:
        raise UnexpectedValueError(f'unexpected value, deet is equal to 0, should not happen!')

VUPDATE_NPAS_FROM_FORECAST_HOUR: Final = vectorize(update_npas_from_forecast_hour, otypes=['int32'])  # ,otypes=['timedelta64[ns]']

def update_ip1_from_level(df: pd.DataFrame):
    
    if df.empty:
        return df
    
    if 'level' in df.columns and 'ip1_kind' in df.columns:
        df['ip1'] = VUPDATE_IP1_WITH_IP_INFOS(df['ip1'], df['level'], df['ip1_kind'])
    else:
        raise MissingColumnError(f'level or ip1_kind columns missing from DataFrame, cannot update ip1 column!') 

    return df

def update_ip1_with_ip_infos(ip1: int, level: float, kind: int):

    # Kind 2, on encode le ip s'il etait deja encode sinon on l'encode facon OLD_STYLE
    # Autres kind:  on encode les ips
    if kind != 2 or ip1 >= 32768:
        return rmn.convertIp(rmn.CONVIP_ENCODE, level, int(kind))
    else:
        return rmn.convertIp(rmn.CONVIP_ENCODE_OLD, level, int(kind))   
         
VUPDATE_IP1_WITH_IP_INFOS = np.vectorize(update_ip1_with_ip_infos, otypes=['int'])

def update_ip2_from_ip2dec(df: pd.DataFrame):
    """ Met a jour le ip2 a partir de ip2dec.  Ne met pas a jour deet et npas. """
    if df.empty:
        return df
    
    if 'ip2_dec' in df.columns and 'ip2_kind' in df.columns:
        df['ip2'] = VUPDATE_IP2_WITH_IP_INFOS(df['ip2'], df['ip2_dec'], df['ip2_kind'])
    else:
        raise MissingColumnError(f'ip2_dec or ip2_kind columns missing from DataFrame, cannot update ip2 column!') 

    return df

def update_ip2_from_ip_infos(ip2: int, ip2_dec: int, ip2_kind: int):
    """ Met a jour le ip2 a partir du ip2 decode.  
        Si ip2 etait deja encode, on encode le ip2
    """
    if ip2 >= 32768 :
        return rmn.convertIp(rmn.CONVIP_ENCODE, ip2_dec, ip2_kind)
    else:
        return(ip2_dec)
         
VUPDATE_IP2_WITH_IP_INFOS = np.vectorize(update_ip2_from_ip_infos, otypes=['int'])

def update_ip2_from_forecast_hour(df: pd.DataFrame):
    if df.empty:
        return df
    
    if 'forecast_hour' in df.columns:
        df['ip2'] = VUPDATE_IP2_WITH_FORECAST_HOUR(df['ip2'], df['forecast_hour'])
    else:
        raise MissingColumnError(f'Forecast_hour column is missing from DataFrame, cannot update ip2 column!') 

    return df

def update_ip2_with_forecast_hour(ip2: int, forecast_hour: pd.Timedelta):
    """ Met a jour le ip2 a partir du forecast_hour.  Si ip2 etait deja encode OU
        si le forecast_hour contient des minutes/secondes, on encode le ip2
    """
    result = forecast_hour/ pd.Timedelta(hours=1)
    if ip2 >= 32768 or not float(result).is_integer():
        return rmn.convertIp(rmn.CONVIP_ENCODE, result, rmn.KIND_HOURS)
    else:
        return(result)
         
VUPDATE_IP2_WITH_FORECAST_HOUR = np.vectorize(update_ip2_with_forecast_hour, otypes=['int'])

def update_ip3_from_ip3dec(df: pd.DataFrame):
    if df.empty:
        return df
    
    if 'ip3_dec' in df.columns and 'ip3_kind' in df.columns:
        df['ip3'] = VUPDATE_IP3_WITH_IP_INFOS(df['ip3'], df['ip3_dec'], df['ip3_kind'])

    return df

def update_ip3_with_ip_infos(ip3: int, ip3_dec: int, ip3_kind: int):

    # Kind 100, n'existe pas, indique que ce n'est pas encode
    if ip3_kind != 100:
        return rmn.convertIp(rmn.CONVIP_ENCODE, ip3_dec, int(ip3_kind))
    else:
       return(ip3_dec)

VUPDATE_IP3_WITH_IP_INFOS = np.vectorize(update_ip3_with_ip_infos, otypes=['int'])

def reduce_interval_columns(df:pd.DataFrame):
    """Remove the interval column and update ip infos.
    Remove column if present.

    :param df: dataframe
    :type df: pd.DataFrame
    :return: dataframe without interval column
    :rtype: pd.DataFrame
    """
    if df.empty:
        return df
    
    if 'interval' in df.columns:
        if not df.loc[df.interval.notna()].empty:
            filtered_df = df.loc[df.interval.notna()]

            if not filtered_df.empty:
                parsed_intervals = filtered_df['interval'].apply(lambda x: pd.Series(VPARSE_INTERVAL(str(x)), index=['type_inter', 'i_low', 'i_high', 'i_kind']))

                # Copie necessaire pour eviter SettingWithCopyWarning
                filtered_df = filtered_df.copy()
                filtered_df.loc[:, 'type_inter'] = parsed_intervals['type_inter']
                filtered_df.loc[:, 'i_low']      = parsed_intervals['i_low']
                filtered_df.loc[:, 'i_high']     = parsed_intervals['i_high']
                filtered_df.loc[:, 'i_kind']     = parsed_intervals['i_kind']

                filtered_df['ip1'] = filtered_df.apply(encode_ip1_with_interval_infos, axis=1)
                filtered_df['ip2'] = filtered_df.apply(encode_ip2_with_interval_infos, axis=1)
                filtered_df['ip3'] = filtered_df.apply(encode_ip3_with_interval_infos, axis=1)

                df.loc[df['interval'].notna(), ['ip1', 'ip2', 'ip3']] = filtered_df.loc[:, ['ip1', 'ip2', 'ip3']]
    return df

def get_interval_infos(inter: str):
    """Extract interval informations from the interval object
    :param inter: an interval 
    :type inter : Interval 
    :param ip   : str
    :param low  : float
    :param high : float
    :param kind : int
    """
    import re
    from fstpy import KIND_DICT

    ip   = -1 
    low  = -1
    high = -1
    kind = -1

    infos_interval = re.split('[:@]', inter)
    pattern = r"([\d\.]+)(\D+)"
    ip = infos_interval[0]

    match = re.match(pattern, infos_interval[1])
    if match:
        low       = match.group(1)
        low_unit  = match.group(2)

    match = re.match(pattern, infos_interval[2])
    if match:
        high      = match.group(1)
        high_unit = match.group(2)

    kind = {v: k for k, v in KIND_DICT.items()}[low_unit]
    
    return ip, float(low), float(high), int(kind)

VPARSE_INTERVAL: Final = vectorize(get_interval_infos, otypes=['str', 'float', 'float', 'int'])

def encode_ip1_with_interval_infos(row):
    if row['type_inter'] == 'ip1':
        return rmn.convertIp(rmn.CONVIP_ENCODE, float(row.i_low), int(row.i_kind)) 
    else:
        # ip1 deja encode?
        if row.ip1 >= 32768 :
            return row.ip1
        else:
            return rmn.convertIp(rmn.CONVIP_ENCODE, float(row.ip1),   int(row.ip1_kind))

def encode_ip2_with_interval_infos(row):
    if row['type_inter'] == ['ip1']:
        # ip2 deja encode?
        if row.ip2 >= 32768 :
            return row.ip2
        else:
            return rmn.convertIp(rmn.CONVIP_ENCODE, int(row.ip2), int(row.ip2_kind))
    else:
        return rmn.convertIp(rmn.CONVIP_ENCODE, float(row.i_high), int(row.i_kind)) 

def encode_ip3_with_interval_infos(row):
    if row['type_inter'] == 'ip1':
        return rmn.convertIp(rmn.CONVIP_ENCODE, float(row.i_high), int(row.i_kind))
    else:
        return rmn.convertIp(rmn.CONVIP_ENCODE, float(row.i_low),  int(row.i_kind)) 
    
def add_data_type_str_column(df: pd.DataFrame) -> pd.DataFrame:
    """Adds the data type decoded to string value column to the dataframe.
    Replaces original column(s) if present.

    :param df: dataframe
    :type df: pd.DataFrame
    :return: data_type_str column added to the dataframe
    :rtype: pd.DataFrame
    """
    if df.empty:
        return df
    if 'datyp' not in df.columns:
            raise MissingColumnError(f'"datyp" is missing from DataFrame columns, cannot add data_type_str column!') 

    if df.datyp.isna().any():
            raise MissingColumnError(f'A "datyp" value is missing from datyp DataFrame column, cannot add data_type_str column!') 

    new_df = copy.deepcopy(df)

    if 'data_type_str' not in new_df.columns:
        new_df['data_type_str'] = VCREATE_DATA_TYPE_STR(new_df.datyp)
    else:
        mask = new_df.data_type_str.isna()
        if mask.any():  
            data_type_str = VCREATE_DATA_TYPE_STR(new_df.loc[mask, 'datyp'])
            new_df.loc[mask, 'data_type_str'] = data_type_str

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
    if df.empty:
        return df
    for col in ['nomvar', 'ip1', 'ip2', 'ip3']:
        if col not in df.columns:
            raise MissingColumnError(f'"{col}" is missing from DataFrame columns, cannot add ip info columns!') 

    for col in ['nomvar', 'ip1', 'ip2', 'ip3']:
        if df[col].isna().any():
            raise MissingColumnError(f'A "{col}" value is missing from {col} DataFrame column, cannot add ip info columns!') 

    new_df        = copy.deepcopy(df)
    required_cols = ['level',   'ip1_kind', 'ip1_pkind', 'ip2_dec', 'ip2_kind', 'ip2_pkind', 
                     'ip3_dec', 'ip3_kind', 'ip3_pkind', 'surface', 'follow_topography', 'ascending', 'interval']
    
    if all([(col not in new_df.columns) for col in required_cols]):
        (new_df['level'],  
         new_df['ip1_kind'], 
         new_df['ip1_pkind'], 
         new_df['ip2_dec'], 
         new_df['ip2_kind'], 
         new_df['ip2_pkind'],
         new_df['ip3_dec'], 
         new_df['ip3_kind'], 
         new_df['ip3_pkind'], 
         new_df['surface'], 
         new_df['follow_topography'], 
         new_df['ascending'], 
         new_df['interval']) = VCREATE_IP_INFO(new_df.nomvar, new_df.ip1, new_df.ip2, new_df.ip3)
    else: 
        # Suppression d'un future warning de pandas; dans notre cas, on veut conserver le meme comportement
        # meme avec le nouveau comportement a venir. On encapsule la suppression du warning pour ce cas seulement.
        with warnings.catch_warnings():
            warnings.filterwarnings('ignore', category=FutureWarning)
            if any([(col not in new_df.columns) for col in required_cols]):
                missing_cols = [x for x in required_cols if x not in new_df.columns]
                for col in missing_cols:
                    new_df[col] = None
            
            mask = new_df.level.isna()
            if mask.any():
                level, *_                                            = VCREATE_IP_INFO(new_df.loc[mask, 'nomvar'], 
                                                                                    new_df.loc[mask, 'ip1'], new_df.loc[mask, 'ip2'],
                                                                                    new_df.loc[mask, 'ip3'])
                new_df.loc[mask,'level'] = level

            mask = new_df.ip1_kind.isna()
            if mask.any():
                _, ip1_kind, *_                                      = VCREATE_IP_INFO(new_df.loc[mask, 'nomvar'], 
                                                                                    new_df.loc[mask, 'ip1'], new_df.loc[mask, 'ip2'],
                                                                                    new_df.loc[mask, 'ip3'])
                new_df.loc[mask,'ip1_kind'] = ip1_kind

            mask = new_df.ip1_pkind.isna()
            if mask.any():
                _, _, ip1_pkind, *_                                  = VCREATE_IP_INFO(new_df.loc[mask, 'nomvar'], 
                                                                                    new_df.loc[mask, 'ip1'], new_df.loc[mask, 'ip2'],
                                                                                    new_df.loc[mask, 'ip3'])
                new_df.loc[mask,'ip1_pkind'] = ip1_pkind

            mask = new_df.ip2_dec.isna()
            if mask.any():
                _, _, _, ip2_dec, *_                                 = VCREATE_IP_INFO(new_df.loc[mask, 'nomvar'], 
                                                                                    new_df.loc[mask, 'ip1'], new_df.loc[mask, 'ip2'],
                                                                                    new_df.loc[mask, 'ip3'])
                new_df.loc[mask,'ip2_dec'] = ip2_dec

            mask = new_df.ip2_kind.isna()
            if mask.any():
                _, _, _, _, ip2_kind, *_                             = VCREATE_IP_INFO(new_df.loc[mask, 'nomvar'], 
                                                                                    new_df.loc[mask, 'ip1'], new_df.loc[mask, 'ip2'],
                                                                                    new_df.loc[mask, 'ip3'])
                new_df.loc[mask,'ip2_kind'] = ip2_kind

            mask = new_df.ip2_pkind.isna()
            if mask.any():
                _, _, _, _, _, ip2_pkind, *_                         = VCREATE_IP_INFO(new_df.loc[mask, 'nomvar'], 
                                                                                    new_df.loc[mask, 'ip1'], new_df.loc[mask, 'ip2'],
                                                                                    new_df.loc[mask, 'ip3'])
                new_df.loc[mask,'ip2_pkind'] = ip2_pkind

            mask = new_df.ip3_dec.isna()
            if mask.any():
                _, _, _, _, _, _, ip3_dec, *_                        = VCREATE_IP_INFO(new_df.loc[mask, 'nomvar'], 
                                                                                    new_df.loc[mask, 'ip1'], new_df.loc[mask, 'ip2'],
                                                                                    new_df.loc[mask, 'ip3'])
                new_df.loc[mask,'ip3_dec'] = ip3_dec

            mask = new_df.ip3_kind.isna()
            if mask.any():
                _, _, _, _, _, _, _, ip3_kind, *_                    = VCREATE_IP_INFO(new_df.loc[mask, 'nomvar'], 
                                                                                    new_df.loc[mask, 'ip1'], new_df.loc[mask, 'ip2'],
                                                                                    new_df.loc[mask, 'ip3'])
                new_df.loc[mask,'ip3_kind'] = ip3_kind
                
            mask = new_df.ip3_pkind.isna()
            if mask.any():
                _, _, _, _, _, _, _, _, ip3_pkind, *_                = VCREATE_IP_INFO(new_df.loc[mask, 'nomvar'], 
                                                                                    new_df.loc[mask, 'ip1'], new_df.loc[mask, 'ip2'],
                                                                                    new_df.loc[mask, 'ip3'])
                new_df.loc[mask,'ip3_pkind'] = ip3_pkind
            
            mask = new_df.surface.isna()
            if mask.any():
                _, _, _, _, _, _, _, _, _, surface, *_               = VCREATE_IP_INFO(new_df.loc[mask, 'nomvar'], 
                                                                                    new_df.loc[mask, 'ip1'], new_df.loc[mask, 'ip2'],
                                                                                    new_df.loc[mask, 'ip3'])
                new_df.loc[mask,'surface'] = surface    

            mask = new_df.follow_topography.isna()
            if mask.any():
                _, _, _, _, _, _, _, _, _, _, follow_topography, *_  = VCREATE_IP_INFO(new_df.loc[mask, 'nomvar'], 
                                                                                    new_df.loc[mask, 'ip1'], new_df.loc[mask, 'ip2'],
                                                                                    new_df.loc[mask, 'ip3'])
                new_df.loc[mask,'follow_topography'] = follow_topography    

            mask = new_df.ascending.isna()
            if mask.any():
                _, _, _, _, _, _, _, _, _, _, _, ascending, _        = VCREATE_IP_INFO(new_df.loc[mask, 'nomvar'], 
                                                                                    new_df.loc[mask, 'ip1'], new_df.loc[mask, 'ip2'],
                                                                                    new_df.loc[mask, 'ip3'])
                new_df.loc[mask,'ascending'] = ascending    

            mask = new_df.interval.isna()
            if mask.any():
                _, _, _, _, _, _, _, _, _, _, _, _, interval         = VCREATE_IP_INFO(new_df.loc[mask, 'nomvar'], 
                                                                                    new_df.loc[mask, 'ip1'], new_df.loc[mask, 'ip2'],
                                                                                    new_df.loc[mask, 'ip3'])
                new_df.loc[mask,'interval'] = interval  

    return new_df

def reduce_ip_info_columns(df: pd.DataFrame):
    """Updates the ip1, ip2, ip3 column with the information from the ip decoded columns when necessary.
    Replaces original column(s) if present.

    :param df: dataframe
    :type df: pd.DataFrame
    :return: Ip's informative columns removed from the updated dataframe.
    :rtype: pd.DataFrame
    """
    if df.empty:
        return df
    
    required_cols = ['level', 'ip1_kind', 'ip1_pkind', 'ip2_dec', 'ip2_kind', 'ip2_pkind', 
                     'ip3_dec','ip3_kind', 'ip3_pkind', 'interval', 'surface', 'follow_topography','ascending']
    all_cols = df.columns.tolist()
    missing_elements = [x for x in required_cols if x not in all_cols]
    
    # Toutes les colonnes sont absentes, donc n'ont pas ete ajoutees  Rien a faire!
    if len(missing_elements) == len(required_cols):
        return df
    
    # Ajoute les colonnes manquantes, sans ecraser l'info des colonnes qui existent deja.
    if missing_elements:
        df = add_ip_info_columns(df)

    # Recuperer l'info de level pour maj ip1
    update_ip1_from_level(df)  
    update_ip2_from_ip2dec(df) 
    if 'forecast_hour' in df.columns: 
        update_ip2_from_forecast_hour(df)
    update_ip3_from_ip3dec(df)

    #  Derniere etape a faire 
    if 'interval' in df.columns:
        reduce_interval_columns(df)
        
    return(df)

def add_columns(df: pd.DataFrame, columns: 'str|list[str]' = ['flags', 'etiket', 'unit', 'dateo', 'datev', 'forecast_hour', 'datyp', 'ip_info', 'description']):
    """If valid columns are provided, they will be added. 
       These include ['flags','etiket','unit','dateo','datev','forecast_hour', 'datyp','ip_info', 'description']
       Replaces original column(s) if present.   

    :param df: dataframe to modify (meta data needs to be present in dataframe)
    :type df: pd.DataFrame
    :param decode: if decode is True, add the specified columns
    :type decode: bool
    :param columns: [description], defaults to  ['flags','etiket','unit','dateo','datev','forecast_hour', 'datyp','ip_info', 'description']
    :type columns: list[str], optional
    """
    if df.empty:
        return df
    cols = ['flags', 'etiket', 'unit', 'dateo', 'datev', 'forecast_hour', 'datyp', 'ip_info', 'description']
    if isinstance(columns,str):
        columns = [columns]
    
    for col in columns:
        if col not in cols:
            logging.warning(f'{col} not found in {cols}')

    if 'etiket' in columns:
        df = add_parsed_etiket_columns(df)

    if 'unit' in columns:
        df = add_unit_column(df)
    
    if 'description' in columns:
        df = add_description_column(df)

    if 'dateo' in columns:
        df = add_decoded_date_column(df, 'dateo')

    if 'datev' in columns:
        df = add_decoded_date_column(df, 'datev')

    if 'forecast_hour' in columns:
        df = add_forecast_hour_column(df)

    if 'datyp' in columns:
        df = add_data_type_str_column(df)

    if ('ip_info' in columns):
        # df = add_ip_info_columns(df)  (Appele dans set_vertical_coordinate_type)
        df = set_vertical_coordinate_type(df)

    if 'flags' in columns:
        df = add_flag_values(df)

    return df    
    
def reduce_columns(df: pd.DataFrame)-> pd.DataFrame:
    """Enlever les colonnes qui ont ete ajoutees et ramener le dataframe a sa plus simple expression
       en reprenant l'info necessaire pour rebatir les colonnes de base.  
       L'ordre d'appel des fonctions pour reduire (reduce_ ...) est important.

    :param df: dataframe to modify 
    :type df : pd.DataFrame
    """
    if df.empty:
        return df
    
    simple_df = df.loc[~df.nomvar.isin(["^^", ">>", "^>", "!!", "!!SF", "HY"])].copy()
    meta_df   = df.loc[ df.nomvar.isin(["^^", ">>", "^>", "!!", "!!SF", "HY"])].copy()

    if simple_df.empty:
        return df

    # Attention, ordre d'appel des fonctions doit etre respecte 
    simple_df = reduce_parsed_etiket_columns(simple_df)
    simple_df = reduce_flag_values(simple_df)
    simple_df = reduce_forecast_hour_column(simple_df)
    simple_df = reduce_decoded_date_column(simple_df)
    simple_df = reduce_ip_info_columns(simple_df)

    new_df = pd.concat([meta_df, simple_df], ignore_index=True)

    # Suppression des colonnes qui ont ete ajoutees autres que unit et etiket_format
    new_df = remove_all_expanded_columns(new_df)
    
    return new_df    

def remove_all_expanded_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Remove all expanded columns from the dataframe except unit and etiket_format

    :param df          : the dataFrame from which to remove columns
    :type df           : pd.DataFrame
    """   
    
    cols_to_remove =  ['label', 'run', 'implementation', 'date_of_observation', 'ensemble_member', 'date_of_validity', 
                       'level', 'ip1_kind', 'ip1_pkind', 'forecast_hour','ip2_dec', 'ip2_kind', 'ip2_pkind', 'ip3_dec',
                       'ip3_kind', 'ip3_pkind', 'interval', 'surface', 'follow_topography', 'ascending', 'description',
                       'data_type_str', 'vctype', 'masks', 'masked', 'multiple_modifications','zapped', 'filtered', 
                       'interpolated', 'unit_converted', 'bounded', 'missing_data', 'ensemble_extra_info']

    df = remove_list_of_columns(df, cols_to_remove)

    return df

def remove_list_of_columns(df: pd.DataFrame, list_of_cols: list) -> pd.DataFrame:
    """Remove columns from the dataframe

    :param df          : the dataFrame from which to remove columns
    :type df           : pd.DataFrame
    :param list_of_cols: a list of columns to remove
    :type list_of_cols : list
    """   
    cols_to_remove = [x for x in list_of_cols if x in df.columns]
    df.drop(labels=cols_to_remove, axis=1, inplace=True)

    return df

def reorder_columns(df):
    """Reorders columns for voir like output

    :param df: input dataFrame
    :type df: pd.DataFrame
    """
    ordered = ['nomvar', 'typvar', 'etiket', 'ni', 'nj', 'nk', 'dateo', 'ip1', 'ip2',
               'ip3', 'deet', 'npas', 'datyp', 'nbits', 'grtyp', 'ig1', 'ig2', 'ig3', 'ig4']
    if df.empty:
        return 
    all_columns = set(df.columns)

    extra_columns = all_columns.difference(set(ordered))
    if len(extra_columns) > 0:
        ordered.extend(list(extra_columns))

    df = df[ordered]

def get_meta_fields_exists(grid_df):
    toctoc = grid_df.loc[grid_df.nomvar == "!!"]
    vcode = []

    if not toctoc.empty:
        for row in toctoc.itertuples():
            vcode.append(row.ig1)
        toctoc = True
    else:
        vcode.append(-1)
        toctoc = False
    p0 = meta_exists(grid_df, "P0")
    e1 = meta_exists(grid_df, "E1")
    pt = meta_exists(grid_df, "PT")
    hy = meta_exists(grid_df, "HY")
    sf = meta_exists(grid_df, "!!SF")
    return toctoc, p0, e1, pt, hy, sf, vcode


def meta_exists(grid_df, nomvar) -> bool:
    df = grid_df.loc[grid_df.nomvar == nomvar]
    return not df.empty

def create_empty_dataframe(num_rows: int) -> pd.DataFrame:
    """Creates an empty dataframe of the given number of rows

    :param num_rows: number of rows to create
    :type num_rows: int
    :return: a dataframe of the given number of rows without data column
    :rtype: pandas Dataframe
    """
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

def update_datev_for_coherence(dateo: int, deet: int, npas: int)-> int:
    """Mise-a-jour de datev a partir de dateo, deet et npas

    :param dateo
    :type dateo: int
    :param deet
    :type deet: int
    :param npas
    :type deet: int
    """
    from rpnpy.rpndate import RPNDate

    newdateofvalidity = RPNDate(int(dateo), dt=deet, nstep=npas)

    return newdateofvalidity.datev

VUPD_DATEV_FOR_COHERENCE: Final = vectorize(update_datev_for_coherence)
