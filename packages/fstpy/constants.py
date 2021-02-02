import pandas as pd
import os
from .context import logger

prefix='/'.join(__file__.split('/')[0:-1])
csv_path = prefix + '/csv/'

#kind,toctoc,P0,E1,PT,HY,SF,vcode,vctype
VCTYPES = pd.read_csv(csv_path + 'verticalcoordinatetypes.csv')
#nomvar,description_fr,description_en,unit
STDVAR = pd.read_csv(csv_path + 'stdvar.csv')
#name,symbol,expression,bias,factor,mass,length,time,electricCurrent,temperature,amountOfSubstance,luminousIntensity
UNITS = pd.read_csv(csv_path + 'units.csv')
#plugin_name,etiket
ETIKET = pd.read_csv(csv_path + 'etiket.csv')
#label,kind,follow_topography,surface_level
LEVELTYPE = pd.read_csv(csv_path + 'leveltype.csv')
#name,value
CONSTANTS = pd.read_csv(csv_path + 'constants.csv')

DATYP_DICT = {
                    0:'X',
                    1:'R',
                    2:'I',
                    4:'S',
                    5:'E',
                    6:'F',
                    7:'A',
                    8:'Z',
                    130:'i',
                    133:'e',
                    134:'f'
                }

KIND_DICT = {
                    -1:'_',
                    0: 'm',   #[metres] (height with respect to sea level)
                    1: 'sg',  #[sigma] (0.0->1.0)
                    2: 'mb',  #[mbars] (pressure in millibars)
                    3: '   ', #[others] (arbitrary code)
                    4: 'M',   #[metres] (height with respect to ground level)
                    5: 'hy',  #[hybrid] (0.0->1.0)
                    6: 'th',  #[theta]
                    10: 'H',  #[hours]
                    15: '  ', #[reserved, integer]
                    17: ' ',  #[index X of conversion matrix]
                    21: 'mp'  #[pressure in metres]
                }

# TYPVAR_DICT = {
#             ANALYSIS:"A",
#             CLIMATOLOGY:"C",
#             RAW_STATION_DATA:"D",
#             MONTHLY_ERROR:"E",
#             VARIOUS_CONSTANTS:"K",
#             VERIFICATION_MATRIX_CONTINGENCY_TABLE:"M",
#             OBSERVATION:"O",
#             FORECAST:"P",
#             DIAGNOSTIC_QPF:"Q",
#             VARIOUS_SCORES:"S",
#             TIME_SERIE:"T",
#             VARIOUS:"X",
#}
def get_constant_row_by_name(df:pd.DataFrame, df_name:str, index:str, name:str) -> pd.Series:
    row = df.loc[df[index] == name]
    if len(row.index):
        return row
    #else:
    #    logger.warning('get_constant_row_by_name - %s - no %s found by that name'%(name,df_name))
        #logger.error('get_constant_row_by_name - available %s are:'%df_name)
        #logger.error(pprint.pformat(sorted(df[index].to_list())))
    return pd.Series(dtype=object)

def get_unit_by_name(name:str) -> pd.Series:
    unit = get_constant_row_by_name(UNITS, 'unit', 'name', name)
    if len(unit.index):
        return unit
    else:
        return get_constant_row_by_name(UNITS, 'unit', 'name', 'scalar')

def get_etikey_by_name(name:str) -> pd.Series:
    return get_constant_row_by_name(ETIKET, 'etiket', 'plugin_name', name)

def get_constant_by_name(name:str) -> pd.Series:
    return get_constant_row_by_name(CONSTANTS, 'value', 'name', name)

def get_column_value_from_row(row, column):
    return row[column].values[0]
