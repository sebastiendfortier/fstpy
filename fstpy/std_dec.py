# -*- coding: utf-8 -*-
import datetime

import numpy as np
import rpnpy.librmn.all as rmn
from rpnpy.rpndate import RPNDate

from fstpy import DATYP_DICT, STDVAR

def get_forecast_hour(deet:int,npas:int) -> datetime.timedelta:
    """creates a timedelta object in seconds from deet * npas

    :param deet: This is the length of a time step used during a model integration, in seconds.
    :type deet: int
    :param npas:This is the time step number at which the field was written during an integration. The number of the initial time step is 0. 
    :type npas: int
    :return: time delta in seconds
    :rtype: datetime.timedelta
    """
    if (deet != 0) and (npas != 0):
        return datetime.timedelta(seconds=int(npas * deet))
    return None    

def decode_ip2(ip2:int):
    """decodes the ip2 int value to its float value, kind and kind string

    :param ip2: encoded value stored in ip2
    :type ip2: int
    :return: decoded ip2 value, kind and printable kind string
    :rtype: float,int,str
    """
    _,i2,_ = rmn.DecodeIp(0,ip2,0) 
    pkind = '' if i2.kind in [-1,3,15,17] else rmn.kindToString(i2.kind).strip()
    return i2.v1,i2.kind,pkind

def decode_ip3(ip3:int):
    """decodes the ip3 int value to its float value, kind and kind string

    :param ip3: encoded value stored in ip3
    :type ip3: int
    :return: decoded ip3 value, kind and printable kind string
    :rtype: float,int,str
    """
    _,_,i3 = rmn.DecodeIp(0,0,ip3) 
    pkind = '' if i3.kind in [-1,3,15,17] else rmn.kindToString(i3.kind).strip()
    return i3.v1,i3.kind,pkind

def get_data_type_str(datyp:int):
    """gets the data type string from the datyp int

    :param datyp: data type int value
    :type datyp: int
    :return: string eqivalent of the datyp int value
    :rtype: str
    """
    return DATYP_DICT[datyp]

def get_level_info(ip1:int):
    """gets all relevant level info from the ip1 int value

    :param ip1: encoded value stored in ip1
    :type ip1: int
    :return: level value, kind and kind str obtained from decoding ip1 and bools representing if the level is a surface level and if it follows topography.
    :rtype: float,int,str,bool,bool
    """
    i1,_,_ = rmn.DecodeIp(ip1,0,0) 
    ip1_pkind = '' if i1.kind in [-1,3,15,17] else rmn.kindToString(i1.kind).strip()
    level=i1.v1
    ip1_kind = i1.kind
    surface = is_surface(ip1_kind,level)
    follow_topography = level_type_follows_topography(ip1_kind)
    return level,ip1_kind,ip1_pkind,surface,follow_topography

def get_unit_and_description(nomvar):
    """Reads the Standard file dictionnary and gets the unit and description associated with the variable name

    :param nomvar: name of the variable
    :type nomvar: str
    :return: unit name and description
    :rtype: str,str

    >>> unit,description = get_unit_and_description('TT')
    'Air Temperature,celsius'
    """
    unit = STDVAR.loc[STDVAR['nomvar'] == f'{nomvar}']['unit'].values
    description = STDVAR.loc[STDVAR['nomvar'] == f'{nomvar}']['description_en'].values
    if len(description):
        description = description[0]
    else:    
        description = ''
    if len(unit):
        unit = unit[0]
    else:
        unit = 'scalar'
    return unit,description    

# written by Micheal Neish creator of fstd2nc
def convert_rmndate_to_datetime(date:int) -> datetime.datetime:
    """returns a datetime object of the decoded RMNDate int

    :param date: RMNDate int value
    :type date: int
    :return: datetime object of the decoded date
    :rtype: datetime.datetime

    >>> convert_rmndate_to_datetime(442998800)
    datetime.datetime(2020, 7, 14, 12, 0)
    """
    dummy_stamps = (0, 10101011)
    if date not in dummy_stamps:
        return RPNDate(int(date)).toDateTime().replace(tzinfo=None)
    else:
        return None

def is_surface(ip1_kind:int,level:float) -> bool:
    """Return a bool that tell us if the level is a surface level

    :param ip1_kind: kind of level
    :type ip1_kind: int
    :param level: value of the level
    :type level: float
    :return: True if the level is a surface level else False
    :rtype: bool

    >>> is_surface(5,0.36116)
    False
    """
    meter_levels = np.arange(0.,10.5,.5).tolist()
    if (ip1_kind == 5) and (level == 1):
        return True
    elif (ip1_kind == 4) and (level in meter_levels):
        return True
    elif (ip1_kind == 1) and (level == 1):
        return True
    else:
        return False

def level_type_follows_topography(ip1_kind:int) -> bool:
    """Returns True if the kind of level is a kind that follows topography

    :param ip1_kind: level type
    :type ip1_kind: int
    :return: True if the kind of level is a kind that follows topography else False
    :rtype: bool

    >>> level_type_follows_topography(5)
    True
    """
    if ip1_kind == 1:
        return True
    elif ip1_kind == 4:
        return True
    elif ip1_kind == 5:
        return True
    else:
        return False  

def get_grid_identifier(nomvar:str,ip1:int,ip2:int,ig1:int,ig2:int) -> str:
    """Create a grid identifer from ip2,ip2 or ig1,ig2 depending of the varibale. 
    Meta information like >> have their grid identifiers strored in ip1,and ip2, 
    while regular viables have them strored in ig1 and ig2

    :param nomvar: name of the variable
    :type nomvar: str
    :param ip1: ip1 value
    :type ip1: int
    :param ip2: ip2 value
    :type ip2: int
    :param ig1: ig1 value
    :type ig1: int
    :param ig2: ig2 value
    :type ig2: int
    :return: concatenation of ig1,ig2 or ip1,ip2 depending on variable name
    :rtype: str

    >>> get_grid_identifier('TT',94733000,6,33792,77761)
    '3379277761'
    """
    nomvar = nomvar.strip()
    if nomvar in ["^>",">>", "^^", "!!", "!!SF"]:
        grid = "".join([str(ip1),str(ip2)])
    elif nomvar == "HY":    
        grid = None
    else:
        grid = "".join([str(ig1),str(ig2)])
    return grid


def get_parsed_etiket(raw_etiket:str):
    """parses the etiket of a standard file to get label, run, implementation and ensemble member if available

    :param raw_etiket: raw etiket before parsing
    :type raw_etiket: str
    :return: the parsed etiket, run, implementation and ensemble member
    :rtype: str  

    >>> get_parsed_etiket('')
    ('', '', '', '')
    >>> get_parsed_etiket('R1_V710_N')
    ('_V710_', 'R1', 'N', '')
    """
    import re
    label = raw_etiket
    run = None
    implementation = None
    ensemble_member = None
    
    match_run = "[RGPEAIMWNC_][\\dRLHMEA_]"
    match_main_cmc = "\\w{5}"
    match_main_spooki = "\\w{6}"
    match_implementation = "[NPX]"
    match_ensemble_member = "\\w{3}"
    match_end = "$"
    
    re_match_cmc_no_ensemble = match_run + match_main_cmc + match_implementation + match_end
    re_match_cmc_ensemble = match_run + match_main_cmc + match_implementation + match_ensemble_member + match_end
    re_match_spooki_no_ensemble = match_run + match_main_spooki + match_implementation + match_end
    re_match_spooki_ensemble = match_run + match_main_spooki + match_implementation + match_ensemble_member + match_end

    if re.match(re_match_cmc_no_ensemble,raw_etiket):
        run = raw_etiket[:2]
        label = raw_etiket[2:7]
        implementation = raw_etiket[7]
    elif re.match(re_match_cmc_ensemble,raw_etiket):
        run = raw_etiket[:2]
        label = raw_etiket[2:7]
        implementation = raw_etiket[7]
        ensemble_member = raw_etiket[8:11]
    elif re.match(re_match_spooki_no_ensemble,raw_etiket):
        run = raw_etiket[:2]
        label = raw_etiket[2:8]
        implementation = raw_etiket[8]
    elif re.match(re_match_spooki_ensemble,raw_etiket):
        run = raw_etiket[:2]
        label = raw_etiket[2:8]
        implementation = raw_etiket[8]
        ensemble_member = raw_etiket[9:12]
    else:
        label = raw_etiket
    return label,run,implementation,ensemble_member    







# def decode_ip(ip:int):
#     """Decode the int ip value into float value, kind int, kind str

#     :param ip: int value for ip
#     :type ip: int
#     :return: decoded ip in the form of value, kind int and kind str
#     :rtype: float,int,str

#     >>> decode_ip(94733000)
#     (0.36116, 5, 'hy')
#     """
#     v_dec_kind = rmn.convertIp(rmn.CONVIP_DECODE,int(ip))
#     value = float("%.6f"%-1) if v_dec_kind[1] == -1 else float("%.6f"%v_dec_kind[0])
#     kind = int(v_dec_kind[1])
#     pkind = get_pkind(kind)
#     return value, kind, pkind


# def get_pkind(ip1_kind:int) -> str:
#     """Gets the string representation of a kind int, if it has no representation, returns empty string

#     :param ip1_kind: kind int
#     :type ip1_kind: int
#     :return: string representation of a kind int
#     :rtype: str

#     >>> get_pkind(5)
#     'hy' 
#     """
#     return '' if ip1_kind in [-1,3,15,17] else rmn.kindToString(ip1_kind).strip()


# def get_level_and_kind(ip1:int):
#     #logger.debug('ip1',ip1)
#     level_kind = rmn.convertIp(rmn.CONVIP_DECODE,int(ip1))
#     #logger.debug('level_kind',level_kind)
#     ip1_kind = int(level_kind[1])
#     level = level_kind[0]
#     level = float("%.6f"%-1) if ip1_kind == -1 else float("%.6f"%level)
#     return level, ip1_kind
#     #df.at[i,'ip1_kind'] = ip1_kind
#     #df.at[i,'level'] = float("%.6f"%-1) if df.at[i,'ip1_kind'] == -1 else float("%.6f"%level)

# def create_decoded_value(v1,v2):
#     if v1 == v2:
#         return v1
#     else:
#         return (v1,v2)

# def decode_ips(nomvar:str,ip1:int,ip2:int,ip3:int):
#     if not (nomvar in [">>","^^","^>","!!"]):
#         pk1, pk2, pk3 = rmn.convertIPtoPK(ip1, ip2, ip3)
#         #print(pk1)
#         level = pk1.v1
#         ip1_kind = pk1.kind
#         pkind = get_pkind(ip1_kind)
#         ip2_dec = create_decoded_value(pk2.v1, pk2.v2)
#         ip2_kind = pk2.kind
#         ip2_pkind = get_pkind(ip2_kind)
#         ip3_dec = create_decoded_value(pk3.v1, pk3.v2)
#         ip3_kind = pk3.kind
#         ip3_pkind = get_pkind(ip3_kind)
#     else:
#         (level,ip1_kind) = rmn.convertIp(rmn.CONVIP_DECODE,int(ip1))
#         (ip2_dec,ip2_kind) = rmn.convertIp(rmn.CONVIP_DECODE,int(ip2))
#         (ip3_dec,ip3_kind) = rmn.convertIp(rmn.CONVIP_DECODE,int(ip3))
#         pkind = get_pkind(ip1_kind)
#         ip2_pkind = get_pkind(ip2_kind)
#         ip3_pkind = get_pkind(ip3_kind)
#     return level,ip1_kind,pkind,ip2_dec,ip2_kind,ip2_pkind,ip3_dec,ip3_kind,ip3_pkind    

# def decode_metadata(nomvar:str,etiket:str,dateo:int,datev:int,deet:int,npas:int,datyp:int,ip1:int,ip2:int,ip3:int):
#     """decodes the values of etiket,dateo,datev,datyp,ip1,ip2,ip3

#     :param nomvar: [description]
#     :type nomvar: str
#     :param etiket: [description]
#     :type etiket: str
#     :param dateo: [description]
#     :type dateo: int
#     :param datev: [description]
#     :type datev: int
#     :param deet: [description]
#     :type deet: int
#     :param npas: [description]
#     :type npas: int
#     :param datyp: [description]
#     :type datyp: int
#     :param ip1: [description]
#     :type ip1: int
#     :param ip2: [description]
#     :type ip2: int
#     :param ip3: [description]
#     :type ip3: int
#     :param level: [description]
#     :type level: int
#     :param ip1_kind: [description]
#     :type ip1_kind: int
#     :return: [description]
#     :rtype: [type]
#     """
#     dec_record = {}
#     dec_record['label'],dec_record['run'],dec_record['implementation'],dec_record['ensemble_member'] = get_parsed_etiket(etiket)
#     dec_record['unit'],dec_record['description']=get_unit_and_description(nomvar)
#     #create a real date of observation
#     dec_record['date_of_observation'] = convert_rmndate_to_datetime(int(dateo))
#     #create a printable date of validity
#     dec_record['date_of_validity'] = convert_rmndate_to_datetime(int(datev))    
#     dec_record['forecast_hour'] = datetime.timedelta(seconds=(npas * deet))         
#     dec_record['level'],dec_record['ip1_kind'],dec_record['pkind'],dec_record['ip2_dec'],dec_record['ip2_kind'],dec_record['ip2_pkind'],dec_record['ip3_dec'],dec_record['ip3_kind'],dec_record['ip3_pkind'] = decode_ips(nomvar,ip1,ip2,ip3)
#     dec_record['data_type_str'] = DATYP_DICT[datyp]
    
#     #set surface flag for surface levels
#     dec_record['surface'] = is_surface(dec_record['ip1_kind'],dec_record['level'])
#     dec_record['follow_topography'] = level_type_follows_topography(dec_record['ip1_kind'])
#     dec_record['unit_converted'] = False
#     dec_record['zapped'] = False
#     dec_record['vctype'] = ''
#     return dec_record
