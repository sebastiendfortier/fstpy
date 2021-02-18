# -*- coding: utf-8 -*-
import rpnpy.librmn.all as rmn
from .constants import STDVAR,DATYP_DICT
import datetime
import numpy as np

def decode_meta_data(nomvar:str,etiket:str,dateo:int,datev:int,deet:int,npas:int,datyp:int,ip1:int,ip2:int,ip3:int):
    """decodes the values of etiket,dateo,datev,datyp,ip1,ip2,ip3

    :param nomvar: [description]
    :type nomvar: str
    :param etiket: [description]
    :type etiket: str
    :param dateo: [description]
    :type dateo: int
    :param datev: [description]
    :type datev: int
    :param deet: [description]
    :type deet: int
    :param npas: [description]
    :type npas: int
    :param datyp: [description]
    :type datyp: int
    :param ip1: [description]
    :type ip1: int
    :param ip2: [description]
    :type ip2: int
    :param ip3: [description]
    :type ip3: int
    :param level: [description]
    :type level: int
    :param ip1_kind: [description]
    :type ip1_kind: int
    :return: [description]
    :rtype: [type]
    """
    dec_record = {}
    dec_record['label'],dec_record['run'],dec_record['implementation'],dec_record['ensemble_member'] = parse_etiket(etiket)
    dec_record['unit'],dec_record['description']=get_unit_and_description(nomvar)
    #create a real date of observation
    dec_record['date_of_observation'] = convert_rmndate_to_datetime(int(dateo))
    #create a printable date of validity
    dec_record['date_of_validity'] = convert_rmndate_to_datetime(int(datev))    
    dec_record['forecast_hour'] = datetime.timedelta(seconds=(npas * deet))         
    dec_record['level'],dec_record['ip1_kind'],dec_record['pkind'],dec_record['ip2_dec'],dec_record['ip2_kind'],dec_record['ip2_pkind'],dec_record['ip3_dec'],dec_record['ip3_kind'],dec_record['ip3_pkind'] = decode_ips(nomvar,ip1,ip2,ip3)
    dec_record['data_type_str'] = DATYP_DICT[datyp]
    
    #set surface flag for surface levels
    dec_record['surface'] = is_surface(dec_record['ip1_kind'],dec_record['level'])
    dec_record['follow_topography'] = level_type_follows_topography(dec_record['ip1_kind'])
    dec_record['unit_converted'] = False
    dec_record['zapped'] = False
    dec_record['vctype'] = ''
    return dec_record

def get_unit_and_description(nomvar):
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
def convert_rmndate_to_datetime(date:int):
    #def stamp2datetime (date):
    from rpnpy.rpndate import RPNDate
    dummy_stamps = (0, 10101011)
    if date not in dummy_stamps:
        return RPNDate(int(date)).toDateTime().replace(tzinfo=None)
    else:
        return str(date)

def is_surface(ip1_kind:int,level:float):
    meter_levels = np.arange(0.,10.5,.5).tolist()
    if (ip1_kind == 5) and (level == 1):
        return True
    elif (ip1_kind == 4) and (level in meter_levels):
        return True
    elif (ip1_kind == 1) and (level == 1):
        return True
    else:
        return False

def level_type_follows_topography(ip1_kind:int):
    if ip1_kind == 1:
        return True
    elif ip1_kind == 4:
        return True
    elif ip1_kind == 5:
        return True
    else:
        return False  

def create_grid_identifier(nomvar:str,ip1:int,ip2:int,ig1:int,ig2:int) -> str:
    if nomvar.strip() in [">>", "^^", "!!", "!!SF", "HY"]:
        grid = "".join([str(ip1),str(ip2)])
    else:
        grid = "".join([str(ig1),str(ig2)])
    return grid

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

def decode_ip1(ip:int):
    v_dec_kind = rmn.convertIp(rmn.CONVIP_DECODE,int(ip))
    level = float("%.6f"%-1) if v_dec_kind[1] == -1 else float("%.6f"%v_dec_kind[0])
    ip1_kind = int(v_dec_kind[1])
    pkind = get_pkind(ip1_kind)
    return level, ip1_kind, pkind

def create_decoded_value(v1,v2):
    if v1 == v2:
        return v1
    else:
        return (v1,v2)

def get_pkind(ip1_kind):
    return '' if ip1_kind in [-1,3,15,17] else rmn.kindToString(ip1_kind).strip()

def decode_ips(nomvar:str,ip1:int,ip2:int,ip3:int):
    if not (nomvar in [">>","^^","^>","!!"]):
        pk1, pk2, pk3 = rmn.convertIPtoPK(ip1, ip2, ip3)
        #print(pk1)
        level = pk1.v1
        ip1_kind = pk1.kind
        pkind = get_pkind(ip1_kind)
        ip2_dec = create_decoded_value(pk2.v1, pk2.v2)
        ip2_kind = pk2.kind
        ip2_pkind = get_pkind(ip2_kind)
        ip3_dec = create_decoded_value(pk3.v1, pk3.v2)
        ip3_kind = pk3.kind
        ip3_pkind = get_pkind(ip3_kind)
    else:
        (level,ip1_kind) = rmn.convertIp(rmn.CONVIP_DECODE,int(ip1))
        (ip2_dec,ip2_kind) = rmn.convertIp(rmn.CONVIP_DECODE,int(ip2))
        (ip3_dec,ip3_kind) = rmn.convertIp(rmn.CONVIP_DECODE,int(ip3))
        pkind = get_pkind(ip1_kind)
        ip2_pkind = get_pkind(ip2_kind)
        ip3_pkind = get_pkind(ip3_kind)
    return level,ip1_kind,pkind,ip2_dec,ip2_kind,ip2_pkind,ip3_dec,ip3_kind,ip3_pkind    

def parse_etiket(raw_etiket:str):
    """parses the etiket of a standard file to get etiket, run, implementation and ensemble member if available

    :param raw_etiket: raw etiket before parsing
    :type raw_etiket: str
    :return: the parsed etiket, run, implementation and ensemble member
    :rtype: str  

    >>> parse_etiket('')
    ('', '', '', '')
    >>> parse_etiket('R1_V710_N')
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