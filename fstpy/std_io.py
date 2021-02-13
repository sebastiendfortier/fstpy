# -*- coding: utf-8 -*-
import rpnpy.librmn.all as rmn
from .config import logger

def open_fst(path:str, mode:str, caller_class:str, error_class:Exception):
    file_modification_time = get_file_modification_time(path,mode,caller_class,error_class)
    file_id = rmn.fstopenall(path, mode)
    logger.info(caller_class + ' - opening file %s', path)
    return file_id, file_modification_time

def close_fst(file_id:int, file:str,caller_class:str):
    logger.info(caller_class + ' - closing file %s', file)
    rmn.fstcloseall(file_id)


# Lightweight test for FST files.
# Uses the same test for fstd98 random files from wkoffit.c (librmn 16.2).
#
# The 'isFST' test from rpnpy calls c_wkoffit, which has a bug when testing
# many small (non-FST) files.  Under certain conditions the file handles are
# not closed properly, which causes the application to run out of file handles
# after testing ~1020 small non-FST files.
def maybeFST(filename):
  with open(filename, 'rb') as f:
    buf = f.read(16)
    if len(buf) < 16: return False
    # Same check as c_wkoffit in librmn
    return buf[12:] == b'STDR'

def get_file_modification_time(path:str,mode:str,caller_class,exception_class):
    import os.path
    import datetime
    import time
    import pathlib
    file = pathlib.Path(path)
    if not file.exists():
        return datetime.datetime.now()
    if (mode == rmn.FST_RO) and (not maybeFST(path)):
        raise exception_class(caller_class + 'not an fst standard file!')
   
    file_modification_time = time.ctime(os.path.getmtime(path))
    file_modification_time = datetime.datetime.strptime(file_modification_time, "%a %b %d %H:%M:%S %Y")
    del os.path
    del datetime
    del time
    del pathlib
    return file_modification_time    
    
def get_records(keys,materialize):
    if materialize:
        records = [rmn.fstluk(k) for k in keys]
    else:    
        records = [rmn.fstprm(k) for k in keys]
    return records   

def get_all_record_keys(file_id, subset):
    if (subset is None) == False:
        keys = rmn.fstinl(file_id,**subset)
    else:
        keys = rmn.fstinl(file_id)
    return keys    

def get_level_and_kind(ip1:int):
    #logger.debug('ip1',ip1)
    level_kind = rmn.convertIp(rmn.CONVIP_DECODE,int(ip1))
    #logger.debug('level_kind',level_kind)
    kind = int(level_kind[1])
    level = level_kind[0]
    level = float("%.6f"%-1) if kind == -1 else float("%.6f"%level)
    return level, kind
    #df.at[i,'kind'] = kind
    #df.at[i,'level'] = float("%.6f"%-1) if df.at[i,'kind'] == -1 else float("%.6f"%level)

def set_surface(df, i, meter_levels):
    if (df.at[i,'kind'] == 5) and (df.at[i,'level'] == 1):
        df.at[i,'surface'] = True
    elif (df.at[i,'kind'] == 4) and (df.at[i,'level'] in meter_levels):
        df.at[i,'surface'] = True
    elif (df.at[i,'kind'] == 1) and (df.at[i,'level'] == 1):
        df.at[i,'surface'] = True
    else:
        df.at[i,'surface'] = False    

def create_grid_identifier(nomvar:str,ip1:int,ip2:int,ig1:int,ig2:int) -> str:
    if nomvar in [">>", "^^", "!!", "!!SF", "HY"]:
        grid = "".join([str(ip1),str(ip2)])
    else:
        grid = "".join([str(ig1),str(ig2)])
    return grid

def create_printable_date_of_observation(date:int):
    #def stamp2datetime (date):
    from rpnpy.rpndate import RPNDate
    dummy_stamps = (0, 10101011)
    if date not in dummy_stamps:
        return RPNDate(int(date)).toDateTime().replace(tzinfo=None)
    else:
        return str(date)
    # if dateo == 0:
    #     return str(dateo)
    # dt = rmn_dateo_to_datetime_object(dateo)
    # return dt.strftime('%Y%m%d %H%M%S')

def rmn_dateo_to_datetime_object(dateo:int):
    import datetime
    res = rmn.newdate(rmn.NEWDATE_STAMP2PRINT, dateo)
    date_str = str(res[0])
    if res[1]:
        time_str = str(res[1])[:-2]
    else:
        time_str = '000000'
    date_str = "".join([date_str,time_str])
    return datetime.datetime.strptime(date_str, '%Y%m%d%H%M%S')

def get_unit(df, i, nomvar):
    import constants
    unit = constants.STDVAR.loc[constants.STDVAR['nomvar'] == f'{nomvar}']['unit'].values
    if len(unit):
        df.at[i,'unit'] = unit[0]
    else:
        df.at[i,'unit'] = 'scalar'
    return df

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
    etiket = raw_etiket
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
        etiket = raw_etiket[2:7]
        implementation = raw_etiket[7]
    elif re.match(re_match_cmc_ensemble,raw_etiket):
        run = raw_etiket[:2]
        etiket = raw_etiket[2:7]
        implementation = raw_etiket[7]
        ensemble_member = raw_etiket[8:11]
    elif re.match(re_match_spooki_no_ensemble,raw_etiket):
        run = raw_etiket[:2]
        etiket = raw_etiket[2:8]
        implementation = raw_etiket[8]
    elif re.match(re_match_spooki_ensemble,raw_etiket):
        run = raw_etiket[:2]
        etiket = raw_etiket[2:8]
        implementation = raw_etiket[8]
        ensemble_member = raw_etiket[9:12]
    else:
        etiket = raw_etiket
    return etiket,run,implementation,ensemble_member
