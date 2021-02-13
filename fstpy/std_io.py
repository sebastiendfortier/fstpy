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