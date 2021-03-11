# -*- coding: utf-8 -*-
import datetime

import rpnpy.librmn.all as rmn

from fstpy import DATYP_DICT


def create_encoded_etiket(label,run,implementation,ensemble_member):
    
    etiket  = label + run + implementation + ensemble_member
    return etiket

def create_encoded_dateo(date_of_observation):
    return date_of_observation

def create_encoded_npas_and_ip2(forecast_hour:datetime.timedelta,deet:int):
    #ip2 = 6, deet = 300, np = 72
    #fhour = 21600
    #npas = hours/deet
    seconds = forecast_hour.seconds
    npas = seconds/deet
    ip2 = seconds/3600.
    return npas,ip2

def create_encoded_ip1(level,ip1_kind):
    return rmn.ip1_all(level,ip1_kind)

def create_encoded_ips(level,ip1_kind,ip3_dec,ip3_kind,ip3_pkind):
    ip1 = 0
    ip2 = 0
    ip3 = 0
    return ip1,ip2,ip3

def create_encoded_datyp(data_type_str):
    new_dict = {v:k for k,v in DATYP_DICT.items()}    
    return new_dict[data_type_str]

def modifiers_to_typvar2(zapped,filtered,interpolated,unit_converted,bounded,ensemble_extra_info,multiple_modifications):
     number_of_modifications = 0
     typvar2 = ''
     if zapped == True:
        number_of_modifications += 1
        typvar2 = 'Z'
     if filtered == True:
        number_of_modifications += 1
        typvar2 = 'F'
     if interpolated == True:
        number_of_modifications += 1
        typvar2 = 'I'
     if unit_converted == True:
        number_of_modifications += 1
        typvar2 = 'U'
     if bounded == True:
        number_of_modifications += 1
        typvar2 = 'B'
     if ensemble_extra_info == True:
        number_of_modifications += 1
        typvar2 = '!'
     if multiple_modifications == True:
        number_of_modifications += 1
        typvar2 = 'M'
     if number_of_modifications > 1:
        # more than one modification has been done. Force M
        typvar2 = 'M'
     return typvar2
   