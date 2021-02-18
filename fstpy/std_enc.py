# -*- coding: utf-8 -*-
def create_encoded_etiket(label,run,implementation,ensemble_member):
    etiket  = label + run + implementation + ensemble_member
    return etiket

def create_encoded_dateo(date_of_observation):
    return date_of_observation

def create_encoded_datev(date_of_validity):
    return date_of_validity

def create_encoded_npas_and_ip2(forecast_hour,deet):
    npas = 0
    ip2 = 0
    return npas,ip2

def create_decoded_ip1(level,kind_pkind):
    ip1 = 0
    return ip1

def create_encoded_ips(level,ip1_kind,ip3_dec,ip3_kind,ip3_pkind):
    ip1 = 0
    ip2 = 0
    ip3 = 0
    return ip1,ip2,ip3

def create_encoded_datyp(data_type_str):
    from .constants import DATYP_DICT
    new_dict = {v:k for k,v in DATYP_DICT.items()}    
    return new_dict[data_type_str]
