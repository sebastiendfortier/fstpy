# -*- coding: utf-8 -*-
def create_encoded_etiket(label,run,implementation,ensemble_member):
    etiket  = label + run + implementation + ensemble_member
    return etiket

def create_encoded_dateo(pdateo):
    return pdateo

def create_encoded_datev(pdatev):
    return pdatev

def create_encoded_npas_and_ip2(fhour,deet):
    npas = 0
    ip2 = 0
    return npas,ip2

def create_decoded_ip1(level,kind_pkind):
    ip1 = 0
    return ip1

def create_encoded_ips(level,kind,ip3_dec,ip3_kind,ip3_pkind):
    ip1 = 0
    ip2 = 0
    ip3 = 0
    return ip1,ip2,ip3

def create_encoded_datyp(pdatyp):
    from .constants import DATYP_DICT
    new_dict = {v:k for k,v in DATYP_DICT.items()}    
    return new_dict[pdatyp]
