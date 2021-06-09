# -*- coding: utf-8 -*-
import ctypes
from fstpy.plugin import Plugin
import math
import sys

import numpy as np
import pandas as pd
import rpnpy.librmn.all as rmn
import rpnpy.vgd.proto as vgdp

from .dataframe import set_vertical_coordinate_type
from .std_reader import load_data
from .utils import initializer

STANDARD_ATMOSPHERE = 1013.25

class PressureError(Exception):
    pass

class Pressure(Plugin):
    """creates a pressure field associated to a level for each identified vertical coordinate type

    :param df: input dataframe 
    :type df: pd.DataFrame
    :param standard_atmosphere: calculate pressure in standard atmosphere if specified, defaults to False
    :type standard_atmosphere: bool, optional
    """
    @initializer
    def __init__(self,df:pd.DataFrame, standard_atmosphere:bool=False):
        if self.df.empty:
            raise PressureError('Pressure - no data to process')
        if 'vctype' not in self.df.columns:
            self.df = set_vertical_coordinate_type(self.df)

    def compute(self) -> pd.DataFrame:
        """groups records by grid->vctype->forecast_hour then applies the appropriate algorithm to compute the pressure

        :return: a dataframe containing available pressure
        :rtype: pd.DataFrame
        """
        pxdfs=[]
        for _,grid in self.df.groupby(['grid']):
            meta_df = grid.query('nomvar in ["!!","HY","P0","PT",">>","^^","PN"]')
            meta_df = load_data(meta_df)
            vctypes_groups = grid.groupby(['vctype'])
            for _, vt in vctypes_groups:
                vctype = vt.vctype.iloc[0]
                fh_groups = vt.groupby(['forecast_hour'])
                for _, fh in fh_groups:
                    px_df = self._compute_pressure(fh,meta_df,vctype)
                    if not(px_df is None):
                        pxdfs.append(px_df)
        if len(pxdfs) > 1:                     
            res = pd.concat(pxdfs,ignore_index=True)
        elif len(pxdfs) == 1: 
            res = pxdfs[0]
        else:
            res = None
        return res

    def _compute_pressure(self,df:pd.DataFrame,meta_df:pd.DataFrame,vctype:str) -> pd.DataFrame:
        """select approprite algorithm according to vctype

        :param df: input dataframe containing a single grid
        :type df: pd.DataFrame
        :param meta_df: metadata dataframe for this grid
        :type meta_df: pd.DataFrame
        :param vctype: vertical coordinate type
        :type vctype: str
        :return: pressure dataframe for all levels of the current vctype
        :rtype: pd.DataFrame
        """
        if vctype == "UNKNOWN":
            px_df = None

        elif vctype == "HYBRID":
            sys.stdout.write('Found HYBRID vertical coordinate type - computing pressure\n')
            px_df = compute_pressure_from_hyb_coord_df(df,meta_df,self.standard_atmosphere)

        elif (vctype == "HYBRID_5005") or( vctype == "HYBRID_STAGGERED"):
            sys.stdout.write('Found HYBRID STAGGERED (5005,5002) vertical coordinate type - computing pressure\n')
            px_df = compute_pressure_from_hybstag_coord_df(df,meta_df,self.standard_atmosphere)

        elif vctype == "PRESSURE":
            sys.stdout.write('Found PRESSURE vertical coordinate type - computing pressure\n')
            px_df = compute_pressure_from_pressure_coord_df(df,self.standard_atmosphere)

        elif vctype == "ETA":
            sys.stdout.write('Found ETA vertical coordinate type - computing pressure\n')
            px_df = compute_pressure_from_eta_coord_df(df,meta_df,self.standard_atmosphere)

        elif vctype == "SIGMA":
            sys.stdout.write('Found SIGMA vertical coordinate type - computing pressure\n')
            px_df = compute_pressure_from_sigma_coord_df(df,meta_df,self.standard_atmosphere)

        return px_df
   
###################################################################################  
###################################################################################  
class Pressure2Pressure:
    """Encompasses information and algorithms to compute pressure for PRESSURE vertical coordinate type

    """
    def __init__(self) -> None:
        pass
    def pressure(self,level,shape):
        pres = np.full(shape,level,dtype=np.float32,order='F')
        return pres
     
###################################################################################            
def compute_pressure_from_pressure_coord_array(levels:list,shape:tuple) -> list:
    """compute pressure array for a PRESSURE vertical coordinate type

    :param levels: list of pressure levels in millibar
    :type levels: list
    :param shape: pressure array shape
    :type shape: tuple
    :return: list of dictionnaries {level:np.ndarray}
    :rtype: list
    """
    p = Pressure2Pressure()
    pressures=[]

    for lvl in levels:
        mydict = {}
        pres = p.pressure(lvl,shape)
        if pres is None:
            continue
        mydict[lvl]=pres
        pressures.append(mydict)    
    return pressures

def compute_pressure_from_pressure_coord_df(df:pd.DataFrame,standard_atmosphere:bool=False) -> pd.DataFrame:
    """Compute the pressure matrix for a dataframe containing one vertical coordinate type (vctype) of type PRESSURE 
       and only one forecast hour

    :param df: contains vaiables of the same vctype (PRESSURE)
    :type df: pd.DataFrame
    :param standard_atmosphere: calculate pressure in standard atmosphere if specified, defaults to False
    :type standard_atmosphere: bool
    :return: dataframe containing PX records for all found levels in the input dataframe
    :rtype: pd.DataFrame
    """
    if df.empty:
        return None
    df = df.drop_duplicates('ip1')
    p = Pressure2Pressure()
    press = []    
    for i in df.index:
        lvl = df.at[i,'level']
        datyp = 5 # E
        nbits = 32
        px_s = create_px_record(df, i, datyp, nbits,standard_atmosphere)
        pres = p.pressure(lvl,(df.at[i,'ni'],df.at[i,'nj']))
        if pres is None:
            continue
        px_s['d'] = pres
        press.append(px_s)
    pressure_df = pd.DataFrame(press)
    # pressure_df = unit_convert(pressure_df,to_unit_name='hectoPascal')
    return pressure_df 
            
###################################################################################
###################################################################################
SIGMA_KIND = 1
SIGMA_VERSION = 1
class Sigma2Pressure:
    """Encompasses information and algorithms to compute pressure for SIGMA vertical coordinate type

    :param levels: list of levels to create the vgrid descriptor
    :type levels: list
    :param p0_data: surface pressure array
    :type p0_data: np.ndarray
    :param standard_atmosphere: use standard atmosphere algorithm, defaults to False
    :type standard_atmosphere: bool, optional
    """
    def __init__(self,levels:list,p0_data:np.ndarray,standard_atmosphere:bool=False) -> None:

        self.levels = levels
        self.levels = np.sort(self.levels)
        self.p0_data = p0_data
        self.standard_atmosphere = standard_atmosphere
        if not self.standard_atmosphere:
            self.create_vgrid_descriptor()

    def __del__(self):
        if not self.standard_atmosphere:
            vgdp.c_vgd_free(self.myvgd)

    def create_vgrid_descriptor(self):
        myvgd = vgdp.c_vgd_construct()
        # see https://wiki.cmc.ec.gc.ca/wiki/Vgrid/C_interface/Cvgd_new_gen for kind and version
        status = vgdp.c_vgd_new_gen(myvgd, SIGMA_KIND, SIGMA_VERSION, self.levels, len(self.levels), None,None,None,None,None,0,0,None,None)
        if status:
            sys.stderr.write("Sigma2Pressure - There was a problem creating the VGridDescriptor\n")
        self.myvgd = myvgd   


    def std_atm_pressure(self,level:float):
        pres = self.sigma_to_pres(level)
        return pres

    def vgrid_pressure(self,ip1:int):
        ip = ctypes.c_int(ip1)
        pres = np.empty((self.p0_data.shape[0],self.p0_data.shape[1],1),dtype='float32', order='F')
        # p0 = unit_convert_array(self.p0_data,from_unit_name='millibar',to_unit_name='pascal') equals * 100.
        status = vgdp.c_vgd_levels(self.myvgd, self.p0_data.shape[0], self.p0_data.shape[1], 1, ip, pres, self.p0_data*100.0, 0)
        if status:
            sys.stderr.write("Sigma2Pressure - There was a problem creating the pressure\n")
            pres = None
        else:
            pres = np.squeeze(pres)    
            # pres = unit_convert_array(pres,from_unit_name='pascal',to_unit_name='hectoPascal')
 
        return pres/100.0

    def sigma_to_pres(self,level:float) -> np.ndarray:
        """sigma to pressure conversion function

        :param levels: current level
        :type level: float
        :return: pressure array for current level
        :rtype: np.ndarray
        """
        pres_value = STANDARD_ATMOSPHERE * level
        pres = np.full(self.p0_data.shape,pres_value,dtype=np.float32,order='F')
        return pres    
###################################################################################            
def compute_pressure_from_sigma_coord_array(levels:list,p0_data:np.ndarray,standard_atmosphere:bool=False) -> list:
    """compute pressure array for a SIGMA vertical coordinate type

    :param levels: list of pressure levels in sigma coord
    :type levels: list
    :param p0_data: surface pressure array
    :type p0_data: np.ndarray
    :param standard_atmosphere: use standard atmosphere algorithm, defaults to False
    :type standard_atmosphere: bool, optional
    :return: list of dictionnaries {level:np.ndarray}
    :rtype: list
    """
    p = Sigma2Pressure(levels,p0_data,standard_atmosphere)
    ips = convert_levels_to_ips(levels, SIGMA_KIND)

    pressures=[]
    if standard_atmosphere:
        for lvl,ip in zip(levels,ips):
            mydict = {}
            pres = p.std_atm_pressure(lvl)
            if pres is None:
                continue
            mydict[ip]=pres
            pressures.append(mydict)    
    else:
        for ip in ips:
            mydict = {}
            pres = p.vgrid_pressure(ip)
            if pres is None:
                continue
            mydict[ip]=pres
            pressures.append(mydict)    
    return pressures

def get_sigma_metadata(meta_df,forecast_hour):
    p0_df = meta_df.query(f'(nomvar=="P0") and (forecast_hour=="{forecast_hour}")')
    if p0_df.empty:
        return None,None,None
    p0_data = p0_df.iloc[0]['d']
    return p0_data, p0_df.iloc[0]['datyp'], p0_df.iloc[0]['nbits'] 

def compute_pressure_from_sigma_coord_df(df:pd.DataFrame,meta_df:pd.DataFrame,standard_atmosphere:bool=False) -> pd.DataFrame:
    """Compute the pressure matrix for a dataframe containing one vertical coordinate type (vctype) of type SIGMA 
       and only one forecast hour

    :param df: contains variables of the same vctype (SIGMA)
    :type df: pd.DataFrame
    :param df: contains all accompanying metadata (P0)
    :type df: pd.DataFrame
    :param standard_atmosphere: calculate pressure in standard atmosphere if specified, defaults to False
    :type standard_atmosphere: bool
    :return: dataframe containing PX records for all found levels in the input dataframe
    :rtype: pd.DataFrame
    """
    forecast_hour = df.iloc[0]['forecast_hour']
    p0_data,datyp, nbits = get_sigma_metadata(meta_df,forecast_hour)
    if p0_data is None:
        return None
    if df.empty:
        return None
    df = df.drop_duplicates('ip1')
    levels = df.level.unique()
    p = Sigma2Pressure(levels,p0_data,standard_atmosphere)
    press = []    
    for i in df.index:
        ip = df.at[i,'ip1']
        lvl = df.at[i,'level']
        px_s = create_px_record(df, i, datyp, nbits, standard_atmosphere)
        if standard_atmosphere:
            pres = p.std_atm_pressure(lvl)
        else:
            pres = p.vgrid_pressure(ip)
        if pres is None:
            continue    
        px_s['d'] = pres
        press.append(px_s)
    pressure_df = pd.DataFrame(press)
    # pressure_df = unit_convert(pressure_df,to_unit_name='hectoPascal')
    return pressure_df               
###################################################################################
###################################################################################
ETA_KIND = 1
ETA_VERSION = 2
class Eta2Pressure:
    """Encompasses information and algorithms to compute pressure for SIGMA vertical coordinate type

    :param levels: list of levels to create the vgrid descriptor
    :type levels: list
    :param pt_data: top pressure array (at least 1 value), if None bb_data must be supplied
    :type pt_data: np.ndarray
    :param bb_data: vertical coordinate descriptor table, if None pt_data must be supplied
    :type bb_data: np.ndarray
    :param p0_data: surface pressure array
    :type p0_data: np.ndarray
    :param standard_atmosphere: use standard atmosphere algorithm, defaults to False
    :type standard_atmosphere: bool, optional
    """
    def __init__(self,levels:list,pt_data:np.ndarray,bb_data:np.ndarray,p0_data:np.ndarray,standard_atmosphere:bool=False) -> None:
        self.levels = np.sort(levels)
        self.levels = self.levels[self.levels <= 1.0]
        self.pt_data = pt_data
        self.bb_data = bb_data
        self.p0_data = p0_data
        self.standard_atmosphere = standard_atmosphere
        if not self.standard_atmosphere:
            self.create_vgrid_descriptor()

    def __del__(self):
        if not self.standard_atmosphere:
            pass

    def create_vgrid_descriptor(self):
        self.get_ptop()
        myvgd = vgdp.c_vgd_construct()
        # see https://wiki.cmc.ec.gc.ca/wiki/Vgrid/C_interface/Cvgd_new_gen for kind and version
        ptop = ctypes.pointer(ctypes.c_double(self.ptop))
        status = vgdp.c_vgd_new_gen(myvgd, ETA_KIND, ETA_VERSION, self.levels.astype('float32'), len(self.levels), None,None,ptop,None,None,0,0,None,None)
        if status:
            sys.stderr.write("Eta2Pressure - There was a problem creating the VGridDescriptor\n")
        self.myvgd = myvgd   

    def get_ptop(self):
        if  not(self.pt_data is None):
            self.ptop = self.pt_data.flatten()[0]*100.0

        elif not(self.bb_data is None):
            # get value from !!
            self.ptop = self.bb_data[0]


    def std_atm_pressure(self,level):
        pres = self.eta_to_pres(level)
        return pres


    def vgrid_pressure(self,ip1):
        ip = ctypes.c_int(ip1)
        pres = np.empty((self.p0_data.shape[0],self.p0_data.shape[1],1),dtype='float32', order='F')
 
        status = vgdp.c_vgd_levels(self.myvgd, self.p0_data.shape[0], self.p0_data.shape[1], 1, ip, pres, self.p0_data*100.0, 0)
        if status:
            sys.stderr.write("Eta2Pressure - There was a problem creating the pressure\n")
            pres = None
        else:
            pres = np.squeeze(pres)    
 
        return pres/100.0

    def eta_to_pres(self,level:float) -> np.ndarray:
        """eta to pressure conversion function

        :param level: current level
        :type level: float
        :return: pressure array for current level
        :rtype: np.ndarray
        """
        self.get_ptop()    
        ptop = self.ptop/100.0
        pres_value = (ptop * ( 1.0 - level)) + level * STANDARD_ATMOSPHERE
        pres = np.full(self.p0_data.shape,pres_value,dtype=np.float32,order='F')
        return pres
###################################################################################
def compute_pressure_from_eta_coord_array(levels:list,pt_data:np.ndarray,bb_data:np.ndarray,p0_data:np.ndarray,standard_atmosphere:bool=False) -> list:
    """compute pressure array for a ETA vertical coordinate type

    :param levels: list of pressure levels in ETA coord
    :type levels: list
    :param pt_data: top pressure array (at least 1 value), if None bb_data must be supplied
    :type pt_data: np.ndarray
    :param bb_data: vertical coordinate descriptor table, if None pt_data must be supplied
    :type bb_data: np.ndarray
    :param p0_data: surface pressure array
    :type p0_data: np.ndarray
    :param standard_atmosphere: use standard atmosphere algorithm, defaults to False
    :type standard_atmosphere: bool, optional
    :return: list of dictionnaries {level:np.ndarray}
    :rtype: list
    """
    p = Eta2Pressure(levels,pt_data,bb_data,p0_data,standard_atmosphere)
    ips = convert_levels_to_ips(levels, ETA_KIND)

    pressures=[]
    if standard_atmosphere:
        for lvl,ip in zip(levels,ips):
            mydict = {}
            pres = p.std_atm_pressure(lvl)
            if pres is None:
                continue
            mydict[ip]=pres
            pressures.append(mydict)    
    else:
        for ip in ips:
            mydict = {}
            pres = p.vgrid_pressure(ip)
            if pres is None:
                continue
            mydict[ip]=pres
            pressures.append(mydict)    
    return pressures

def get_eta_metadata(meta_df,forecast_hour):
    p0_df = meta_df.query(f'(nomvar=="P0") and (forecast_hour=="{forecast_hour}")')
    if p0_df.empty:
        return None,None,None,None,None
    p0_data = p0_df.iloc[0]['d']    
    pt_df = meta_df.query(f'(nomvar=="PT") and (forecast_hour=="{forecast_hour}")')
    if not pt_df.empty: 
        pt_data = pt_df.iloc[0]['d']
    else:
        pt_data = None,None,None,None,None
    bb_df = meta_df.query('(nomvar=="!!") and (ig1==1002)')
    if not bb_df.empty: 
        bb_data = bb_df.iloc[0]['d']
    else:
        bb_data = None,None,None,None,None
    if bb_df.empty and pt_df.empty:
        return None,None,None,None,None
    return p0_data, pt_data, bb_data, p0_df.iloc[0]['datyp'], p0_df.iloc[0]['nbits']    

def compute_pressure_from_eta_coord_df(df:pd.DataFrame,meta_df:pd.DataFrame,standard_atmosphere:bool=False) -> pd.DataFrame:
    """Compute the pressure matrix for a dataframe containing one vertical coordinate type (vctype) of type ETA 
       and only one forecast hour

    :param df: contains variables of the same vctype (ETA)
    :type df: pd.DataFrame
    :param df: contains all accompanying metadata (P0, PT or !!)
    :type df: pd.DataFrame
    :param standard_atmosphere: calculate pressure in standard atmosphere if specified, defaults to False
    :type standard_atmosphere: bool
    :return: dataframe containing PX records for all found levels in the input dataframe
    :rtype: pd.DataFrame
    """
    forecast_hour = df.iloc[0]['forecast_hour']
    p0_data, pt_data, bb_data, datyp, nbits = get_eta_metadata(meta_df,forecast_hour)
    if p0_data is None:
        return None
    if df.empty:
        return None
    df = df.drop_duplicates('ip1')
    levels = df.level.unique()
    p = Eta2Pressure(levels,pt_data,bb_data,p0_data,standard_atmosphere)
    press = []    
    for i in df.index:
        ip = df.at[i,'ip1']
        lvl = df.at[i,'level']
        px_s = create_px_record(df, i, datyp, nbits,standard_atmosphere)
        if standard_atmosphere:
            pres = p.std_atm_pressure(lvl)
        else:
            pres = p.vgrid_pressure(ip)
        if pres is None:
            continue    
        px_s['d'] = pres
        press.append(px_s)
    pressure_df = pd.DataFrame(press)
    return pressure_df            
###################################################################################
###################################################################################
HYBRID_KIND=5
HYBRID_VERSION=1
class Hybrid2Pressure:
    """Encompasses information and algorithms to compute pressure for HYBRID vertical coordinate type

    :param levels: list of levels to create the vgrid descriptor
    :type levels: list
    :param hy_data:  GEM hybrid vertical coordinate descriptor array
    :type hy_data: np.ndarray
    :param hy_ig1: reference pressure
    :type hy_ig1: float
    :param hy_ig2: r coefficient
    :type hy_ig2: float
    :param bb_data: vertical coordinate descriptor table
    :type bb_data: np.ndarray
    :param p0_data: surface pressure array
    :type p0_data: np.ndarray
    :param standard_atmosphere: use standard atmosphere algorithm, defaults to False
    :type standard_atmosphere: bool, optional
    """
    def __init__(self,hy_data:np.ndarray,hy_ig1:float,hy_ig2:float,p0_data:np.ndarray,levels:list,standard_atmosphere:bool=False) -> None:
        self.hy_data = hy_data
        self.hy_ig1 = hy_ig1
        self.hy_ig2 = hy_ig2
        self.p0_data = p0_data
        self.levels = levels
        # self.levels = np.append(self.levels,1.0)
        self.levels.sort()
        self.levels = self.levels.astype('float32')
        self.standard_atmosphere = standard_atmosphere
        if not self.standard_atmosphere:
            self.create_vgrid_descriptor()

    def __del__(self):
        if not self.standard_atmosphere:
            vgdp.c_vgd_free(self.myvgd)

    
    def create_vgrid_descriptor(self):
        self.get_hybrid_coord_info()
        myvgd = vgdp.c_vgd_construct()
        # see https://wiki.cmc.ec.gc.ca/wiki/Vgrid/C_interface/Cvgd_new_gen for kind and version
        ptop = ctypes.pointer(ctypes.c_double(self.ptop*100.0))
        pref = ctypes.pointer(ctypes.c_double(self.pref*100.0))
        rcoef = ctypes.pointer(ctypes.c_float(self.rcoef))
        status = vgdp.c_vgd_new_gen(myvgd, HYBRID_KIND, HYBRID_VERSION, self.levels, len(self.levels), rcoef,None,ptop,pref,None,0,0,None,None)
        if status:
            sys.stderr.write("Hybrid2Pressure - There was a problem creating the VGridDescriptor\n")
        self.myvgd = myvgd   


    def get_hybrid_coord_info(self):
        self.ptop = self.hy_data[0]
        self.pref = self.hy_ig1
        self.rcoef = self.hy_ig2/1000.0

    def std_atm_pressure(self,lvl):
        self.get_ptop_pref_rcoef()
        pres = self.hyb_to_pres(lvl)
        return pres

    def vgrid_pressure(self,ip1):
        ip = ctypes.c_int(ip1)
        pres = np.empty((self.p0_data.shape[0],self.p0_data.shape[1],1),dtype='float32', order='F')
        status = vgdp.c_vgd_levels(self.myvgd, self.p0_data.shape[0], self.p0_data.shape[1], 1, ip, pres, self.p0_data*100.0, 0)
        if status:
            sys.stderr.write("Hybrid2Pressure - There was a problem creating the pressure\n")
            pres = None
        else:
            pres = np.squeeze(pres)    
        return pres/100.0    

    def get_ptop_pref_rcoef(self):
        """get ptop, pref and rcoef values from hy pds

        :param hy_df: dataframe of hy
        :type hy_df: pd.DataFrame
        :param levels: level array
        :type levels: np.ndarray
        :return: presure at top, reference pressure, reference coefficient
        :rtype: tuple
        """
        self.ptop = self.hy_data[0]
        self.pref = self.hy_ig1
        self.rcoef = self.hy_ig2 / 1000.0


    def hyb_to_pres(self,level):
        """hybrid to pressure conversion function

        :param level: current level
        :type level: float
        :return: pressure array for current level
        :rtype: np.ndarray
        """

        term0 =  (self.ptop / self.pref)
        # term1 = (level + (1.0 - level) * term0)
        # term2 = (term1 - term0) 
        term3 = (1.0 / (1.0 - term0))
        term4 = (level - term0)
        # evalTerm0 = (0.0 if term2 < 0 else term2 )
        evalTerm1 = (0.0 if term4 < 0 else term4 )
        # term5 = (math.pow( evalTerm0 * term3, self.rcoef))
        term6 = (math.pow( evalTerm1 * term3, self.rcoef))

        pres_value = ( self.pref * ( level - term6 )) + term6 * STANDARD_ATMOSPHERE
        pres = np.full(self.p0_data.shape,pres_value,dtype=np.float32,order='F')
        return pres


###################################################################################        
def compute_pressure_from_hyb_coord_array(hy_data:np.ndarray,hy_ig1:float,hy_ig2:float,p0_data:np.ndarray,levels:list,standard_atmosphere:bool=False) -> list:
    """compute pressure array for a HYBRID vertical coordinate type

    :param hy_data:  GEM hybrid vertical coordinate descriptor array
    :type hy_data: np.ndarray
    :param hy_ig1: reference pressure
    :type hy_ig1: float
    :param hy_ig2: r coefficient
    :type hy_ig2: float
    :param p0_data: surface pressure array
    :type p0_data: np.ndarray
    :param levels: list of pressure levels in HYBRID coord
    :type levels: list
    :param standard_atmosphere: use standard atmosphere algorithm, defaults to False
    :type standard_atmosphere: bool, optional
    :return: list of dictionnaries {level:np.ndarray}
    :rtype: list
    """
    p = Hybrid2Pressure(hy_data,hy_ig1,hy_ig2,p0_data,levels,standard_atmosphere)
    ips = convert_levels_to_ips(levels, HYBRID_KIND)

    pressures=[]
    if standard_atmosphere:
        for lvl,ip in zip(levels,ips):
            mydict = {}
            pres = p.std_atm_pressure(lvl)
            if pres is None:
                continue
            mydict[ip]=pres
            pressures.append(mydict)    
    else:
        for ip in ips:
            mydict = {}
            pres = p.vgrid_pressure(ip)
            if pres is None:
                continue
            mydict[ip]=pres
            pressures.append(mydict)     
    return pressures

def get_hyb_metadata(meta_df,forecast_hour):
    p0_df = meta_df.query(f'(nomvar=="P0") and (forecast_hour=="{forecast_hour}")')
    if p0_df.empty:
        return None,None,None,None
    p0_data = p0_df.iloc[0]['d']
    hy_df = meta_df.query('nomvar=="HY"')
    if hy_df.empty:
        return None,None,None,None
    hy_data = hy_df.iloc[0]['d']
    hy_ig1 = hy_df.iloc[0]['ig1']
    hy_ig2 = hy_df.iloc[0]['ig2']
    return p0_data,hy_data,hy_ig1,hy_ig2, p0_df.iloc[0]['datyp'], p0_df.iloc[0]['nbits'] 

def compute_pressure_from_hyb_coord_df(df:pd.DataFrame,meta_df:pd.DataFrame,standard_atmosphere:bool=False) -> pd.DataFrame:
    """Compute the pressure matrix for a dataframe containing one vertical coordinate type (vctype) of type HYBRID 
       and only one forecast hour

    :param df: contains variables of the same vctype (HYBRID)
    :type df: pd.DataFrame
    :param meta_df: contains all accompanying metadata (P0, HY)
    :type meta_df: pd.DataFrame
    :param standard_atmosphere: calculate pressure in standard atmosphere if specified, defaults to False
    :type standard_atmosphere: bool
    :return: dataframe containing PX records for all found levels in the input dataframe
    :rtype: pd.DataFrame
    """
    forecast_hour = df.iloc[0]['forecast_hour']
    p0_data,hy_data,hy_ig1,hy_ig2, datyp, nbits = get_hyb_metadata(meta_df,forecast_hour)
    if p0_data is None:
        return None
    if df.empty:
        return None
    df = df.drop_duplicates('ip1')
    levels = df.level.unique()
    p = Hybrid2Pressure(hy_data,hy_ig1,hy_ig2,p0_data,levels,standard_atmosphere)
    press = []    
    for i in df.index:
        ip = df.at[i,'ip1']
        lvl = df.at[i,'level']
        px_s = create_px_record(df, i, datyp, nbits,standard_atmosphere)
        if standard_atmosphere:
            pres = p.std_atm_pressure(lvl)
        else:
            pres = p.vgrid_pressure(ip)
        if pres is None:
            continue    
        px_s['d'] = pres
        press.append(px_s)
    pressure_df = pd.DataFrame(press)
    return pressure_df
###################################################################################
###################################################################################
class HybridStaggered2Pressure:
    """Encompasses information and algorithms to compute pressure for HYBRID_STAGGERED vertical coordinate type

    :param bb_data: vertical coordinate descriptor table
    :type bb_data: np.ndarray
    :param p0_data: surface pressure array
    :type p0_data: np.ndarray
    :param standard_atmosphere: use standard atmosphere algorithm, defaults to False
    :type standard_atmosphere: bool, optional
    """
    def __init__(self,bb_data:np.ndarray,p0_data:np.ndarray,standard_atmosphere:bool=False) -> None:
        self.bb_data = bb_data
        self.p0_data = p0_data
        self.standard_atmosphere = standard_atmosphere
        if not self.standard_atmosphere:
            self.create_vgrid_descriptor()

    def __del__(self):
        if not self.standard_atmosphere:
            vgdp.c_vgd_free(self.myvgd)

    def create_vgrid_descriptor(self):
        myvgd = vgdp.c_vgd_construct()
        bb_dataptr = self.bb_data.ctypes.data_as(ctypes.POINTER(ctypes.c_double))
        status = vgdp.c_vgd_new_from_table(myvgd,bb_dataptr,self.bb_data.shape[0],self.bb_data.shape[1],1)
        if status:
            sys.stderr.write("HybridStaggered2Pressure - There was a problem creating the VGridDescriptor\n")
        self.myvgd = myvgd   

    def get_std_atm_pressure(self, a:float, b:float, pref:float) -> np.ndarray:
        """ hybrid staggered to pressure conversion function
        Formule utilisee:  Px en Pa = exp( a + b * ln(p0*100.0/ pref)).
        On doit diviser le resultat par 100 pour le ramener en hPa.
        :param a :
        :type a : float
        :param b :
        :type b : float
        :param p0 : en hPa
        :type p0: float
        :param pref : en Pa
        :type pref: float
        :return pressure for current level
        :rtype: float
        """    
        pres_value = ( math.exp( a + b * math.log(STANDARD_ATMOSPHERE*100.0 / pref))/100.0)
        pres = np.full(self.p0_data.shape,pres_value,dtype=np.float32,order='F')
        return pres 

    def std_atm_pressure(self,ip1):
        ips = self.bb_data[0][3:].astype(int)
        ipindex = np.where(ips==ip1)
        a_8 = self.bb_data[1][3:]
        b_8 = self.bb_data[2][3:]
        pref = self.bb_data[1][1]
        pres = self.get_std_atm_pressure(a_8[ipindex][0], b_8[ipindex][0], pref)
        return pres

    def vgrid_pressure(self,ip1):
        ip = ctypes.c_int(ip1)
        pres = np.empty((self.p0_data.shape[0],self.p0_data.shape[1],1),dtype='float32', order='F')
        status = vgdp.c_vgd_levels(self.myvgd, self.p0_data.shape[0], self.p0_data.shape[1], 1, ip, pres, self.p0_data*100.0, 0)
        if status:
            sys.stderr.write("HybridStaggered2Pressure - There was a problem creating the pressure\n")
            pres = None
        else:
            pres = np.squeeze(pres)    
 
        return pres/100.0

    def get_pressure(self):
        if(self.standard_atmosphere):
            press_func = self.std_atm_pressure
        else:
            press_func = self.vgrid_pressure
        return press_func
###################################################################################
def compute_pressure_from_hybstag_coord_array(ip1s:list,bb_data:np.ndarray,p0_data:np.ndarray,standard_atmosphere:bool=False) -> np.ndarray:
    """compute pressure array for a HYBRID_STAGGERED vertical coordinate type

    :param bb_data: vertical coordinate descriptor table
    :type bb_data: np.ndarray
    :param p0_data: surface pressure array
    :type p0_data: np.ndarray
    :param standard_atmosphere: use standard atmosphere algorithm, defaults to False
    :type standard_atmosphere: bool, optional
    :return: list of dictionnaries {level:np.ndarray}
    :rtype: list
    """
    p = HybridStaggered2Pressure(bb_data,p0_data,standard_atmosphere)
    pressures=[]
    for ip in ip1s:
        mydict = {}
        pres = p.get_pressure()(ip)
        if pres is None:
            continue
        mydict[ip] = pres
        pressures.append(mydict)    
    return pressures

def get_hybstag_metadata(meta_df,forecast_hour):
    p0_df = meta_df.query(f'(nomvar=="P0") and (forecast_hour=="{forecast_hour}")')
    if p0_df.empty:
        return None,None,None,None
    p0_data = p0_df.iloc[0]['d']
    bb_df = meta_df.query('(nomvar=="!!") and (ig1 in [5002,5005])')
    if bb_df.empty:
        return None,None,None,None
    bb_data = bb_df.iloc[0]['d']

    return  p0_data, bb_data, p0_df.iloc[0]['datyp'], p0_df.iloc[0]['nbits']   

def compute_pressure_from_hybstag_coord_df(df:pd.DataFrame,meta_df:pd.DataFrame,standard_atmosphere:bool=False) -> pd.DataFrame:
    """Compute the pressure matrix for a dataframe containing one vertical coordinate type (vctype) of type HYBRID_STAGGERED 
       and only one forecast hour

    :param df: contains variables of the same vctype (HYBRID_STAGGERED)
    :type df: pd.DataFrame
    :param df: contains all accompanying metadata (P0, !!)
    :type df: pd.DataFrame
    :param standard_atmosphere: calculate pressure in standard atmosphere if specified, defaults to False
    :type standard_atmosphere: bool
    :return: dataframe containing PX records for all found levels in the input dataframe
    :rtype: pd.DataFrame
    """
    forecast_hour = df.iloc[0]['forecast_hour']
    p0_data, bb_data, datyp, nbits = get_hybstag_metadata(meta_df,forecast_hour)

    if p0_data is None:
        return None
    if df.empty:
        return None
    df = df.drop_duplicates('ip1')
    
    p = HybridStaggered2Pressure(bb_data,p0_data,standard_atmosphere)
    press = []    
    for i in df.index:
        ip = df.at[i,'ip1']
        if ip == 0:
            continue
        px_s = create_px_record(df, i, datyp, nbits,standard_atmosphere)
        pres = p.get_pressure()(ip)
        if pres is None:
            continue
        px_s['d'] = pres
        press.append(px_s)
    pressure_df = pd.DataFrame(press)

    return pressure_df

###################################################################################
###################################################################################
def create_px_record(df, i, datyp, nbits,standard_atmosphere:bool):
    nomvar = 'PX'
    unit = 'hectoPascal'
    description = 'Pressure of the Model'
    if standard_atmosphere:
        nomvar = 'PXSA'
        unit = 'millibar'
        description = 'Pressure of the model standard atmosphere'
    px_s = df.loc[i, df.columns != 'd'].copy(deep=True)
    px_s['nomvar'] = nomvar
    px_s['unit'] = unit
    px_s['description'] = description
    px_s['nbits'] = nbits
    px_s['datyp'] = datyp
    px_s['key'] = None
    return px_s

def convert_levels_to_ips(levels, kind):
    vip1_all = np.vectorize(rmn.ip1_all)
    ips = vip1_all(levels,kind)
    return ips
