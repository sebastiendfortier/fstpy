# -*- coding: utf-8 -*-
import ctypes
import math
import sys

import numpy as np
import pandas as pd
import rpnpy.librmn.all as rmn
import rpnpy.vgd.all as vgd
import rpnpy.vgd.proto as vgdp

from fstpy.unit import do_unit_conversion, do_unit_conversion_array

from .dataframe_utils import select, zap
from .std_reader import load_data
from .utils import initializer, validate_df_not_empty

STANDARD_ATMOSPHERE = 1013.25

class PressureError(Exception):
    pass

class Pressure:
    plugin_requires = {'(nomvar in ["TT","TD"]) and (unit=="celsius")' }
    plugin_result = {'nomvar':'ES','etiket':'Pressure','unit':'celsius'}
    @initializer
    def __init__(self,df:pd.DataFrame, standard_atmosphere=False):
        validate_df_not_empty(df,Pressure,PressureError) 
        # self.df = load_data(self.df)
        # self.groups= get_groups(self.df)

    def compute(self) -> pd.DataFrame:
        pxdfs=[]
        for _,grid in self.df.groupby(['grid']):
            meta_df = grid.query('nomvar in ["!!","HY","P0","PT",">>","^^","PN"]')
            meta_df = load_data(meta_df)
            grid = pd.concat([grid, meta_df]).drop_duplicates(subset=['nomvar','typvar','etiket','ni','nj','nk','dateo','ip1','ip2','ip3','deet','npas','grtyp','datyp','nbits','ig1','ig2','ig3','ig4','key'],keep=False)
            vctypes_groups = grid.groupby(['vctype'])
            for _, vt in vctypes_groups:
                vctype = vt.vctype.iloc[0]
                # print(vctype)
                # print(meta_df[['nomvar','ni','nj','ip1','grid','vctype']],'\n',vt[['nomvar','ni','nj','ip1','grid','vctype']],'\n-------------\n')
                px_df = self.compute_pressure(vt,meta_df,vctype)
                if not(px_df is None):
                    pxdfs.append(px_df)
        if len(pxdfs) > 1:                     
            res = pd.concat(pxdfs)
        elif len(pxdfs) == 1: 
            res = pxdfs[0]
        else:
            res = None
        return res

    def compute_pressure(self,df,meta_df,vctype):
        if vctype == "UNKNOWN":
            px_df = None

        elif vctype == "HYBRID":
            p0_df = meta_df.query('nomvar=="P0"')
            if p0_df.empty:
                return None
            p0_data = p0_df.iloc[0]['d']
            hy_df = meta_df.query('nomvar=="HY"')
            if hy_df.empty:
                return None
            hy_data = hy_df.iloc[0]['d']
            hy_ig1 = hy_df.iloc[0]['ig1']
            hy_ig2 = hy_df.iloc[0]['ig2']
            px_df = compute_pressure_from_hyb_coord_df(df,hy_data,hy_ig1,hy_ig2,p0_data,self.standard_atmosphere)

        elif (vctype == "HYBRID_5005") or( vctype == "HYBRID_STAGGERED"):
            p0_df = meta_df.query('nomvar=="P0"')
            if p0_df.empty:
                return None
            p0_data = p0_df.iloc[0]['d']
            bb_df = meta_df.query('nomvar=="!!"')
            if bb_df.empty:
                return None
            bb_data = bb_df.iloc[0]['d']
            px_df = compute_pressure_from_hybstag_coord_df(df,bb_data,p0_data,self.standard_atmosphere)

        elif vctype == "PRESSURE":
            px_df = compute_pressure_from_pressure_coord_df(df)

        elif vctype == "ETA":
            p0_df = meta_df.query('nomvar=="P0"')
            if p0_df.empty:
                return None
            p0_data = p0_df.iloc[0]['d']
            pt_df = meta_df.query('nomvar=="PT"')
            if  not pt_df.empty:
                pt_data = pt_df.iloc[0]['d']
            else:
                pt_data = None
            bb_df = meta_df.query('nomvar=="!!"')
            if not bb_df.empty: 
                bb_data = bb_df.iloc[0]['d']
            else:
                bb_data = None
            if bb_df.empty and pt_df.empty:
                return None    
            px_df = compute_pressure_from_eta_coord_df(df,pt_data,bb_data,p0_data,self.standard_atmosphere)

        elif vctype == "SIGMA":
            p0_df = meta_df.query('nomvar=="P0"')
            if p0_df.empty:
                return None
            p0_data = p0_df.iloc[0]['d']
            px_df = compute_pressure_from_sigma_coord_df(df,p0_data,self.standard_atmosphere)

        return px_df
   
###################################################################################  
###################################################################################  
class Pressure2Pressure:
    def __init__(self) -> None:
        pass
    def pressure(self,level,shape):
        pres = np.full(shape,level,dtype=np.float32,order='F')
        return pres
     
###################################################################################            
def compute_pressure_from_pressure_coord_array(levels,kind,shape) -> list:
    p = Pressure2Pressure()
    vip1_all = np.vectorize(rmn.ip1_all)
    ips = vip1_all(levels,kind)
    pressures=[]

    for lvl,ip in zip(levels,ips):
        pres = p.pressure(lvl,shape)
        mydict = {}
        mydict[ip]=pres
        pressures.append(mydict)    
    return pressures

def compute_pressure_from_pressure_coord_df(df:pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return None
    df = df.drop_duplicates('ip1')
    p = Pressure2Pressure()
    press = []    
    for i in df.index:
        lvl = df.at[i,'level']
        px_s = df.loc[i, df.columns != 'd'].copy(deep=True)
        px_s['nomvar'] = "PX"
        px_s['unit'] = "pascal"
        px_s['description'] = "Pressure of the Model"
        pres = p.pressure(lvl,(df.at[i,'ni'],df.at[i,'nj']))
        px_s['d'] = pres
        press.append(px_s)
    pressure_df = pd.DataFrame(press)
    # pressure_df = do_unit_conversion(pressure_df,to_unit_name='hectoPascal')
    return pressure_df 
            
###################################################################################
###################################################################################
class Sigma2Pressure:
    def __init__(self,levels,p0_data,standard_atmosphere) -> None:
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
        # bb_dataptr = self.bb_data.ctypes.data_as(ctypes.POINTER(ctypes.c_double))
        # see https://wiki.cmc.ec.gc.ca/wiki/Vgrid/C_interface/Cvgd_new_gen for kind and version
        kind = 1
        version = 1
        status = vgdp.c_vgd_new_gen(myvgd, kind, version, self.levels, len(self.levels), None,None,None,None,None,0,0,None,None)
        if status:
            sys.stderr.write("Sigma2Pressure - There was a problem creating the VGridDescriptor\n")
        self.myvgd = myvgd   


    def std_atm_pressure(self,level):
        pres = self.get_pres_eta_to_pres(level)
        return pres

    def vgrid_pressure(self,ip1):
        ip = ctypes.c_int(ip1)
        pres = np.empty((self.p0_data.shape[0],self.p0_data.shape[1],1),dtype='float32', order='F')
        status = vgdp.c_vgd_levels(self.myvgd, self.p0_data.shape[0], self.p0_data.shape[1], 1, ip, pres, self.p0_data, 0)
        if status:
            sys.stderr.write("Sigma2Pressure - There was a problem creating the pressure\n")
            pres = None
        else:
            pres = np.squeeze(pres)    
            # pres = do_unit_conversion_array(pres,from_unit_name='pascal',to_unit_name='hectoPascal')
 
        return pres

    def get_pres_Sigma2Pressure_to_pres(levels):
        """Sigma2Pressure to pressure conversion function

        :param p0: surface pressure
        :type p0: float
        :param level: current level
        :type level: float
        :return: pressure for current level
        :rtype: float
        """
        def Sigma2Pressure_to_pres(level) -> float:
            return STANDARD_ATMOSPHERE * level
        vpres_to_Sigma2Pressure = np.vectorize(Sigma2Pressure_to_pres)
        pres_values = vpres_to_Sigma2Pressure(levels)
        return pres_values    
###################################################################################            
def compute_pressure_from_sigma_coord_array(levels,kind,p0_data,standard_atmosphere) -> list:
    p = Sigma2Pressure(levels,p0_data,standard_atmosphere)
    vip1_all = np.vectorize(rmn.ip1_all)
    ips = vip1_all(levels,kind)

    pressures=[]
    if standard_atmosphere:
        for lvl,ip in zip(levels,ips):
            pres = p.std_atm_pressure(lvl)
            mydict = {}
            mydict[ip]=pres
            pressures.append(mydict)    
    else:
        for ip in ips:
            pres = p.vgrid_pressure(ip)
            mydict = {}
            mydict[ip]=pres
            pressures.append(mydict)    
    return pressures

def compute_pressure_from_sigma_coord_df(df:pd.DataFrame,p0_data,standard_atmosphere) -> pd.DataFrame:
    if df.empty:
        return None
    df = df.drop_duplicates('ip1')
    levels = df.level.unique()
    p = Sigma2Pressure(levels,p0_data,standard_atmosphere)
    press = []    
    for i in df.index:
        ip = df.at[i,'ip1']
        lvl = df.at[i,'level']
        px_s = df.loc[i, df.columns != 'd'].copy(deep=True)
        px_s['nomvar'] = "PX"
        px_s['unit'] = "pascal"
        px_s['description'] = "Pressure of the Model"
        if standard_atmosphere:
            pres = p.std_atm_pressure(lvl)
        else:
            pres = p.vgrid_pressure(ip)
        px_s['d'] = pres
        press.append(px_s)
    pressure_df = pd.DataFrame(press)
    # pressure_df = do_unit_conversion(pressure_df,to_unit_name='hectoPascal')
    return pressure_df               
###################################################################################
###################################################################################
class Eta2Pressure:
    def __init__(self,levels,pt_data,bb_data,p0_data,standard_atmosphere) -> None:
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
            vgdp.c_vgd_free(self.myvgd)

    def create_vgrid_descriptor(self):
        self.get_ptop()
        myvgd = vgdp.c_vgd_construct()
        # bb_dataptr = self.bb_data.ctypes.data_as(ctypes.POINTER(ctypes.c_double))
        # see https://wiki.cmc.ec.gc.ca/wiki/Vgrid/C_interface/Cvgd_new_gen for kind and version
        kind = 1
        version = 2
        status = vgdp.c_vgd_new_gen(myvgd, kind, version, self.levels, len(self.levels), None,None,self.ptop,None,None,0,0,None,None)
        if status:
            sys.stderr.write("Eta2Pressure - There was a problem creating the VGridDescriptor\n")
        self.myvgd = myvgd   

    def get_ptop(self):
        if  not(self.pt_data is None):
            self.ptop = self.pt_data.flatten()[0]

        elif not(self.bb_data is None):
            # get value from !!
            self.ptop = self.bb_data[0]/100.0
            # ptop = ((float)(*BBfm)(0,1,0))/100.0
        self.ptop = ctypes.pointer(ctypes.c_double(self.ptop*100))    


    def std_atm_pressure(self,level):
        pres = self.get_pres_eta_to_pres(level)
        return pres

    def vgrid_pressure(self,ip1):
        ip = ctypes.c_int(ip1)
        pres = np.empty((self.p0_data.shape[0],self.p0_data.shape[1],1),dtype='float32', order='F')
        status = vgdp.c_vgd_levels(self.myvgd, self.p0_data.shape[0], self.p0_data.shape[1], 1, ip, pres, self.p0_data, 0)
        if status:
            sys.stderr.write("Eta2Pressure - There was a problem creating the pressure\n")
            pres = None
        else:
            pres = np.squeeze(pres)    
            # pres = do_unit_conversion_array(pres,from_unit_name='pascal',to_unit_name='hectoPascal')
 
        return pres

    def get_pres_eta_to_pres(self,level):
        """eta to pressure conversion function

        :param ptop: top pressure
        :type ptop: float
        :param level: current level
        :type level: float
        :param p0: surface pressure
        :type p0: float
        :return: pressure for current level
        :rtype: float
        """
        def eta_to_pres(level:float, ptop:float, p0:float) -> float:
            return (ptop * ( 1.0 - level)) + level * p0
        veta_to_pres = np.vectorize(eta_to_pres)
        pres_values = veta_to_pres(level,self.ptop,self.p0_data)
        return pres_values
###################################################################################
def compute_pressure_from_eta_coord_array(levels,kind,pt_data,bb_data,p0_data,standard_atmosphere) -> list:
    p = Eta2Pressure(levels,pt_data,bb_data,p0_data,standard_atmosphere)
    vip1_all = np.vectorize(rmn.ip1_all)
    ips = vip1_all(levels,kind)

    pressures=[]
    if standard_atmosphere:
        for lvl,ip in zip(levels,ips):
            pres = p.std_atm_pressure(lvl)
            mydict = {}
            mydict[ip]=pres
            pressures.append(mydict)    
    else:
        for ip in ips:
            pres = p.vgrid_pressure(ip)
            mydict = {}
            mydict[ip]=pres
            pressures.append(mydict)    
    return pressures

def compute_pressure_from_eta_coord_df(df:pd.DataFrame,pt_data,bb_data,p0_data,standard_atmosphere) -> pd.DataFrame:
    if df.empty:
        return None
    df = df.drop_duplicates('ip1')
    levels = df.level.unique()
    p = Eta2Pressure(levels,pt_data,bb_data,p0_data,standard_atmosphere)
    press = []    
    for i in df.index:
        ip = df.at[i,'ip1']
        lvl = df.at[i,'level']
        px_s = df.loc[i, df.columns != 'd'].copy(deep=True)
        px_s['nomvar'] = "PX"
        px_s['unit'] = "pascal"
        px_s['description'] = "Pressure of the Model"
        if standard_atmosphere:
            pres = p.std_atm_pressure(lvl)
        else:
            pres = p.vgrid_pressure(ip)
        px_s['d'] = pres
        press.append(px_s)
    pressure_df = pd.DataFrame(press)
    # pressure_df = do_unit_conversion(pressure_df,to_unit_name='hectoPascal')
    return pressure_df            
###################################################################################
###################################################################################
class Hybrid2Pressure:
    def __init__(self,hy_data,hy_ig1,hy_ig2,p0_data,levels,kind,standard_atmosphere) -> None:
        self.hy_data = hy_data
        self.hy_ig1 = hy_ig1
        self.hy_ig2 = hy_ig2
        self.p0_data = p0_data
        self.levels = levels
        # self.levels = np.append(self.levels,1.0)
        self.levels.sort()
        self.levels = self.levels.astype('float32')
        self.kind = kind
        self.standard_atmosphere = standard_atmosphere
        if not self.standard_atmosphere:
            self.create_vgrid_descriptor()

    def __del__(self):
        if not self.standard_atmosphere:
            vgdp.c_vgd_free(self.myvgd)

    
    def create_vgrid_descriptor(self):
        self.get_hybrid_coord_info()
        myvgd = vgdp.c_vgd_construct()
        # bb_dataptr = self.bb_data.ctypes.data_as(ctypes.POINTER(ctypes.c_double))
        # see https://wiki.cmc.ec.gc.ca/wiki/Vgrid/C_interface/Cvgd_new_gen for kind and version
        kind = 5
        version = 1
        ptop = ctypes.pointer(ctypes.c_double(self.ptop))
        pref = ctypes.pointer(ctypes.c_double(self.pref))
        rcoef = ctypes.pointer(ctypes.c_float(self.rcoef))
        status = vgdp.c_vgd_new_gen(myvgd, kind, version, self.levels, len(self.levels), rcoef,None,ptop,pref,None,0,0,None,None)
        if status:
            sys.stderr.write("Hybrid2Pressure - There was a problem creating the VGridDescriptor\n")
        self.myvgd = myvgd   


    def get_hybrid_coord_info(self):
        self.ptop = self.hy_data[0]
        self.pref = self.hy_ig1
        self.rcoef = self.hy_ig2/1000.0

    def std_atm_pressure(self,lvl):
        self.get_ptop_pref_rcoef()
        pres = self.get_pres_hyb_to_pres(lvl, self.kind, self.ptop, self.pref, self.rcoef)
        return pres

    def vgrid_pressure(self,ip1):
        ip = ctypes.c_int(ip1)
        pres = np.empty((self.p0_data.shape[0],self.p0_data.shape[1],1),dtype='float32', order='F')
        status = vgdp.c_vgd_levels(self.myvgd, self.p0_data.shape[0], self.p0_data.shape[1], 1, ip, pres, self.p0_data, 0)
        if status:
            sys.stderr.write("Hybrid2Pressure - There was a problem creating the pressure\n")
            pres = None
        else:
            pres = np.squeeze(pres)    
            # pres = do_unit_conversion_array(pres,from_unit_name='pascal',to_unit_name='hectoPascal')
        return pres    

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
        self.pref = self.ig1
        self.rcoef = self.ig2 / 1000.0


    def get_pres_hyb_to_pres(levels,kind:int,ptop:float,pref:float,rcoef:float):
        """hybrid to pressure conversion function

        :param kind: current level kind
        :type kind: int
        :param level: current level
        :type level: float
        :param ptop: pressure at top
        :type ptop: float
        :param pref: reference pressure
        :type pref: float
        :param rcoef: reference coefficient
        :type rcoef: float
        :param p0: surface pressure
        :type p0: float
        :return: pressure for current level
        :rtype: float
        """
        def std_atm_pressure1(level:float,kind:int,ptop:float,pref:float,rcoef:float) -> float:
            term0 =  (ptop / pref)
            term1 = (level + (1.0 - level) * term0)
            term2 = (term1 - term0) 
            term3 = (1.0 / (1.0 - term0))
            term4 = (level - term0)
            evalTerm0 = (0.0 if term2 < 0 else term2 )
            evalTerm1 = (0.0 if term4 < 0 else term4 )
            term5 = (math.pow( evalTerm0 * term3, rcoef))
            term6 = (math.pow( evalTerm1 * term3, rcoef))

            if kind == 1:
                return ( pref * ( term1 - term5 )) + term5 * STANDARD_ATMOSPHERE
            elif kind == 5:  
                return ( pref * ( level - term6 )) + term6 * STANDARD_ATMOSPHERE
            else:
                return -1.
        vhyb_to_pres = np.vectorize(std_atm_pressure1)
        pres_values = vhyb_to_pres(levels,kind,ptop,pref,rcoef)
        return pres_values


###################################################################################        
def compute_pressure_from_hyb_coord_array(hy_data,hy_ig1,hy_ig2,p0_data,levels,kind,standard_atmosphere) -> list:
    p = Hybrid2Pressure(hy_data,hy_ig1,hy_ig2,p0_data,levels,kind,standard_atmosphere)
    vip1_all = np.vectorize(rmn.ip1_all)
    ips = vip1_all(levels,kind)

    pressures=[]
    if standard_atmosphere:
        for lvl,ip in zip(levels,ips):
            pres = p.std_atm_pressure(lvl)
            mydict = {}
            mydict[ip]=pres
            pressures.append(mydict)    
    else:
        for ip in ips:
            pres = p.vgrid_pressure(ip)
            mydict = {}
            mydict[ip]=pres
            pressures.append(mydict)     
    return pressures

def compute_pressure_from_hyb_coord_df(df:pd.DataFrame,hy_data,hy_ig1,hy_ig2,p0_data,standard_atmosphere) -> pd.DataFrame:
    if df.empty:
        return None
    df = df.drop_duplicates('ip1')
    levels = df.level.unique()
    kind = df.iloc[0]['ip1_kind']
    p = Hybrid2Pressure(hy_data,hy_ig1,hy_ig2,p0_data,levels,kind,standard_atmosphere)
    press = []    
    for i in df.index:
        ip = df.at[i,'ip1']
        lvl = df.at[i,'level']
        px_s = df.loc[i, df.columns != 'd'].copy(deep=True)
        px_s['nomvar'] = "PX"
        px_s['unit'] = "pascal"
        px_s['description'] = "Pressure of the Model"
        if standard_atmosphere:
            pres = p.std_atm_pressure(lvl)
        else:
            pres = p.vgrid_pressure(ip)
        px_s['d'] = pres
        press.append(px_s)
    pressure_df = pd.DataFrame(press)
    # pressure_df = do_unit_conversion(pressure_df,to_unit_name='hectoPascal')
    return pressure_df
###################################################################################
###################################################################################
class HybridStaggered2Pressure:
    def __init__(self,bb_data,p0_data,standard_atmosphere) -> None:
        self.bb_data = bb_data
        self.p0_data = p0_data
        self.standard_atmosphere = standard_atmosphere
        if not self.standard_atmosphere:
            self.create_vgrid_descriptor()
            # print(vgd.vgd_print_desc(self.myvgd, convip=-1))

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
        def hybstag_to_pres(a:float, b:float, pref:float) -> float:
            return ( math.exp( a + b * math.log(STANDARD_ATMOSPHERE*100.0/ pref))/100.0)   

        vhybstag_to_pres = np.vectorize(hybstag_to_pres)
        pres_values = vhybstag_to_pres(a, b, pref)
        return pres_values

    def std_atm_pressure(self,ip1):
        ips = self.bb_data[0][3:].astype(int)
        ipindex = np.where(ips==ip1)
        a_8 = self.bb_data[1][3:]
        b_8 = self.bb_data[2][3:]
        pref = self.bb_data[1][1]
        pres = self.get_std_atm_pressure(a_8[ipindex], b_8[ipindex], pref)
        return pres

    def vgrid_pressure(self,ip1):
        ip = ctypes.c_int(ip1)
        pres = np.empty((self.p0_data.shape[0],self.p0_data.shape[1],1),dtype='float32', order='F')
        status = vgdp.c_vgd_levels(self.myvgd, self.p0_data.shape[0], self.p0_data.shape[1], 1, ip, pres, self.p0_data, 0)
        if status:
            sys.stderr.write("HybridStaggered2Pressure - There was a problem creating the pressure\n")
            pres = None
        else:
            pres = np.squeeze(pres)    
            # pres = do_unit_conversion_array(pres,from_unit_name='pascal',to_unit_name='hectoPascal')
 
        return pres

    def get_pressure(self):
        if(self.standard_atmosphere):
            press_func = self.std_atm_pressure
        else:
            press_func = self.vgrid_pressure
        return press_func
###################################################################################
def compute_pressure_from_hybstag_coord_array(ip1s,bb_data,p0_data,standard_atmosphere) -> np.ndarray:
    p = HybridStaggered2Pressure(bb_data,p0_data,standard_atmosphere)
    pressures=[]
    for ip in ip1s:
        mydict = {}
        mydict[ip] = p.get_pressure()(ip)
        pressures.append(mydict)    
    return pressures

def compute_pressure_from_hybstag_coord_df(df:pd.DataFrame,bb_data,p0_data,standard_atmosphere) -> pd.DataFrame:
    if df.empty:
        return None
    df = df.drop_duplicates('ip1')
    p = HybridStaggered2Pressure(bb_data,p0_data,standard_atmosphere)
    press = []    
    for i in df.index:
        ip = df.at[i,'ip1']
        if ip == 0:
            continue
        px_s = df.loc[i, df.columns != 'd'].copy(deep=True)
        px_s['nomvar'] = "PX"
        px_s['unit'] = "pascal"
        px_s['description'] = "Pressure of the Model"
        pres = p.get_pressure()(ip)
        px_s['d'] = pres
        press.append(px_s)
    pressure_df = pd.DataFrame(press)
    # pressure_df = do_unit_conversion(pressure_df,to_unit_name='hectoPascal')
    return pressure_df
###################################################################################
###################################################################################


 