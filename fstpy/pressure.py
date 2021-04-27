# -*- coding: utf-8 -*-
from fstpy.unit import do_unit_conversion, do_unit_conversion_array
from .utils import initializer, validate_df_not_empty,get_groups
from .dataframe_utils import select,zap
from .std_reader import load_data
import pandas as pd
import numpy as np
import math
import rpnpy.vgd.all as vgd
import sys
import rpnpy.librmn.all as rmn
import rpnpy.vgd.proto as vgdp
import ctypes

STANDARD_ATMOSPHERE = 1013.25

class PressureError(Exception):
    pass

class Pressure:
    plugin_requires = {'(nomvar in ["TT","TD"]) and (unit=="celsius")' }
    plugin_result = {'nomvar':'ES','etiket':'Pressure','unit':'celsius'}
    @initializer
    def __init__(self,df:pd.DataFrame, standard_atmosphere=False):
        validate_df_not_empty(df,Pressure,PressureError) 
        self.df = select(self.df,self.plugin_requires)
        self.df = load_data(self.df)
        self.groups= get_groups(df)

    def compute(self) -> pd.DataFrame:
        pxdfs=[]
        for _, group in self.groups:
            levels = group.levels.unique()
            ttdf = select(group, 'nomvar=="TT"')
            tddf = select(group, 'nomvar=="TD"')
            esdf = ttdf.copy(deep=True)
            esdf = zap(esdf,**self.plugin_result)
            #ES = TT - TD  (if ES < 0.0 , ES = 0.0)
            esdf['d'] = ttdf['d'] - tddf['d']
            esdf['d'] = np.where(esdf['d'] < 0.0, 0.0 )
            pxdfs.append(esdf)
        res = pd.concat(pxdfs)
        return res

    def algo(self,df,meta_df,vctype):
        if vctype == "UNKNOWN":
            px_df = None

        elif vctype == "HYBRID":
            p0_df = meta_df.query('nomvar=="P0"')
            p0_data = p0_df.iloc[0]['d']
            hy_df = meta_df.query('nomvar=="HY"')
            hy_data = hy_df.iloc[0]['d']
            hy_ig1 = hy_df.iloc[0]['ig1']
            hy_ig2 = hy_df.iloc[0]['ig2']
            px_df = hyb_pressure_df(df,hy_data,hy_ig1,hy_ig2,p0_data,self.standard_atmosphere)

        elif (vctype == "HYBRID_5005") or( vctype == "HYBRID_STAGGERED"):
            p0_df = meta_df.query('nomvar=="P0"')
            p0_data = p0_df.iloc[0]['d']
            bb_df = meta_df.query('nomvar=="!!"')
            bb_data = bb_df.iloc[0]['d']
            px_df = hybstag_pressure_df(df,bb_data,p0_data,self.standard_atmosphere)

        elif vctype == "PRESSURE":
            p0_df = meta_df.query('nomvar=="P0"')
            p0_data = p0_df.iloc[0]['d']
            px_df = pressure_pressure_array_pressure_df(df,p0_data,self.standard_atmosphere)

        elif vctype == "ETA":
            p0_df = meta_df.query('nomvar=="P0"')
            p0_data = p0_df.iloc[0]['d']
            pt_df = meta_df.query('nomvar=="PT"')
            pt_data = pt_df.iloc[0]['d']
            bb_df = meta_df.query('nomvar=="!!"')
            bb_data = bb_df.iloc[0]['d']
            px_df = eta_pressure_df(df,pt_data,bb_data,p0_data,self.standard_atmosphere)

        elif vctype == "SIGMA":
            p0_df = meta_df.query('nomvar=="P0"')
            p0_data = p0_df.iloc[0]['d']
            px_df = sigma_pressure_df(df,p0_data,self.standard_atmosphere)

        return px_df

class Pressure2Pressure:
    @initializer
    def __init__(self,levels,p0_data,standard_atmosphere) -> None:
        if not self.standard_atmosphere:
            self.create_vgrid_descriptor()

    def create_vgrid_descriptor(self):    
        try:
		    # myvgd = vgd.vgd_new_gen_2001(&myvgd, vLevels, numVLevels, 0, 0)
            self.myvgd = vgd.vgd_new_pres(self.levels)
        except vgd.VGDError:
            sys.stderr.write("There was a problem creating the VGridDescriptor")

    def std_atm_pressure(self,level):
        pres = np.full(self.p0_data.shape,level,dtype=np.float32,order='F')
        pres = self.get_pres_eta_to_pres(level)
        return pres

    def vgrid_pressure(self,ip1):
        p0_data = do_unit_conversion_array(self.p0_data,from_unit_name='millibar',to_unit_name='pascal')
        pres = vgd.vgd_levels(self.myvgd, rfld=p0_data,ip1list=ip1)
        return pres          
###################################################################################            
def pressure_pressure_array(levels,kind,p0_data,standard_atmosphere) -> list:
    p = Pressure2Pressure(levels,p0_data,standard_atmosphere)
    vip1_all = np.vectorize(rmn.ip1_all)
    ips = vip1_all(levels,kind)

    pressures=[]
    if standard_atmosphere:
        for lvl,ip in zip(levels,ips):
            pres = p.std_atm_pressure(lvl,p0_data)
            mydict = {}
            mydict[ip]=pres
            pressures.append(mydict)    
    else:
        for ip in ips:
            pres = p.vgrid_pressure(ip,p0_data)
            mydict = {}
            mydict[ip]=pres
            pressures.append(mydict)    
    return pressures

def pressure_pressure_array_pressure_df(df:pd.DataFrame,p0_data,standard_atmosphere) -> pd.DataFrame:
    df = df.drop_duplicates('ip1')
    levels = df.level.unique()
    p = Pressure2Pressure(levels,standard_atmosphere)
    press = []    
    for i in df.index:
        ip = df.at[i,'ip1']
        lvl = df.at[i,'level']
        px_s = df.iloc[i, df.columns != 'd'].copy(deep=True)
        px_s['nomvar'] = "PX"
        px_s['unit'] = "pascal"
        px_s['description'] = "Pressure of the Model"
        if standard_atmosphere:
            pres = p.std_atm_pressure(lvl)
        else:
            pres = p.vgrid_pressure(ip,p0_data)
        px_s['d'] = pres
        press.append(px_s)
    pressure_df = pd.DataFrame(press)
    pressure_df = do_unit_conversion(pressure_df,to_unit_name='hectoPascal')
    return pressure_df 
            
###################################################################################
###################################################################################
class Sigma2Pressure:
    @initializer
    def __init__(self,levels,standard_atmosphere) -> None:
        if not self.standard_atmosphere:
            self.create_vgrid_descriptor()

    def create_vgrid_descriptor(self):
        try:
	        # myvgd = vgd.vgd_new_gen_1001(&myvgd, vLevels, numVLevels, 0, 0)
            self.myvgd = vgd.vgd_new_sigm(self.levels)
        except vgd.VGDError:
            sys.stderr.write("There was a problem creating the VGridDescriptor")


    def std_atm_pressure(self,level):
        pres = self.get_pres_eta_to_pres(level)
        return pres

    def vgrid_pressure(self,ip1,p0_data):
        p0_data = do_unit_conversion_array(p0_data,from_unit_name='millibar',to_unit_name='pascal')
        pres = vgd.vgd_levels(self.myvgd, rfld=p0_data,ip1list=ip1)
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
def sigma_pressure_array(levels,kind,p0_data,standard_atmosphere) -> list:
    p = Sigma2Pressure(levels,standard_atmosphere)
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
            pres = p.vgrid_pressure(ip,p0_data)
            mydict = {}
            mydict[ip]=pres
            pressures.append(mydict)    
    return pressures

def sigma_pressure_df(df:pd.DataFrame,p0_data,standard_atmosphere) -> pd.DataFrame:
    df = df.drop_duplicates('ip1')
    levels = df.level.unique()
    p = Sigma2Pressure(levels,standard_atmosphere)
    press = []    
    for i in df.index:
        ip = df.at[i,'ip1']
        lvl = df.at[i,'level']
        px_s = df.iloc[i, df.columns != 'd'].copy(deep=True)
        px_s['nomvar'] = "PX"
        px_s['unit'] = "pascal"
        px_s['description'] = "Pressure of the Model"
        if standard_atmosphere:
            pres = p.std_atm_pressure(lvl)
        else:
            pres = p.vgrid_pressure(ip,p0_data)
        px_s['d'] = pres
        press.append(px_s)
    pressure_df = pd.DataFrame(press)
    pressure_df = do_unit_conversion(pressure_df,to_unit_name='hectoPascal')
    return pressure_df               
###################################################################################
###################################################################################
class Eta2Pressure:
    @initializer
    def __init__(self,levels,pt_data,bb_data,p0_data,standard_atmosphere) -> None:
        if not self.standard_atmosphere:
            self.create_vgrid_descriptor()

    def create_vgrid_descriptor(self):
        self.ptop = self.get_ptop()
        try:
	        # myvgd = vgd.vgd_new_gen_1002(&myvgd, vLevels, numVLevels, ptopavg*100.0, 0, 0)
            self.myvgd = vgd.vgd_new_eta(self.levels, self.ptop*100.0)
        except vgd.VGDError:
            sys.stderr.write("There was a problem creating the VGridDescriptor")
        
    def get_ptop(self):
        if  len(self.pt_data):
            self.ptop = self.pt_data[0]

        elif len(self.bb_data):
            # get value from !!
            self.ptop = self.bb_data[0]/100.0
            # ptop = ((float)(*BBfm)(0,1,0))/100.0
        else:
            self.ptop = -999.0

    def std_atm_pressure(self,level):
        pres = self.get_pres_eta_to_pres(level)
        return pres

    def vgrid_pressure(self,ip1):
        self.p0_data = do_unit_conversion_array(self.p0_data,from_unit_name='millibar',to_unit_name='pascal')
        pres = vgd.vgd_levels(self.myvgd, rfld=self.p0_data,ip1list=ip1)
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
def eta_pressure_array(levels,kind,pt_data,bb_data,p0_data,standard_atmosphere) -> list:
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

def eta_pressure_df(df:pd.DataFrame,pt_data,bb_data,p0_data,standard_atmosphere) -> pd.DataFrame:
    df = df.drop_duplicates('ip1')
    levels = df.level.unique()
    p = Eta2Pressure(levels,pt_data,bb_data,p0_data,standard_atmosphere)
    press = []    
    for i in df.index:
        ip = df.at[i,'ip1']
        lvl = df.at[i,'level']
        px_s = df.iloc[i, df.columns != 'd'].copy(deep=True)
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
    pressure_df = do_unit_conversion(pressure_df,to_unit_name='hectoPascal')
    return pressure_df            
###################################################################################
###################################################################################
class Hybrid2Pressure:
    @initializer
    def __init__(self,hy_data,hy_ig1,hy_ig2,p0_data,levels,kind,standard_atmosphere) -> None:
        if not self.standard_atmosphere:
            self.create_vgrid_descriptor()

    def create_vgrid_descriptor(self):
        self.levels.append(1.0)
        self.get_hybrid_coord_info()
        try:
            # myvgd = vgd.vgd_new_gen_5001(&myvgd, &tmpVLevels[0], (int)tmpVLevels.size(), ptopavg*100.0, pref*100.0, rcoef, 0,0)
            self.myvgd = vgd.vgd_new_hyb(self.levels, self.rcoef, self.ptop*100.0, self.pref*100.0)
        except vgd.VGDError:
            sys.stderr.write("There was a problem creating the VGridDescriptor")

    def get_hybrid_coord_info(self):
        if self.hy_data is None:
            self.ptop = -999.0
            self.pref = -999.0
            self.rcoef = -999.0
        else:
            self.ptop = self.hy_data[0]
            self.pref = self.ig1
            self.rcoef = self.ig2/1000.0

    def std_atm_pressure(self,lvl):
        self.get_ptop_pref_rcoef()
        pres = self.get_pres_hyb_to_pres(lvl, self.kind, self.ptop, self.pref, self.rcoef)
        return pres

    def vgrid_pressure(self,ip1):
        pres = vgd.vgd_levels(self.myvgd, rfld=self.p0_data,ip1list=ip1)
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
        if len(self.hy_data) == 0:
            self.ptop = -1.
            self.pref = -1.
            self.rcoef = -1.

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
def hyb_pressure_array(hy_data,hy_ig1,hy_ig2,p0_data,levels,kind,standard_atmosphere) -> list:
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

def hyb_pressure_df(df:pd.DataFrame,hy_data,hy_ig1,hy_ig2,p0_data,standard_atmosphere) -> pd.DataFrame:
    df = df.drop_duplicates('ip1')
    levels = df.level.unique()
    kind = df.iloc[0]['ip1_kind']
    p = Hybrid2Pressure(hy_data,hy_ig1,hy_ig2,p0_data,levels,kind,standard_atmosphere)
    press = []    
    for i in df.index:
        ip = df.at[i,'ip1']
        lvl = df.at[i,'level']
        px_s = df.iloc[i, df.columns != 'd'].copy(deep=True)
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
    pressure_df = do_unit_conversion(pressure_df,to_unit_name='hectoPascal')
    return pressure_df
###################################################################################
###################################################################################
class HybridStaggered2Pressure:
    @initializer
    def __init__(self,bb_data,p0_data,standard_atmosphere) -> None:
        if not self.standard_atmosphere:
            self.create_vgrid_descriptor()

    def __del__(self):
        if not self.standard_atmosphere:
            vgdp.c_vgd_free(self.myvgd)

    def create_vgrid_descriptor(self):
        myvgd = vgdp.c_vgd_construct()
        self.bb_data = self.bb_data.ctypes.data_as(ctypes.POINTER(ctypes.c_double))
        status = vgdp.c_vgd_new_from_table(myvgd,self.bb_data,self.bb_data.shape[0],self.bb_data.shape[1],1)
        if not status:
            sys.stderr.write("There was a problem creating the VGridDescriptor")
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
        pres = np.empty((self.p0_ni,self.p0_nj,1),dtype='float32', order='F')
        status = vgdp.c_vgd_levels(self.myvgd, self.p0_data.shape[0], self.p0_data.shape[1], 1, ip, pres, self.p0_data, 0)
        if not status:
            sys.stderr.write("There was a problem creating the VGridDescriptor")
            pres = None
        pres = np.squeeze(pres)    
        return pres

    def get_pressure(self):
        if(self.standard_atmosphere):
            press_func = self.std_atm_pressure
        else:
            press_func = self.vgrid_pressure
        return press_func
###################################################################################
def hybstag_pressure_array(ip1s,bb_data,p0_data,standard_atmosphere) -> np.ndarray:
    p = HybridStaggered2Pressure(bb_data,p0_data,standard_atmosphere)
    pressures=[]
    for ip in ip1s:
        pres = p.get_pressure()(ip)
        mydict = {}
        mydict[ip]=pres
        pressures.append(mydict)    
    return pressures

def hybstag_pressure_df(df:pd.DataFrame,bb_data,p0_data,standard_atmosphere) -> pd.DataFrame:
    df = df.drop_duplicates('ip1')
    p = HybridStaggered2Pressure(bb_data,p0_data,standard_atmosphere)
    press = []    
    for i in df.index:
        ip = df.at[i,'ip1']
        px_s = df.iloc[i, df.columns != 'd'].copy(deep=True)
        px_s['nomvar'] = "PX"
        px_s['unit'] = "pascal"
        px_s['description'] = "Pressure of the Model"
        pres = p.get_pressure()(ip)
        px_s['d'] = pres
        press.append(px_s)
    pressure_df = pd.DataFrame(press)
    pressure_df = do_unit_conversion(pressure_df,to_unit_name='hectoPascal')
    return pressure_df
###################################################################################
###################################################################################


 