# -*- coding: utf-8 -*-
from fstpy.plugin import Plugin
import numpy as np
import pandas as pd
import rpnpy.librmn.all as rmn
import numpy as np

from .std_reader import load_data
from .utils import initializer

class InterpolationHorizontalGridError(Exception):
    pass

class InterpolationHorizontalGrid(Plugin):
    methods = ['field','user']
    grid_types = ['A','B','G','L','N','S']
    extrapolation_types = ['nearest','linear','maximum','minimum','value','abort']
    interpolation_types = ['nearest','bi-linear','bi-cubic']

    @initializer
    def __init__(self,df:pd.DataFrame, method:str, interpolation_type:str, extrapolation_type:str, grtyp:str=None, ni:int=None, nj:int=None, param1:float=None, param2:float=None, param3:float=None, param4:float=None, extrapolation_value:float=None, nomvar:str=None):
        self.validate_input()

    def validate_input(self):
        if self.df.empty:
            raise InterpolationHorizontalGridError('InterpolationHorizontalGrid - no data to process')
        self.validate_params()

        self.set_options()
        self.define_output_grid()

        self.groups =  self.df.groupby(by=['grid'])


        # print(self.output_grid)

    def define_output_grid(self):
        # print('defining output grid')
        if self.method == 'user':
            if self.grtyp in ['L','N','S']:
                self.ig1,self.ig2,self.ig3,self.ig4 = rmn.cxgaig(self.grtyp, self.param1,self.param2,self.param3,self.param4)
                
                self.output_grid = define_grid(self.grtyp,'',self.ni,self.nj,self.ig1,self.ig2,self.ig3,self.ig4,None,None,None)
                # print(self.output_grid)
            else:
                self.output_grid = define_grid(self.grtyp,'',self.ni,self.nj,int(self.param1),int(self.param2),int(self.param3),int(self.param4),None,None,None)
        else:
            if self.nomvar is None:
                raise InterpolationHorizontalGridError('InterpolationHorizontalGrid - you must supply a nomvar with field defined method')    
            field_df = self.df.query(f'nomvar=="{self.nomvar}"')

            if len(field_df.grid.unique()) > 1: 
                raise InterpolationHorizontalGridError('InterpolationHorizontalGrid - reference field found for multiple grids')    
            # print(field_df[['nomvar','typvar','etiket','ni','nj','nk','ig1','ig2','ig3','ig4','grtyp']])
            grid = field_df.iloc[0]['grid']
            self.grtyp = field_df.iloc[0]['grtyp']
            self.ni = field_df.iloc[0]['ni']
            self.nj = field_df.iloc[0]['nj']
            self.ig1 = field_df.iloc[0]['ig1']
            self.ig2 = field_df.iloc[0]['ig2']
            self.ig3 = field_df.iloc[0]['ig3']
            self.ig4 = field_df.iloc[0]['ig4']
            meta_df = self.df.query(f"(nomvar in ['>>','^^','^>']) and (grid=='{grid}')").reset_index(drop=True)
            
            # others_df = self.df.query("nomvar not in ['>>','^^','^>','!!','HY','UU','VV','PT']").reset_index(drop=True)

            if ('>>' in meta_df.nomvar.to_list()) and  ('^^' in meta_df.nomvar.to_list()):
                lat_df = meta_df.query('nomvar==">>"')
                lat_df = load_data(lat_df)
                lon_df = meta_df.query('nomvar=="^^"')
                lon_df = load_data(lon_df)
                self.ni = lat_df.iloc[0]['ni']
                self.nj = lon_df.iloc[0]['nj']
                grref = lat_df.iloc[0]['grtyp']
                ax = lat_df.iloc[0]['d']
                ay = lon_df.iloc[0]['d']
                self.ig1 = lat_df.iloc[0]['ig1']
                self.ig2 = lat_df.iloc[0]['ig2']
                self.ig3 = lat_df.iloc[0]['ig3']
                self.ig4 = lat_df.iloc[0]['ig4']
                self.output_grid = define_grid(self.grtyp,grref,self.ni,self.nj,self.ig1,self.ig2,self.ig3,self.ig4,ax,ay,None)
                self.ig1 = lat_df.iloc[0]['ip1']
                self.ig2 = lat_df.iloc[0]['ip2']
                self.ig3 = 0
                self.ig4 = 0

            elif ('^>' in meta_df.nomvar.to_list()):
                tictac_df = meta_df.query('nomvar=="^>"')
                tictac_df = load_data(tictac_df)
                self.output_grid = define_grid(self.grtyp,'',0,0,0,0,0,0,None,None,tictac_df.iloc[0]['d'])    
                self.ig1 = tictac_df.iloc[0]['ip1']
                self.ig2 = tictac_df.iloc[0]['ip2']
                self.ig3 = 0
                self.ig4 = 0

            else:
                self.output_grid = define_grid(self.grtyp,'',self.ni,self.nj,self.ig1,self.ig2,self.ig3,self.ig4,None,None,None) 
 

    def define_input_grid(self,grtyp,source_df,meta_df):
        ni = source_df.iloc[0]['ni']
        nj = source_df.iloc[0]['nj']
        ig1 = source_df.iloc[0]['ig1']
        ig2 = source_df.iloc[0]['ig2']
        ig3 = source_df.iloc[0]['ig3']
        ig4 = source_df.iloc[0]['ig4']
        # print('define_input_grid\n',meta_df)
        if ('>>' in meta_df.nomvar.to_list()) and  ('^^' in meta_df.nomvar.to_list()):
            # print('found ^^ and >>')
            lat_df = meta_df.query('nomvar==">>"')
            lon_df = meta_df.query('nomvar=="^^"')
            ni = lat_df.iloc[0]['ni']
            nj = lon_df.iloc[0]['nj']
            grref = lat_df.iloc[0]['grtyp']
            ax = lat_df.iloc[0]['d']
            ay = lon_df.iloc[0]['d']
            ig1 = lat_df.iloc[0]['ig1']
            ig2 = lat_df.iloc[0]['ig2']
            ig3 = lat_df.iloc[0]['ig3']
            ig4 = lat_df.iloc[0]['ig4']
            # print(grtyp,ig1,ig2,ig3,ig3)
            input_grid = define_grid(grtyp,grref,ni,nj,ig1,ig2,ig3,ig4,ax,ay,None)
            
        elif ('^>' in meta_df.nomvar.to_list()):
            tictac_df = meta_df.query('nomvar=="^>"')
            input_grid = define_grid(grtyp,'',0,0,0,0,0,0,None,None,tictac_df.iloc[0]['d'])    

        else:
            input_grid = define_grid(grtyp,' ',ni,nj,ig1,ig2,ig3,ig4,None,None,None) 

        return input_grid        

    def validate_params(self):
        if self.interpolation_type not in self.interpolation_types:
            raise InterpolationHorizontalGridError(f'InterpolationHorizontalGrid - interpolation_type {self.interpolation_type} not in {self.interpolation_types}')
        if self.extrapolation_type not in self.extrapolation_types:
            raise InterpolationHorizontalGridError(f'InterpolationHorizontalGrid - extrapolation_type {self.extrapolation_type} not in {self.extrapolation_types}')
        if self.method not in self.methods:
            raise InterpolationHorizontalGridError(f'InterpolationHorizontalGrid - method {self.method} not in {self.methods}')
        if self.method == 'user':    
            if self.grtyp not in self.grid_types:
                raise InterpolationHorizontalGridError(f'InterpolationHorizontalGrid - grtyp {self.grtyp} not in {self.grid_types}')    

    def set_options(self):
        self.set_interpolation_type_options()
        self.set_extrapolation_type_options()

    def set_extrapolation_type_options(self):
        if self.extrapolation_type == 'value':
            if self.extrapolation_value is None:
                raise InterpolationHorizontalGridError(f'InterpolationHorizontalGrid - extrapolation_value {self.extrapolation_value} is not set')
            rmn.ezsetval('EXTRAP_VALUE', self.extrapolation_value)
            rmn.ezsetopt('EXTRAP_DEGREE', 'VALUE')
        else:
            # print( self.extrapolation_type.upper())
            rmn.ezsetopt('EXTRAP_DEGREE', self.extrapolation_type.upper())

    def set_interpolation_type_options(self):
        if self.interpolation_type == 'nearest':
            rmn.ezsetopt('INTERP_DEGREE', 'NEAREST')
        elif self.interpolation_type == 'bi-linear':
            rmn.ezsetopt('INTERP_DEGREE', 'LINEAR')    
        elif self.interpolation_type == 'bi-cubic':
            rmn.ezsetopt('INTERP_DEGREE', 'CUBIC')

    def create_grid_set(self,input_grid):
        _ = rmn.ezdefset(self.output_grid, input_grid)


    def compute(self) -> pd.DataFrame:
        results = []
        no_mod = []
        for _,current_group in self.groups:
            # print(current_group[['nomvar','forecast_hour']])
            # print(grid)
            # print(current_group[['nomvar','typvar','etiket','ni','nj','nk','ig1','ig2','ig3','ig4','date_of_validity']])
            current_group = load_data(current_group)
            meta_df = current_group.query("nomvar in ['>>','^^','^>']").reset_index(drop=True)
            # print(meta_df[['nomvar','typvar','etiket','ni','nj','nk','ig1','ig2','ig3','ig4','date_of_validity']])
            vect_df = current_group.query("nomvar in ['UU','VV']").reset_index(drop=True)
            uu_df = vect_df.query('nomvar=="UU"').reset_index(drop=True)
            vv_df = vect_df.query('nomvar=="VV"').reset_index(drop=True)
            # others_df = current_group.query("nomvar not in ['>>','^^','^>','!!','HY','UU','VV','PT']").reset_index(drop=True)
            others_df = current_group.query("nomvar not in ['UU','VV','PT','>>','^^','^>','!!','HY']").reset_index(drop=True)
            pt_df = current_group.query("nomvar=='PT'").reset_index(drop=True)

            # print(meta_df.iloc[0]['grid'] if not meta_df.empty else 0,vect_df.iloc[0]['grid'] if not vect_df.empty else 0,uu_df.iloc[0]['grid'] if not uu_df.empty else 0,vv_df.iloc[0]['grid'] if not vv_df.empty else 0,others_df.iloc[0]['grid'] if not others_df.empty else 0,pt_df.iloc[0]['grid'] if not pt_df.empty else 0)
            if not vect_df.empty:
                # print('selected UU and VV')
                grtyp = vect_df.iloc[0]['grtyp']
                source_df = vect_df
            elif not others_df.empty:
                # print('selected others')
                grtyp = others_df.iloc[0]['grtyp']
                source_df = others_df
            elif not pt_df.empty:    
                # print('selected PT')
                grtyp = pt_df.iloc[0]['grtyp']
                source_df = pt_df
            else:
                continue
            # print('-----------------------------------------------------------\ncurrent_group')    
            # print(f'input grid {grtyp}')
            # print('meta_df\n',meta_df[['nomvar','etiket','ni','nj','nk','grtyp','ig1','ig2','ig3','ig4','grid']])
            # print('vect_df\n',vect_df[['nomvar','etiket','ni','nj','nk','grtyp','ig1','ig2','ig3','ig4','grid']])
            # print('others\n',others_df[['nomvar','etiket','ni','nj','nk','grtyp','ig1','ig2','ig3','ig4','grid']])
            # print('pt_df\n',pt_df[['nomvar','etiket','ni','nj','nk','grtyp','ig1','ig2','ig3','ig4','grid']])
            # print(source_df[['nomvar','typvar','etiket','ni','nj','nk','ig1','ig2','ig3','ig4','date_of_validity']])
            input_grid = self.define_input_grid(grtyp,source_df,meta_df)

            in_params = rmn.ezgxprm(input_grid)
            out_params = rmn.ezgxprm(self.output_grid)
            if in_params == out_params:
                # print('APPENDING\n',current_group)
                no_mod.append(current_group)
                continue

            self.create_grid_set(input_grid)

            if (not uu_df.empty) and (not vv_df.empty):
                uu_int_df = uu_df.copy(deep=True)
                vv_int_df = vv_df.copy(deep=True)

                for i in uu_df.index:
                    (uu, vv) = rmn.ezuvint(self.output_grid, input_grid, uu_df.at[i,'d'], vv_df.at[i,'d'])
                    # print(vv_df.at[i,'d'].dtype)
                    # print(vv.dtype)
                    # print(uu_df.at[i,'d'].dtype)
                    # print(uu.dtype)
                    uu_int_df.at[i,'d'] = uu
                    vv_int_df.at[i,'d'] = vv
                results.append(uu_int_df)    
                results.append(vv_int_df)    

            if not others_df.empty:
                # scalar except PT
                others_int_df = others_df.copy(deep=True)
                for i in others_df.index:
                    arr = rmn.ezsint(self.output_grid, input_grid, others_df.at[i,'d'])
                    others_int_df.at[i,'d'] = arr
                results.append(others_int_df)        

            if not pt_df.empty:        
                # PT
                pt_int_df = pt_df.copy(deep=True)    
                # always for PT
                rmn.ezsetopt('EXTRAP_DEGREE', 'NEAREST')
                for i in pt_df.index:
                    arr = rmn.ezsint(self.output_grid, input_grid, pt_df.at[i,'d'])
                    pt_int_df.at[i,'d'] = arr
                results.append(pt_int_df)            
                # reset extrapolation options        
                self.set_extrapolation_type_options()

            # rmn.gdrls(input_grid)
            # rmn.gdrls(self.output_grid)
        res_df = pd.concat(results,ignore_index=True)   

        no_mod_df = pd.DataFrame(dtype=object)
        # print('LEN NO_MOD\n',len(no_mod))
        # print('NO_MOD\n',no_mod)
        if len(no_mod): 
            no_mod_df = pd.concat(no_mod,ignore_index=True)
        #     print(no_mod_df)
        # elif len(no_mod) == 1:
        #     no_mod_df = no_mod[0]
        #     print(no_mod_df)

        # myshape =  tuple((self.ni,self.nj))
        # res_df.loc['shape'] = None
        # res_df.loc['shape'] = myshape
        # result_specifications = {'ni':self.ni,'nj':self.nj,'ig1':self.ig1,'ig2':self.ig2,'ig3':self.ig3,'ig4':self.ig4,'interpolated':True}
        # for k,v in result_specifications.items():res_df[k]=v
        res_df.drop(columns=['shape'],inplace=True)
        res_df["shape"] = [(self.ni,self.nj)] * len(res_df)
        res_df['ni'] = self.ni
        res_df['nj'] = self.nj
        res_df['grtyp'] = self.grtyp
        # res_df['path'] = ''
        res_df['interpolated'] = True
        res_df['ig1'] = self.ig1
        res_df['ig2'] = self.ig2
        res_df['ig3'] = self.ig3
        res_df['ig4'] = self.ig4
        
        
        if not no_mod_df.empty:
            res_df = pd.concat([res_df,no_mod_df],ignore_index=True)
            # print('RES_DF',res_df)
        # else:
        #     print('RES_DF EMPTY')

        return res_df





###################################################################################  
###################################################################################  


def define_grid(grtyp:str,grref:str,ni:int,nj:int,ig1:int,ig2:int,ig3:int,ig4:int,ax:np.ndarray,ay:np.ndarray,tictac:np.ndarray) -> int:
    #longitude = X
    grid_types = ['A','B','E','G','L','N','S','U','X','Y','Z','#']
    grid_id = -1
    
    if grtyp not in grid_types:
        raise InterpolationHorizontalGridError(f'InterpolationHorizontalGrid - grtyp {grtyp} not in {grid_types}')


    if  grtyp in ['Y','Z','#']:
        
        grid_params = {'grtyp':grtyp,'grref':grref,'ni':int(ni),'nj':int(nj),'ay':ay,'ax':ax,'ig1':int(ig1),'ig2':int(ig2),'ig3':int(ig3),'ig4':int(ig4)}
        # print(f"{grid_params['ni']}\t{grid_params['nj']}\t{grid_params['grtyp']}\t{grid_params['grref']}\t{grid_params['ig1']}\t{grid_params['ig2']}\t{grid_params['ig3']}\t{grid_params['ig4']}\t")
        # grid_params = {'grtyp':grtyp,'grref':grref,'ni':int(ni),'nj':int(nj),'ay':ay,'ax':ax}
        # print(grid_params)
        # grid_params['ig1'],grid_params['ig2'],grid_params['ig3'],grid_params['ig4'] = rmn.cxgaig(grtyp, ig1,ig2,ig3,ig4)
        # (int, int, str, str, int, int, int, int, _np.ndarray, _np.ndarray)
        # print(type(grid_params['ni']),type(grid_params['nj']),type(grid_params['grtyp']),type(grid_params['grref']),type(grid_params['ig1']),type(grid_params['ig2']),type(grid_params['ig3']),type(grid_params['ig4']),type(grid_params['ax']),type(grid_params['ay']))
        grid_id = rmn.ezgdef_fmem(grid_params)
        # params = rmn.ezgxprm(grid_id)
        # print(f"Grid type={params['grtyp']}/{params['grref']} of size={params['ni']}, {params['nj']} - {params}")

    elif grtyp == 'U':
        start_pos = 5
        ni = int(tictac[start_pos])
        nj = int(tictac[start_pos+1])
        encoded_ig1 = tictac[start_pos+6]
        encoded_ig2 = tictac[start_pos+7]
        encoded_ig3 = tictac[start_pos+8]
        encoded_ig4 = tictac[start_pos+9]
        position_ax = start_pos + 10
        position_ay = position_ax + ni
        sub_grid_ref = 'E'
        ig1,ig2,ig3,ig4 = rmn.cxgaig(sub_grid_ref, encoded_ig1, encoded_ig2, encoded_ig3,encoded_ig4)
        next_pos = position_ay + nj
        ax = tictac[position_ax:position_ay]
        ay = tictac[position_ay:next_pos]
        grid_params = {'grtyp':'Z','grref':'E','ni':ni,'nj':nj,'ig1':ig1,'ig2':ig2,'ig3':ig3,'ig4':ig4,'ay':ay,'ax':ax}

        # Definition de la 1ere sous-grille
        sub_grid_id_1 = rmn.ezgdef_fmem(grid_params)


        start_pos = next_pos
        ni = int(tictac[start_pos])
        nj = int(tictac[start_pos+1])
        encoded_ig1 = tictac[start_pos+6]
        encoded_ig2 = tictac[start_pos+7]
        encoded_ig3 = tictac[start_pos+8]
        encoded_ig4 = tictac[start_pos+9]
        position_ax = start_pos + 10
        position_ay = position_ax + ni
        sub_grid_ref = 'E'
        ig1,ig2,ig3,ig4 = rmn.cxgaig(sub_grid_ref, encoded_ig1, encoded_ig2, encoded_ig3,encoded_ig4)
        next_pos = position_ay + nj
        ax = tictac[position_ax:position_ay]
        ay = tictac[position_ay:next_pos]
        grid_params = {'grtyp':'Z','grref':'E','ni':ni,'nj':nj,'ig1':ig1,'ig2':ig2,'ig3':ig3,'ig4':ig4,'ay':ay,'ax':ax}
        # Definition de la 1ere sous-grille
        sub_grid_id_2 = rmn.ezgdef_fmem(grid_params)

        vercode = 1
        grtyp = 'U'
        grref = 'F'
        grid_id = rmn.ezgdef_supergrid(ni, nj, grtyp, grref, vercode, (sub_grid_id_1,sub_grid_id_2))        
        # params = rmn.ezgxprm(grid_id)
        # print(f"Grid type={params['grtyp']}/{params['grref']} of size={params['ni']}, {params['nj']} - {params}")

    else:
        # grid_params = {'grtyp':'Z','grref':grtyp,'ni':int(ni),'nj':int(nj)}

        # if isinstance(ig1,float):
        #     grid_params = {'grtyp':'Z','grref':grtyp,'ni':int(ni),'nj':int(nj)}
        #     grid_params['ig1'],grid_params['ig2'],grid_params['ig3'],grid_params['ig4'] = rmn.cxgaig(grtyp, ig1,ig2,ig3,ig4)
        # else:

        # grid_params = {'grtyp':'Z','grref':grtyp,'ni':int(ni),'nj':int(nj),'ig1':int(ig1),'ig2':int(ig2),'ig3':int(ig3),'ig4':int(ig4),'iunit':0}
        grid_params = {'grtyp':grtyp,'ni':int(ni),'nj':int(nj),'ig1':int(ig1),'ig2':int(ig2),'ig3':int(ig3),'ig4':int(ig4),'iunit':0}
        # print(f"{grid_params['ni']}\t{grid_params['nj']}\t{grid_params['grtyp']}\t{grid_params['ig1']}\t{grid_params['ig2']}\t{grid_params['ig3']}\t{grid_params['ig4']}\t")
        # grid_params['ig1'],grid_params['ig2'],grid_params['ig3'],grid_params['ig4'] = rmn.cxgaig(grtyp, ig1,ig2,ig3,ig4)
        # print(grid_params)
        # grid_params['ax'] = np.empty((ni,1), dtype=np.float32, order='F')
        # grid_params['ay'] = np.empty((1,nj), dtype=np.float32, order='F')
        # for i in range(ni): grid_params['ax'][i,0] = i#ig2+float(i)*ig1
        # for j in range(nj): grid_params['ay'][0,j] = j#ig4+float(j)*ig3
        # print(grid_params)
        grid_id = rmn.ezqkdef(grid_params)
        # int gdid = c_ezqkdef((int)xAxisSize, (int)yAxisSize, &grtyp[0], ig1, ig2, ig3, ig4, 0);
        # grid_id = rmn.ezgdef_fmem(grid_params)
        # params = rmn.ezgxprm(grid_id)
        # print(f"Grid type={params['grtyp']}/{params['grref']} of size={params['ni']}, {params['nj']} - {params}")

    return grid_id


