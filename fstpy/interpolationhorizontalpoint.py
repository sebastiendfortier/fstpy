# -*- coding: utf-8 -*-
from fstpy.plugin import Plugin
import numpy as np
import pandas as pd
import rpnpy.librmn.all as rmn
import numpy as np
import sys
from .std_reader import load_data
from .utils import initializer

class InterpolationHorizontalPointError(Exception):
    pass

class InterpolationHorizontalPoint(Plugin):
    """Horizontal Interpolation of fields to a set of latitudes and longitudes

    :param df: Input dataframe
    :type df: pd.DataFrame
    :param interpolation_type: Type of interpolation 'nearest','bi-linear','bi-cubic'
    :type interpolation_type: str
    :param extrapolation_type: Type of extrapolation 'nearest','linear','maximum','minimum','value','abort'
    :type extrapolation_type: str
    :param extrapolation_value: value for extrapolation when type is value, defaults to None
    :type extrapolation_value: float, optional
    """

    grid_types = ['A','B','G','L','N','S']
    extrapolation_types = ['maximum','minimum','value','abort']
    interpolation_types = ['nearest','bi-linear','bi-cubic']

    @initializer
    def __init__(self,df:pd.DataFrame, interpolation_type:str, extrapolation_type:str, extrapolation_value:float=None):
        self.validate_input()

    def validate_input(self):
        if self.df.empty:
            raise InterpolationHorizontalPointError('InterpolationHorizontalPoint - no data to process')
        self.validate_params()
        set_interpolation_type_options(self.interpolation_type)
        set_extrapolation_type_options(self.extrapolation_type,self.extrapolation_value)
        self.define_output_grid()

        self.groups =  self.df.groupby(by=['grid'])


    def define_output_grid(self):
        if self.method == 'user':

            if self.grtyp in ['L','N','S']:
                self.ig1,self.ig2,self.ig3,self.ig4 = rmn.cxgaig(self.grtyp, self.param1,self.param2,self.param3,self.param4)
                self.output_grid = define_grid(self.grtyp,'',self.ni,self.nj,self.ig1,self.ig2,self.ig3,self.ig4,None,None,None)
            else:
                self.output_grid = define_grid(self.grtyp,'',self.ni,self.nj,int(self.param1),int(self.param2),int(self.param3),int(self.param4),None,None,None)

        else: # method field defined
            if self.nomvar is None:
                raise InterpolationHorizontalPointError('InterpolationHorizontalPoint - you must supply a nomvar with field defined method')    

            field_df = self.df.query(f'nomvar=="{self.nomvar}"')

            #check for more than one definition for the field method
            if len(field_df.grid.unique()) > 1: 
                raise InterpolationHorizontalPointError('InterpolationHorizontalPoint - reference field found for multiple grids')  

            # get grtyp from the field
            self.grtyp = field_df.iloc[0]['grtyp']

            self.ni,self.nj,self.ig1,self.ig2,self.ig3,self.ig4 = set_grid_parameters(field_df)
            
            # get meta for this fields grid
            grid = field_df.iloc[0]['grid']
            meta_df = self.df.query(f"(nomvar in ['>>','^^','^>']) and (grid=='{grid}')").reset_index(drop=True)
            
            # define grid from meta
            if not meta_df.empty:
                if ('>>' in meta_df.nomvar.to_list()) and  ('^^' in meta_df.nomvar.to_list()):
                    self.ni,self.nj,grref, ax, ay,self.ig1,self.ig2,self.ig3,self.ig4 = get_grid_paramters_from_latlon_fields(meta_df)
                    self.output_grid = define_grid(self.grtyp,grref,self.ni,self.nj,self.ig1,self.ig2,self.ig3,self.ig4,ax,ay,None)
                    self.ig1,self.ig2,self.ig3,self.ig4 = set_output_column_values(meta_df,field_df) 

                elif ('^>' in meta_df.nomvar.to_list()):
                    tictac_df = meta_df.query('nomvar=="^>"')
                    tictac_df = load_data(tictac_df)
                    self.output_grid = define_grid(self.grtyp,'',0,0,0,0,0,0,None,None,tictac_df.iloc[0]['d'])   
                    self.ig1,self.ig2,self.ig3,self.ig4 = set_output_column_values(meta_df,field_df) 


            # define grid from field
            else:
                self.output_grid = define_grid(self.grtyp,'',self.ni,self.nj,self.ig1,self.ig2,self.ig3,self.ig4,None,None,None) 

 

    def validate_params(self):
        if self.interpolation_type not in self.interpolation_types:
            raise InterpolationHorizontalPointError(f'InterpolationHorizontalPoint - interpolation_type {self.interpolation_type} not in {self.interpolation_types}')
        if self.extrapolation_type not in self.extrapolation_types:
            raise InterpolationHorizontalPointError(f'InterpolationHorizontalPoint - extrapolation_type {self.extrapolation_type} not in {self.extrapolation_types}')


    def compute(self) -> pd.DataFrame:
        results = []
        no_mod = []
        for _,current_group in self.groups:

            current_group = load_data(current_group)

            keep_intact_hy_field(current_group, no_mod)

            keep_toctoc(current_group, results)

            vect_df = current_group.query("nomvar in ['UU','VV']").reset_index(drop=True)

            others_df = current_group.query("nomvar not in ['UU','VV','PT','>>','^^','^>','!!','HY']").reset_index(drop=True)

            pt_df = current_group.query("nomvar=='PT'").reset_index(drop=True)

            source_df, grtyp = select_input_grid_source_data(vect_df, others_df, pt_df)

            if source_df.empty:
                continue

            meta_df = current_group.query("nomvar in ['>>','^^','^>']").reset_index(drop=True)
            input_grid = define_input_grid(grtyp,source_df,meta_df)

            grids_are_equal = check_in_out_grid_equality(input_grid,self.output_grid)

            if grids_are_equal:
                no_mod.append(current_group)
                continue

            create_grid_set(input_grid,self.output_grid)

            vectorial_interpolation(vect_df,results,input_grid,self.output_grid)

            scalar_interpolation(others_df,results,input_grid,self.output_grid)

            scalar_interpolation(pt_df,results,input_grid,self.output_grid)


        res_df = pd.DataFrame(dtype=object)
        if len(results):
            res_df = pd.concat(results,ignore_index=True)   

        no_mod_df = pd.DataFrame(dtype=object)

        if len(no_mod): 
            no_mod_df = pd.concat(no_mod,ignore_index=True)

        toctoc_res_df = set_new_grid_identifiers_for_toctoc(res_df,self.ig1,self.ig2)

        other_res_df = set_new_grid_identifiers(res_df,self.grtyp,self.ni,self.nj,self.ig1,self.ig2,self.ig3,self.ig4)
        
        if not toctoc_res_df.empty:
            other_res_df = pd.concat([other_res_df,toctoc_res_df],ignore_index=True)

        if not no_mod_df.empty:
            other_res_df = pd.concat([other_res_df,no_mod_df],ignore_index=True)

        return other_res_df






###################################################################################  
###################################################################################  
def set_extrapolation_type_options(extrapolation_type,extrapolation_value):
    if extrapolation_type == 'value':
        if extrapolation_value is None:
            raise InterpolationHorizontalPointError(f'InterpolationHorizontalPoint - extrapolation_value {extrapolation_value} is not set')
        rmn.ezsetval('EXTRAP_VALUE', extrapolation_value)
        rmn.ezsetopt('EXTRAP_DEGREE', 'VALUE')
    else:
        # print( self.extrapolation_type.upper())
        rmn.ezsetopt('EXTRAP_DEGREE', extrapolation_type.upper())

def set_interpolation_type_options(interpolation_type):
    if interpolation_type == 'nearest':
        rmn.ezsetopt('INTERP_DEGREE', 'NEAREST')
    elif interpolation_type == 'bi-linear':
        rmn.ezsetopt('INTERP_DEGREE', 'LINEAR')    
    elif interpolation_type == 'bi-cubic':
        rmn.ezsetopt('INTERP_DEGREE', 'CUBIC')

def scalar_interpolation(df,results,input_grid,output_grid):
    if df.empty:
        return
    # scalar except PT
    int_df = df.copy(deep=True)
    
    is_pt = False
    if df.iloc[0]['nomvar'] == 'PT':
        is_pt = True

    # special case for PT
    if is_pt:
        # always for PT
        extrap_degree = rmn.ezgetopt(rmn.EZ_OPT_EXTRAP_DEGREE, vtype=str)
        rmn.ezsetopt('EXTRAP_DEGREE', 'NEAREST')    

    for i in df.index:
        arr = rmn.ezsint(output_grid, input_grid, df.at[i,'d'])
        int_df.at[i,'d'] = arr
    results.append(int_df)       
    if is_pt:
        # reset extrapolation options
        rmn.ezsetopt('EXTRAP_DEGREE', extrap_degree)    



def vectorial_interpolation(vect_df,results,input_grid,output_grid):
    if vect_df.empty:
        return
    uu_df = vect_df.query('nomvar=="UU"').reset_index(drop=True)
    vv_df = vect_df.query('nomvar=="VV"').reset_index(drop=True)

    if (uu_df.empty) or (vv_df.empty):
        return

    uu_int_df = uu_df.copy(deep=True)
    vv_int_df = vv_df.copy(deep=True)

    for i in uu_df.index:
        (uu, vv) = rmn.ezuvint(output_grid, input_grid, uu_df.at[i,'d'], vv_df.at[i,'d'])
        uu_int_df.at[i,'d'] = uu
        vv_int_df.at[i,'d'] = vv

    results.append(uu_int_df)    
    results.append(vv_int_df)    


def create_grid_set(input_grid,output_grid):
    rmn.ezdefset(output_grid, input_grid)

def check_in_out_grid_equality(input_grid,output_grid):
    in_params = rmn.ezgxprm(input_grid)
    in_params.pop('id')
    out_params = rmn.ezgxprm(output_grid)
    out_params.pop('id')
    return in_params == out_params

def select_input_grid_source_data(vect_df, others_df, pt_df):
    grtyp = ''
    if not vect_df.empty:
        grtyp = vect_df.iloc[0]['grtyp']
        source_df = vect_df
    elif not others_df.empty:
        grtyp = others_df.iloc[0]['grtyp']
        source_df = others_df
    elif not pt_df.empty:    
        grtyp = pt_df.iloc[0]['grtyp']
        source_df = pt_df
    else:
        source_df = pd.DataFrame(dtype=object)
    return source_df, grtyp

def define_input_grid(grtyp,source_df,meta_df):
    ni,nj,ig1,ig2,ig3,ig4 = set_grid_parameters(source_df)

    if not meta_df.empty:
        if ('>>' in meta_df.nomvar.to_list()) and  ('^^' in meta_df.nomvar.to_list()):
            ni,nj,grref,ax,ay,ig1,ig2,ig3,ig4 = get_grid_paramters_from_latlon_fields(meta_df)
            input_grid = define_grid(grtyp,grref,ni,nj,ig1,ig2,ig3,ig4,ax,ay,None)
            
        elif ('^>' in meta_df.nomvar.to_list()):
            tictac_df = meta_df.query('nomvar=="^>"')
            input_grid = define_grid(grtyp,'',0,0,0,0,0,0,None,None,tictac_df.iloc[0]['d'])    

    else:
        input_grid = define_grid(grtyp,' ',ni,nj,ig1,ig2,ig3,ig4,None,None,None) 

    return input_grid       

def keep_intact_hy_field(current_group, no_mod):
    hy_df = current_group.query("nomvar in ['HY']").reset_index(drop=True)
    if not hy_df.empty:
        no_mod.append(hy_df)

def keep_toctoc(current_group, results):
    toctoc_df = current_group.query("nomvar in ['!!']").reset_index(drop=True)
    # we can add toctoc from input grid
    if not toctoc_df.empty:
        results.append(toctoc_df)

def get_grid_paramters_from_latlon_fields(meta_df):
    lat_df = meta_df.query('nomvar==">>"')
    lat_df = load_data(lat_df)
    lon_df = meta_df.query('nomvar=="^^"')
    lon_df = load_data(lon_df)
    ni = lat_df.iloc[0]['ni']
    nj = lon_df.iloc[0]['nj']
    grref = lat_df.iloc[0]['grtyp']
    ax = lat_df.iloc[0]['d']
    ay = lon_df.iloc[0]['d']
    ig1 = lat_df.iloc[0]['ig1']
    ig2 = lat_df.iloc[0]['ig2']
    ig3 = lat_df.iloc[0]['ig3']
    ig4 = lat_df.iloc[0]['ig4']
    return ni,nj,grref,ax,ay,ig1,ig2,ig3,ig4

def set_grid_parameters(df):
    ni = df.iloc[0]['ni']
    nj = df.iloc[0]['nj']
    ig1 = df.iloc[0]['ig1']
    ig2 = df.iloc[0]['ig2']
    ig3 = df.iloc[0]['ig3']
    ig4 = df.iloc[0]['ig4']
    return ni,nj,ig1,ig2,ig3,ig4

def set_output_column_values(meta_df,field_df):
    ig1 = meta_df.iloc[0]['ip1']
    ig2 = meta_df.iloc[0]['ip2']
    ig3 = field_df.iloc[0]['ig3']
    ig4 = field_df.iloc[0]['ig4']
    return ig1,ig2,ig3,ig4

def set_new_grid_identifiers_for_toctoc(res_df,ig1,ig2):
    toctoc_res_df = res_df.query('nomvar == "!!"').copy(deep=True)
    toctoc_res_df['ip1'] = ig1
    toctoc_res_df['ip2'] = ig2
    return toctoc_res_df


def set_new_grid_identifiers(res_df,grtyp,ni,nj,ig1,ig2,ig3,ig4):
    other_res_df = res_df.query('nomvar != "!!"').copy(deep=True)
    shape_list = [(ni,nj) for _ in range(len(other_res_df.index))]
    other_res_df["shape"] = shape_list
    other_res_df['ni'] = ni
    other_res_df['nj'] = nj
    other_res_df['grtyp'] = grtyp
    other_res_df['interpolated'] = True
    other_res_df['ig1'] = ig1
    other_res_df['ig2'] = ig2
    other_res_df['ig3'] = ig3
    other_res_df['ig4'] = ig4
    return other_res_df
    
def define_grid(grtyp:str,grref:str,ni:int,nj:int,ig1:int,ig2:int,ig3:int,ig4:int,ax:np.ndarray,ay:np.ndarray,tictac:np.ndarray) -> int:
    #longitude = X
    grid_types = ['A','B','E','G','L','N','S','U','X','Y','Z','#']
    grid_id = -1
    
    if grtyp not in grid_types:
        raise InterpolationHorizontalPointError(f'InterpolationHorizontalPoint - grtyp {grtyp} not in {grid_types}')


    if  grtyp in ['Y','Z','#']:
        
        grid_params = {'grtyp':grtyp,'grref':grref,'ni':int(ni),'nj':int(nj),'ay':ay,'ax':ax,'ig1':int(ig1),'ig2':int(ig2),'ig3':int(ig3),'ig4':int(ig4)}
        grid_id = rmn.ezgdef_fmem(grid_params)

    elif grtyp == 'U':
        ni, nj, sub_grid_id_1, sub_grid_id_2 = create_type_u_sub_grids(tictac, ni, nj, ig1, ig2, ig3, ig4, ax, ay)

        vercode = 1
        grtyp = 'U'
        grref = ''

        grid_id = rmn.ezgdef_supergrid(ni, 2*nj, grtyp, grref, vercode, (sub_grid_id_1,sub_grid_id_2))        

    else:
        grid_params = {'grtyp':grtyp,'ni':int(ni),'nj':int(nj),'ig1':int(ig1),'ig2':int(ig2),'ig3':int(ig3),'ig4':int(ig4),'iunit':0}
        grid_id = rmn.ezqkdef(grid_params)

    return grid_id

def create_type_u_sub_grids(tictac, ni, nj, ig1, ig2, ig3, ig4, ax, ay):
    start_pos = 5
    tictac = tictac.flatten()

    ni, nj, ig1, ig2, ig3, ig4, ay, ax, next_pos = get_grid_parameters_from_tictac_offset(tictac, start_pos, ni, nj, ig1, ig2, ig3, ig4, ax, ay)

    grid_params = {'grtyp':'Z','grref':'E','ni':ni,'nj':nj,'ig1':ig1,'ig2':ig2,'ig3':ig3,'ig4':ig4,'ay':ay,'ax':ax}

    # Definition de la 1ere sous-grille
    sub_grid_id_1 = rmn.ezgdef_fmem(grid_params)


    start_pos = next_pos
    ni, nj, ig1, ig2, ig3, ig4, ay, ax, _ = get_grid_parameters_from_tictac_offset(tictac, start_pos, ni, nj, ig1, ig2, ig3, ig4, ax, ay)

    grid_params = {'grtyp':'Z','grref':'E','ni':ni,'nj':nj,'ig1':ig1,'ig2':ig2,'ig3':ig3,'ig4':ig4,'ay':ay,'ax':ax}

    # Definition de la 1ere sous-grille
    sub_grid_id_2 = rmn.ezgdef_fmem(grid_params)
    return ni, nj, sub_grid_id_1, sub_grid_id_2

def get_grid_parameters_from_tictac_offset(tictac, start_pos, ni, nj, ig1, ig2, ig3, ig4, ax, ay):
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
    return ni, nj, ig1, ig2, ig3, ig4, ay, ax, next_pos



def find_index_of_lat_lon_not_in_grid(input_grid, grid_horizontal_dimension, grid_vertical_dimension, latitudes, longitudes):
    # get X and Y position of all latitudes longitudes coordinate ( to later check if latlon are in the source grid )
    # xy = {
    #        'id' : grid id, same as input arg
    #        'x'  : list of points x-coor (numpy.ndarray)
    #        'y'  : list of points y-coor (numpy.ndarray)
    #    }
    coords = rmn.gdxyfll(input_grid, latitudes, longitudes)
    # RpnFunctions::ezGetXYPositionFromLatLon(input_grid, x, y, latitudes, longitudes)

    x_grid_lower_bound = 0.5
    x_grid_upper_bound = grid_horizontal_dimension + 0.5
    y_grid_lower_bound = 0.5
    y_grid_upper_bound = grid_vertical_dimension + 0.5

    #find index of latlon not in input grid limits
    epsilon = 0.00002
    indexes=[]
    for i in range(len(latitudes)):
        # need an epsilon
        # ex: for the south pole ezscint can return something like 0.499987 instead of 0.5
        if(    _lt_(coords['x'][i], x_grid_lower_bound, epsilon)
            or _gt_(coords['x'][i], x_grid_upper_bound, epsilon)
            or _lt_(coords['y'][i], y_grid_lower_bound, epsilon)
            or _gt_(coords['y'][i], y_grid_upper_bound, epsilon)):

            sys.stdout.write(f"latlon coordinate {latitudes[i]} , {longitudes[i]} outside input grid.\n")
            sys.stdout.write(f"x_grid_lower_bound={x_grid_lower_bound} , x_grid_upper_bound={x_grid_upper_bound}, y_grid_lower_bound={y_grid_lower_bound}, y_grid_upper_bound={y_grid_upper_bound}\n")
            sys.stdout.write(f"ezscint value for X is:{coords['x'][i]}\n")
            sys.stdout.write(f"ezscint value for Y is:{coords['y'][i]}\n")
            sys.stdout.write(f"epsilon:{epsilon}\n")
            indexes.append(i)
    return indexes        



def do_extrapolation(interpolated_data, data_before_interpolation, indexes, extrapolation_type, extrapolation_value):
    if extrapolation_type == "VALUE":
        # replace value at latlon outside grid by a fixed value
         for i in range(indexes):
            interpolated_data[i] = extrapolation_value
        
    
    elif extrapolation_type == "MAXIMUM":
        min, max = find_min_max(data_before_interpolation)
        max = max + (max - min) * 0.05

        # replace value at latlon outside grid by the max value of the fields
        for i in range(indexes):
            interpolated_data[i] = max
        
    
    elif extrapolation_type == "MINIMUM":
        min, max = find_min_max(data_before_interpolation)
        min = min - (max - min) * 0.05

        # replace value at latlon outside grid by the min value of the fields
        for i in range(indexes):
            interpolated_data[i] = min
    
    elif extrapolation_type == "ABORT":
        raise InterpolationHorizontalPointError("ABORTED AS REQUESTED BY THE USE OF THE 'extrapolation_type ABORT' OPTION.\n")
    

def find_min_max(array):
    return np.min(array), np.max(array)


def _eq_(value, threshold, epsilon=0.00001):
    return (abs(value - threshold) <= epsilon)

def _ge_(value, threshold, epsilon=0.00001):
    return ((value > threshold) or _eq_(value, threshold, epsilon))

def _lt_(value, threshold, epsilon=0.00001):
    return (not _ge_(value, threshold, epsilon))

def _le_(value, threshold, epsilon=0.00001):
    return (value < threshold or _eq_(value, threshold, epsilon))

def _gt_(value, threshold, epsilon=0.00001):
    return (not _le_(value, threshold, epsilon))
