# -*- coding: utf-8 -*-
import numpy as np
import pandas as pd
import rpnpy.vgd.all as vgd


def get_df_from_grid(grid_params: dict) -> pd.DataFrame:
    """For grid types Z, Y or U, produces a DataFrame of >>,^^ or ^> fields

    :param grid_params: horizontal grid definition dictionnary
    :type grid_params: dict
    :return: DataFrame of >>,^^ or ^> fields
    :rtype: pd.DataFrame
    """
    g = grid_params
    if (g['grtyp'] == 'Z') or (g['grtyp'] == 'Y'):
        meta_df = pd.DataFrame(
            [
                {'nomvar': '>>', 'typvar': 'X', 'etiket': '', 'ni': g['ni'], 'nj':1, 'nk':1, 'dateo':0, 'ip1':g['ig1'], 
                'ip2':g['ig2'], 'ip3':0, 'deet':0, 'npas':0, 'datyp':5, 'nbits':32, 'grtyp':g['grref'], 
                'ig1':g['ig1ref'], 'ig2':g['ig2ref'], 'ig3':g['ig3ref'], 'ig4':g['ig4ref'], 'datev':0, 'd':g['ax']},
                {'nomvar': '^^', 'typvar': 'X', 'etiket': '', 'ni': 1, 'nj': g['nj'], 'nk':1, 'dateo':0, 'ip1':g['ig1'], 
                'ip2':g['ig2'], 'ip3':0, 'deet':0, 'npas':0, 'datyp':5, 'nbits':32, 'grtyp':g['grref'], 
                'ig1':g['ig1ref'], 'ig2':g['ig2ref'], 'ig3':g['ig3ref'], 'ig4':g['ig4ref'], 'datev':0, 'd':g['ay']}
            ]
            )

    elif g['grtyp'] == 'U':
        meta_df = pd.DataFrame(
            [
                {'nomvar': '^>', 'typvar': 'X', 'etiket': '', 'ni': g['axy'].shape[0], 'nj':1, 'nk':1, 'dateo':0, 
                'ip1':g['ig1'], 'ip2':g['ig2'], 'ip3':0, 'deet':0, 'npas':0, 'datyp':5, 'nbits':32, 'grtyp':g['grref'], 
                'ig1':g['ig1ref'], 'ig2':g['ig2ref'], 'ig3':g['ig3ref'], 'ig4':g['ig4ref'], 'datev':0, 'd':g['axy']}
            ]
            )
    else:
        meta_df = pd.DataFrame(dtype=object)
    return meta_df

# def get_df_from_vgrid(vgrid_descriptor: vgd.VGridDescriptor, ip1: int, ip2: int) -> pd.DataFrame:
#     v = vgrid_descriptor
#     vcoord = vertical_coord_to_dict(v)
#     vers = str(vcoord['VERSION']).zfill(3)
#     ig1 = int(''.join([str(vcoord['KIND']),vers]))
#     data = vcoord['VTBL']
#     meta_df = pd.DataFrame([{'nomvar':'!!', 'typvar':'X', 'etiket':'', 'ni':data.shape[0], 'nj':data.shape[1], 'nk':1, 'dateo':0, 'ip1':ip1, 'ip2':ip2, 'ip3':0, 'deet':0, 'npas':0, 'datyp':5, 'nbits':64, 'grtyp':'X', 'ig1':ig1, 'ig2':0, 'ig3':0, 'ig4':0, 'datev':0, 'd':data}])
#     return meta_df

# def vertical_coord_to_dict(vgrid_descriptor) -> dict:
#     myvgd = vgrid_descriptor
#     vcoord={}
#     vcoord['KIND'] = vgd.vgd_get(myvgd,'KIND')
#     vcoord['VERSION'] = vgd.vgd_get(myvgd,'VERSION')
#     vcoord['VTBL'] = np.asfortranarray(np.squeeze(vgd.vgd_get(myvgd,'VTBL')))
#     return vcoord

# gp = {
#     'grtyp' : 'Z',
#     'grref' : 'E',
#     'ni'    : 90,
#     'nj'    : 45,
#     'lat0'  : 35.,
#     'lon0'  : 250.,
#     'dlat'  : 0.5,
#     'dlon'  : 0.5,
#     'xlat1' : 0.,
#     'xlon1' : 180.,
#     'xlat2' : 1.,
#     'xlon2' : 270.
# }
# g = rmn.encodeGrid(gp)

# 'xlat1': 0.0,
# 'xlon1': 180.0,
# 'xlat2': 1.0,
# 'xlon2': 270.0,
# 'ni': 90,
# 'nj': 45,
# 'rlat0': 34.059606166461926,
# 'rlon0': 250.23401123256826,
# 'dlat': 0.5,
# 'dlon': 0.5,
# 'lat0': 35.0,
# 'lon0': 250.0,
# 'grtyp': 'Z',
# 'grref': 'E',
# 'ig1ref': 900,          ig1
# 'ig2ref': 10,           ig2
# 'ig3ref': 43200,        ig3
# 'ig4ref': 43200,        ig4
# 'ig1': 66848,
# 'ig2': 39563,
# 'ig3': 0,
# 'ig4': 0,
# 'id': 0,
# 'tag1': 66848,
# 'tag2': 39563,
# 'tag3': 0,
# 'shape': (90, 45)}
#   nomvar typvar etiket  ni  nj  nk  dateo    ip1    ip2  ip3  ...  datyp  nbits  grtyp  ig1 ig2    ig3    ig4  datev        grid
# 0     >>      X         90   1   1      0  66848  39563    0  ...      5     32      E  900  10  43200  43200      0  6684839563
# 1     ^^      X          1  45   1      0  66848  39563    0  ...      5     32      E  900  10  43200  43200      0  6684839563

# {'nomvar':'>>', typvar:'X', 'etiket':'', 'ni':g.ni, nj:1, 'nk':1, 'dateo':0, 'ip1':g.ig1, 'ip2':g.ig2, 'ip3':0, 'datyp':5, 'nbits':32, 'grtyp':g.grref, 'ig1':g.ig1ref, 'ig2':g.ig2ref, 'ig3':g.ig3ref, 'ig4'g.ig4ref:, 'datev':0, 'd':g.ax}
# {'nomvar':'^^', typvar:'X', 'etiket':'', 'ni':1, nj:g.nj, 'nk':1, 'dateo':0, 'ip1':g.ig1, 'ip2':g.ig2, 'ip3':0, 'datyp':5, 'nbits':32, 'grtyp':g.grref, 'ig1':g.ig1ref, 'ig2':g.ig2ref, 'ig3':g.ig3ref, 'ig4'g.ig4ref:, 'datev':0, 'd':g.ay}
