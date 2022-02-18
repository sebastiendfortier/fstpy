# -*- coding: utf-8 -*-
import logging
import pandas as pd
from fstpy.dataframe import add_columns, add_grid_column, add_path_and_key_columns

from fstpy.std_vgrid import VerticalCoordType, get_vertical_coord
from .dataframe_utils import metadata_cleanup

from .utils import initializer

STANDARD_ATMOSPHERE = 1013.25


class QuickPressureError(Exception):
    pass


class QuickPressure():
    """creates a pressure field associated to a level for each identified vertical coordinate type

    :param df: input dataframe
    :type df: pd.DataFrame
    :param standard_atmosphere: calculate pressure in standard atmosphere if specified, defaults to False
    :type standard_atmosphere: bool, optional
    """

    @initializer
    def __init__(self, df: pd.DataFrame, standard_atmosphere: bool = False):
        self.validate_input()

    def validate_input(self):
        if self.df.empty:
            logging.error('No data to process')

        self.meta_df = self.df.loc[self.df.nomvar.isin(
            ["^^", ">>", "^>", "!!", "!!SF", "HY", "P0", "PT"])].reset_index(drop=True)

        if 'grid' not in self.df.columns:
            self.df = add_grid_column(self.df)
        if 'ip1_kind' not in self.df.columns:
            self.df = add_columns(self.df, 'ip_info')
        if 'path' not in self.df.columns:
            self.df = add_path_and_key_columns(self.df)

    def compute(self):
        grid_groups = self.df.groupby(['path'])
        df_list = []
        for _, path_df in grid_groups:
            grid_groups = path_df.groupby(['grid'])

            for _, grid_df in grid_groups:
                grids_meta_df = grid_df.loc[grid_df.nomvar.isin(["!!", "P0", "PT", "!!SF"])]#.reset_index(drop=True)
                vctypes_groups = grid_df.groupby(['vctype'])

                for vctype, vt_df in vctypes_groups:
                    if vctype == VerticalCoordType.UNKNOWN:
                        continue

                    datev_groups = vt_df.groupby(['datev'])
                    for _, dv_df in datev_groups:
                        without_meta_df = dv_df.loc[(dv_df.ip1 != 0) & (~dv_df.nomvar.isin(
                            ["!!", "HY", "P0", "PT", ">>", "^^", "PX", "PXSA"]))]

                        if without_meta_df.empty:
                            continue
                        else:
                            vcoord = get_vertical_coord(path_df, grids_meta_df, without_meta_df)

                            if self.standard_atmosphere:
                                px_df = vcoord.pressure_standard_atmosphere()
                            else:
                                px_df = vcoord.pressure()
                            if not px_df.empty:
                                df_list.append(px_df)

        df_list.append(self.meta_df)
        res_df = pd.concat(df_list)#, ignore_index=True)
        res_df = metadata_cleanup(res_df)
        return res_df
