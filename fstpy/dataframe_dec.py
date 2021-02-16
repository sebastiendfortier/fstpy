# -*- coding: utf-8 -*-



def set_typvar(df, i):
    if ('typvar' in df.columns) and ('unit_converted' in df.columns) and (df.at[i,'unit_converted'] == True) and (len(df.at[i,'typvar']) == 1):
        df.at[i,'typvar']  += 'U'