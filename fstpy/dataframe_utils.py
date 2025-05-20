# -*- coding: utf-8 -*-
import datetime
import logging
import math

import numpy as np
import pandas as pd

from .rmn_interface import RmnInterface

from fstpy import DATYP_DICT
from fstpy.utils import to_numpy, safe_concatenate

from .dataframe import add_columns, add_ip_info_columns, reorder_columns
from .std_dec import convert_rmndate_to_datetime
from .std_vgrid import set_vertical_coordinate_type


class SelectError(Exception):
    pass


def select_meta(df: pd.DataFrame) -> pd.DataFrame:
    """select all metadata fields in a dataframe from

    :param df: input dataframe
    :type df: pd.DataFrame
    :return: output dataframe of metadat fileds only
    :rtype: pd.DataFrame

    """
    meta_df = df.loc[df.nomvar.isin(["!!", "P0", "PT", ">>", "^^", "^>", "HY", "!!SF"])]
    return meta_df


def select_with_meta(df: pd.DataFrame, nomvar: list) -> pd.DataFrame:
    """Select fields with accompaning meta data

    :param df: dataframe to select from
    :type df: pd.DataFrame
    :param nomvar: list of nomvars to select
    :type nomvar: list
    :raises SelectError: if dataframe is empty, if nothing to select or if variable not found in dataframe
    :return: dataframe with selection results
    :rtype: pd.DataFrame

    """
    if df.empty:
        raise SelectError(f"dataframe is empty - nothing to select into")

    if not isinstance(nomvar, list):
        nomvar = [nomvar]

    results = []

    if len(nomvar) == 0:
        raise SelectError(f"nomvar is empty - nothing to select")

    for var in nomvar:
        res_df = df.loc[df.nomvar == var]
        if res_df.empty:
            raise SelectError(f"missing {var} in dataframe")
        results.append(res_df)

    meta_df = select_meta(df)

    if not meta_df.empty:
        results.append(meta_df)

    selection_result_df = pd.safe_concat(results)

    selection_result_df = metadata_cleanup(selection_result_df)

    return selection_result_df


def metadata_cleanup(df: pd.DataFrame, strict_toctoc=True) -> pd.DataFrame:
    """Cleans the metadata from a dataframe according to rules.

    :param df: dataframe to clean
    :type df: pd.DataFrame
    :return: dataframe with only cleaned meta_data
    :rtype: pd.DataFrame
    """

    if df.empty:
        return df

    df = set_vertical_coordinate_type(df)

    no_meta_df = df.loc[~df.nomvar.isin(["!!", "P0", "PT", ">>", "^^", "^>", "HY", "!!SF"])]

    # get deformation fields
    grid_deformation_fields_df = get_grid_deformation_fields(df, no_meta_df)

    sigma_ips = get_sigma_ips(no_meta_df)

    hybrid_ips = get_hybrid_ips(no_meta_df)

    # get P0's
    p0_fields_df = get_p0_fields(df, no_meta_df, hybrid_ips, sigma_ips)

    # get PT's
    pt_fields_df = get_pt_fields(df, no_meta_df, sigma_ips)

    # get HY
    hy_field_df = get_hy_field(df, hybrid_ips)

    pressure_ips = get_pressure_ips(no_meta_df)

    # get !!'s strict
    toctoc_fields_df = get_toctoc_fields(df, no_meta_df, hybrid_ips, sigma_ips, pressure_ips, strict_toctoc)

    new_df = safe_concatenate(
        [grid_deformation_fields_df, p0_fields_df, pt_fields_df, hy_field_df, toctoc_fields_df, no_meta_df]
    )

    # new_df.sort_index(inplace=True)
    # new_df.reset_index(inplace=True)

    return new_df


class VoirError(Exception):
    pass


def voir(df: pd.DataFrame, style=False):
    """Displays the metadata of the supplied records in the rpn voir format"""
    if df.empty:
        raise VoirError("No records to process")

    to_print_df = df.copy()
    to_print_df["datyp"] = to_print_df["datyp"].map(DATYP_DICT)
    to_print_df["datev"] = to_print_df["datev"].apply(convert_rmndate_to_datetime)
    to_print_df["dateo"] = to_print_df["dateo"].apply(convert_rmndate_to_datetime)
    to_print_df = add_ip_info_columns(to_print_df)

    res_df = to_print_df.sort_values(by=["nomvar", "level"], ascending=[True, False])

    if style:
        res_df = res_df.drop(
            columns=[
                "dateo",
                "grid",
                "run",
                "implementation",
                "ensemble_member",
                "d",
                "ip1_kind",
                "ip2_dec",
                "ip2_kind",
                "ip2_pkind",
                "ip3_dec",
                "ip3_kind",
                "ip3_pkind",
                "date_of_observation",
                "date_of_validity",
                "forecast_hour",
                "d",
                "surface",
                "follow_topography",
                "ascending",
                "interval",
                "label",
                "unit",
                "description",
                "zapped",
                "filtered",
                "interpolated",
                "unit_converted",
                "bounded",
                "missing_data",
                "ensemble_extra_info",
                "vctype",
                "data_type_str",
                "level",
                "ip1_pkind",
                "multiple_modifications",
            ],
            errors="ignore",
        )
        res_df = reorder_columns(
            res_df,
            ordered=[
                "nomvar",
                "typvar",
                "etiket",
                "ni",
                "nj",
                "nk",
                "datev",
                "level",
                " ",
                "ip1",
                "ip2",
                "ip3",
                "deet",
                "npas",
                "datyp",
                "nbits",
                "grtyp",
                "ig1",
                "ig2",
                "ig3",
                "ig4",
            ],
        )
    else:
        res_df = res_df.drop(
            columns=[
                "datev",
                "grid",
                "run",
                "implementation",
                "ensemble_member",
                "d",
                "ip1_kind",
                "ip2_dec",
                "ip2_kind",
                "ip2_pkind",
                "path",
                "key",
                "shape",
                "ip3_dec",
                "ip3_kind",
                "ip3_pkind",
                "date_of_observation",
                "date_of_validity",
                "forecast_hour",
                "d",
                "surface",
                "follow_topography",
                "ascending",
                "interval",
                "label",
                "unit",
                "description",
                "zapped",
                "filtered",
                "interpolated",
                "unit_converted",
                "bounded",
                "missing_data",
                "ensemble_extra_info",
                "vctype",
                "data_type_str",
                "level",
                "ip1_pkind",
                "multiple_modifications",
            ],
            errors="ignore",
        )

    # print('    NOMV TV   ETIQUETTE        NI      NJ    NK (DATE-O  h m s) FORECASTHOUR      IP1        LEVEL        IP2       IP3     DEET     NPAS  DTY   G   IG1   IG2   IG3   IG4')
    print("\n%s" % res_df.reset_index(drop=True).to_string(header=True))


class FstStatError(Exception):
    """Error raised when there is an issue with fststat.

    1. When dataframe is empty
    2. When there are no records to process

    """

    pass


def fststat(df: pd.DataFrame):
    """Produces summary statistics for a dataframe

    :param df: input dataframe
    :type df: pd.DataFrame
    """
    logging.info("fststat")

    if df.empty:
        raise FstStatError("fststat - no records to process")
    df = add_columns(df, ["ip_info"])
    compute_stats(df)


def compute_stats(df: pd.DataFrame):
    pd.options.display.float_format = "{:8.6E}".format
    df["min"] = None
    df["max"] = None
    df["mean"] = None
    df["std"] = None
    df["min_pos"] = None
    df["max_pos"] = None
    # print(f"        {'nomvar':6s} {'typvar':6s} {'level':8s} {'ip1':9s} {'ip2':4s} {'ip3':4s} {'dateo':10s} {'etiket':14s} {'mean':8s} {'std':8s} {'min_pos':12s} {'min':8s} {'max_pos':12s} {'max':8s}")
    # i  = 0
    for row in df.itertuples():
        d = to_numpy(row.d)
        min_pos = np.unravel_index(np.argmin(d), (row.ni, row.nj))
        df.at[row.Index, "min_pos"] = (min_pos[0] + 1, min_pos[1] + 1)
        max_pos = np.unravel_index(np.argmax(d), (row.ni, row.nj))
        df.at[row.Index, "max_pos"] = (max_pos[0] + 1, max_pos[1] + 1)
        df.at[row.Index, "min"] = np.min(d)
        df.at[row.Index, "max"] = np.max(d)
        df.at[row.Index, "mean"] = np.mean(d)
        df.at[row.Index, "std"] = np.std(d)
        # print(f'{i:5d} - {row.nomvar:6s} {row.typvar:6s} {row.level:8.6f} {row.ip1:9d} {row.ip2:4d} {row.ip3:4d} {row.dateo:10d} {row.etiket:14s} {np.mean(d):8.6f} {np.std(d):8.6f} {str(min_pos):12s} {np.min(d):8.6f} {str(max_pos):12s} {np.max(d):8.6f}')
        # i = i+1
    print(
        df[
            [
                "nomvar",
                "typvar",
                "level",
                "ip1",
                "ip2",
                "ip3",
                "dateo",
                "etiket",
                "mean",
                "std",
                "min_pos",
                "min",
                "max_pos",
                "max",
            ]
        ].to_string()
    )


def get_kinds_and_ip1(df: pd.DataFrame) -> dict:
    ip1s = df.ip1.unique()
    kinds = {}
    for ip1 in ip1s:
        if math.isnan(ip1):
            continue
        (_, kind) = RmnInterface.convert_ip(RmnInterface.CONVIP_DECODE, int(ip1))
        if kind not in kinds.keys():
            kinds[kind] = []
        kinds[kind].append(ip1)

    return kinds


def get_ips(df: pd.DataFrame, sigma=False, hybrid=False, pressure=False) -> list:
    kinds = get_kinds_and_ip1(df)

    ip1_list = []
    if sigma:
        if 1 in kinds.keys():
            ip1_list.extend(kinds[1])
    if hybrid:
        if 5 in kinds.keys():
            ip1_list.extend(kinds[5])
    if pressure:
        if 2 in kinds.keys():
            ip1_list.extend(kinds[2])
    return ip1_list


def get_model_ips(df: pd.DataFrame) -> list:
    return get_ips(df, sigma=True, hybrid=True)


def get_sigma_ips(df: pd.DataFrame) -> list:
    return get_ips(df, sigma=True)


def get_pressure_ips(df: pd.DataFrame) -> list:
    return get_ips(df, pressure=True)


def get_hybrid_ips(df: pd.DataFrame) -> list:
    return get_ips(df, hybrid=True)


def get_toctoc_fields(
    df: pd.DataFrame, no_meta_df: pd.DataFrame, hybrid_ips: list, sigma_ips: list, pressure_ips: list, strict=True
):
    mask_toctoc = df.nomvar == "!!"
    other_df = df.loc[~mask_toctoc]
    toctoc_df = df.loc[mask_toctoc]

    # if df contains !!SF you dont want to remove the !!
    if not df.loc[df["nomvar"] == "!!SF"].empty:
        return pd.safe_concat([toctoc_df, df.loc[df["nomvar"] == "!!SF"]])

    df_list = []

    if not toctoc_df.empty:
        toctoc_df_to_keep = toctoc_df[
            toctoc_df.apply(lambda row, other_df=other_df: is_toctoc_necessary(row["grid"], row["d"], other_df), axis=1)
        ]
        if not toctoc_df_to_keep.empty:
            df_list.append(toctoc_df_to_keep)

    hybrid_fields_df = pd.DataFrame(dtype=object)
    # hybrid
    if len(hybrid_ips):
        hybrid_fields_df = no_meta_df.loc[no_meta_df.ip1.isin(hybrid_ips)]

    hybrid_grids = []
    if not hybrid_fields_df.empty:
        hybrid_grids = hybrid_fields_df.grid.unique()

    # sigma
    sigma_fields_df = pd.DataFrame(dtype=object)
    if len(sigma_ips):
        sigma_fields_df = no_meta_df.loc[no_meta_df.ip1.isin(sigma_ips)]

    sigma_grids = []
    if not sigma_fields_df.empty:
        sigma_grids = sigma_fields_df.grid.unique()

    # pressure
    pressure_fields_df = pd.DataFrame(dtype=object)
    if len(pressure_ips):
        pressure_fields_df = no_meta_df.loc[no_meta_df.ip1.isin(pressure_ips)]

    pressure_grids = []
    if not pressure_fields_df.empty:
        pressure_grids = pressure_fields_df.grid.unique()

    for grid in hybrid_grids:
        # grids_no_meta_df = no_meta_df.loc[no_meta_df.grid == grid]
        # vctypes = list(grids_no_meta_df.vctype.unique())
        hyb_toctoc_df = toctoc_df.loc[
            (toctoc_df.grid == grid)
            & (toctoc_df.ig1.isin([1003, 5001, 5002, 5003, 5004, 5005, 5100, 5999, 21001, 21002]))
        ]
        # vctypes = list(hyb_toctoc_df.ig1.unique())
        # vctypes = numeric_vctype_to_string(vctypes)
        if not hyb_toctoc_df.empty:
            df_list.append(hyb_toctoc_df)

    # vcode 1001 -> Sigma levels
    # vcode 1002 -> Eta levels
    for grid in sigma_grids:
        # grids_no_meta_df = no_meta_df.loc[no_meta_df.grid == grid]
        sigma_toctoc_df = toctoc_df.loc[(toctoc_df.grid == grid) & (toctoc_df.ig1.isin([1001, 1002]))]
        # vctypes = list(sigma_toctoc_df.ig1.unique())
        # vctypes = numeric_vctype_to_string(vctypes)
        if not sigma_toctoc_df.empty:
            df_list.append(sigma_toctoc_df)

    # vcode 2001 -> Pressure levels
    for grid in pressure_grids:
        presure_toctoc_df = toctoc_df.loc[(toctoc_df.grid == grid) & (toctoc_df.ig1 == 2001)]
        if not presure_toctoc_df.empty:
            df_list.append(presure_toctoc_df)

    toctoc_fields_df = pd.DataFrame(dtype=object)

    if len(df_list):
        toctoc_fields_df = pd.safe_concat(df_list)

    toctoc_fields_df = toctoc_fields_df.drop_duplicates(
        subset=[
            "grtyp",
            "nomvar",
            "typvar",
            "ni",
            "nj",
            "nk",
            "ip1",
            "ip2",
            "ip3",
            "deet",
            "npas",
            "nbits",
            "ig1",
            "ig2",
            "ig3",
            "ig4",
            "datev",
            "dateo",
            "datyp",
        ],
        ignore_index=True,
    )

    # toctoc_fields_df.sort_index(inplace=True)
    return toctoc_fields_df


# def numeric_vctype_to_string(vctypes):
#     vctype_list = []
#     for vctype in vctypes:
#         if vctype == 5002:
#             vctype_list.append('HYBRID_5002')
#         elif vctype == 5001:
#             vctype_list.append('HYBRID_5001')
#         elif vctype == 5005:
#             vctype_list.append('HYBRID_5005')
#         elif vctype == 2001:
#             vctype_list.append('PRESSURE_2001')
#         elif vctype == 1002:
#             vctype_list.append('ETA_1002')
#         elif vctype == 1001:
#             vctype_list.append('SIGMA_1001')
#         else:
#             vctype_list.append('UNKNOWN')
#     return vctype_list


def intersect_arrays(arr1, arr2) -> bool:
    found_intersections = np.intersect1d(arr1, arr2)

    # Return True if intersections are found
    return found_intersections


def is_toctoc_necessary(grid, d, other_df):
    import dask.array as da

    list_ip1_df = other_df.loc[other_df["grid"] == grid, "ip1"].values
    list_ip1_toctoc = d[0]
    data_type = list_ip1_df.dtype
    # ip1_intersection = np.intersect1d(list_ip1_toctoc,list_ip1_df)
    intersections = da.map_blocks(intersect_arrays, list_ip1_toctoc, list_ip1_df, dtype=data_type)
    result = intersections.compute()

    return result.size != 0


def get_hy_field(df: pd.DataFrame, hybrid_ips: list):
    hy_field_df = pd.DataFrame(dtype=object)
    if len(hybrid_ips):
        hy_field_df = df.loc[df.nomvar == "HY"]

    hy_field_df = hy_field_df.drop_duplicates(
        subset=[
            "grtyp",
            "nomvar",
            "typvar",
            "ni",
            "nj",
            "nk",
            "ip1",
            "ip2",
            "ip3",
            "deet",
            "npas",
            "nbits",
            "ig1",
            "ig2",
            "ig3",
            "ig4",
            "datev",
            "dateo",
            "datyp",
        ],
        ignore_index=True,
    )
    # hy_field_df.sort_index(inplace=True)

    return hy_field_df


def get_grid_deformation_fields(df: pd.DataFrame, no_meta_df: pd.DataFrame):
    col_subset = [
        "nomvar",
        "typvar",
        "etiket",
        "ni",
        "nj",
        "nk",
        "dateo",
        "ip1",
        "ip2",
        "ip3",
        "deet",
        "npas",
        "ig1",
        "ig2",
        "ig3",
        "ig4",
    ]

    grid_deformation_fields_df = pd.DataFrame(dtype=object)

    groups = no_meta_df.groupby(["grid", "dateo", "datev", "deet", "npas"])

    df_list = []

    for (grid, dateo, _, deet, npas), group in groups:
        if len(list(group.ni.unique())) > 1:
            logging.error(f"grid with fields of different sizes for ni {group.ni.unique()}")
        if len(list(group.nj.unique())) > 1:
            logging.error(f"grid with fields of different sizes for nj {group.nj.unique()}")

        lat_df = get_specific_meta_field(df, col_subset, "^^", grid, dateo, deet, npas)
        lon_df = get_specific_meta_field(df, col_subset, ">>", grid, dateo, deet, npas)
        tictac_df = get_specific_meta_field(df, col_subset, "^>", grid, dateo, deet, npas)

        df_list.append(lat_df)
        df_list.append(lon_df)
        df_list.append(tictac_df)

    grid_deformation_fields_df = safe_concatenate(df_list)

    grid_deformation_fields_df = grid_deformation_fields_df.drop_duplicates(subset=col_subset, ignore_index=True)

    return grid_deformation_fields_df


def get_specific_meta_field(df, col_subset, nomvar, grid, dateo, deet, npas):
    subset = col_subset.copy()
    # try very strict match
    field_df = df.loc[
        (df.nomvar == nomvar) & (df.grid == grid) & (df.dateo == dateo) & (df.deet == deet) & (df.npas == npas)
    ]

    if field_df.empty:
        # try a strict match
        field_df = df.loc[(df.nomvar == nomvar) & (df.grid == grid) & (df.dateo == dateo)]
        if field_df.empty:
            # try a loose match
            field_df = df.loc[(df.nomvar == nomvar) & (df.grid == grid)]
            if not field_df.empty:
                # we found something on loose match - remove the duplicates
                subset.remove("deet")
                subset.remove("npas")
                subset.remove("dateo")
                field_df = field_df.drop_duplicates(subset=subset)
        else:
            # we found something on strict match - remove the duplicates
            subset.remove("deet")
            subset.remove("npas")
            field_df = field_df.drop_duplicates(subset=subset)
    else:
        # we found something on very strict match - remove the duplicates
        field_df = field_df.drop_duplicates(subset=subset)
    return field_df


def get_p0_fields(df: pd.DataFrame, no_meta_df: pd.DataFrame, hybrid_ips: list, sigma_ips: list) -> pd.DataFrame:
    """
    Decide quels P0 seront conserves, selons les champs presents et types de coordonnees verticales.

    Description:
        Scenarios:
            1. Niveaux hybrid:
                a. Si 1.5M ou 10M present (hybrid 5005), on garde P0
                b. Si champs HY present, on garde P0
                c. Si on a un toctoc avec le bon ig1 (correspondant au kind 5), on conserve le P0
                d. Si on a un toctoc avec un ig1 ne faisant pas partie de la liste de tous les cas possibles de
                ig1, c'est donc un vieux !! et on conserve le P0
            2. Niveaux sigma:
                a. Si on un P0 correspondant a la grille, dateo et datev d'un champs, on garde le P0

    :param df: The DataFrame containing meta and non meta data.
    :type df: pd.DataFrame
    :param no_meta_df: DataFrame without metadata information.
    :type no_meta_df: pd.DataFrame
    :param hybrid_ips: List of hybrid levels in the DataFrame.
    :type hybrid_ips: list
    :param sigma_ips: List of sigma levels in the DataFrame.
    :type sigma_ips: list
    :return: A DataFrame containing P0 fields.
    :rtype: pd.DataFrame
    """
    mask_p0 = df.nomvar == "P0"
    p0_df = df.loc[mask_p0]
    other_df = df.loc[~mask_p0]
    p0_fields_df = pd.DataFrame(dtype=object)

    df_list = []

    # check for P0 associated with 5005 surface level
    ip1_1_5M = RmnInterface.convert_ip(RmnInterface.CONVIP_ENCODE, 1.5, 4)
    ip1_10M = RmnInterface.convert_ip(RmnInterface.CONVIP_ENCODE, 10, 4)
    for p0 in p0_df.iterrows():
        # Make sure it only compares the same grid for the same time
        grid = p0[1]["grid"]
        dateo = p0[1]["dateo"]
        datev = p0[1]["datev"]

        p0_work = p0[1].to_frame().T
        # Conversion des colonnes a boolean pour eviter warning "object-dtype columns with all-bool values ..."
        boolean_cols = ["surface", "ascending", "follow_topography"]
        p0_work = convert_cols_to_boolean_dtype(p0_work, boolean_cols)

        # Get list of ip1 on the same grid at the same time
        list_ip1_df = other_df.loc[
            (other_df.grid == grid) & (other_df.dateo == dateo) & (other_df.datev == datev), "ip1"
        ].values

        # Check if 1.5M or 10M is in the list (hybrid 5005) Note: Difference avec spooki CPP qui ne conserve pas le P0
        if ip1_1_5M in list_ip1_df or ip1_10M in list_ip1_df:
            df_list.append(p0_work)
            if p0_fields_df.empty:
                p0_fields_df = p0_work
            else:
                p0_fields_df = pd.safe_concat([p0_fields_df, p0_work])

    hybrid_grids = set(no_meta_df.loc[no_meta_df["ip1"].isin(hybrid_ips)]["grid"])

    hy_df = get_hy_field(df, hybrid_ips)
    toctoc_df = df.loc[df.nomvar == "!!"]
    # Si champs HY est present, on conserve P0
    # Sinon si on a un toctoc avec le bon ig1 (correspondant au kind 5), on conserve le P0
    # Si on a un toctoc avec un ig1 ne faisant pas partie de la liste de tous les cas possibles de
    # ig1, c'est donc un vieux !! et on conserve le P0
    for grid in hybrid_grids:
        ni = no_meta_df.loc[no_meta_df.grid == grid].ni.unique()[0]
        nj = no_meta_df.loc[no_meta_df.grid == grid].nj.unique()[0]

        # Champs HY present
        if not hy_df.empty:
            combined_filter = (p0_df.grid == grid) & (p0_df.ni == ni) & (p0_df.nj == nj)
            unique_date_pairs = set(
                zip(no_meta_df.loc[no_meta_df.grid == grid, "dateo"], no_meta_df.loc[no_meta_df.grid == grid, "datev"])
            )
            for dateo, datev in unique_date_pairs:
                filtered_p0_df = p0_df.loc[combined_filter & (p0_df.dateo == dateo) & (p0_df.datev == datev)]
                if not filtered_p0_df.empty:
                    df_list.append(filtered_p0_df)
        else:
            if not toctoc_df.empty:
                all_igs_list = [1001, 1002, 1003, 2001, 4001, 5001, 5002, 5003, 5004, 5005, 5100, 5999, 21001, 21002]
                hy_igs_list = [1003, 5001, 5002, 5003, 5004, 5005, 5100, 5999, 21001, 21002]
                same_grid = toctoc_df.grid == grid

                hyb_toctoc_df = toctoc_df.loc[same_grid & (toctoc_df.ig1.isin(hy_igs_list))]
                old_toctoc_df = toctoc_df.loc[same_grid & ~(toctoc_df.ig1.isin(all_igs_list))]

                if not hyb_toctoc_df.empty or not old_toctoc_df.empty:
                    filtered_p0_df = p0_df.loc[(p0_df.grid == grid) & (p0_df.ni == ni) & (p0_df.nj == nj)]
                    # Check for emptiness before appending to avoid problem with boolean cols
                    if not filtered_p0_df.empty:
                        df_list.append(filtered_p0_df)

    sigma_grids = set()
    for ip1 in sigma_ips:
        matching_rows = no_meta_df[no_meta_df["ip1"] == ip1]
        if not matching_rows.empty:
            grids = set(matching_rows["grid"])
            for grid in grids:
                sigma_grids.add(grid)

    for grid in sigma_grids:
        ni = no_meta_df.loc[no_meta_df.grid == grid].ni.unique()[0]
        nj = no_meta_df.loc[no_meta_df.grid == grid].nj.unique()[0]

        combined_filter = (p0_df.grid == grid) & (p0_df.ni == ni) & (p0_df.nj == nj)

        unique_date_pairs = set(
            zip(no_meta_df.loc[no_meta_df.grid == grid, "dateo"], no_meta_df.loc[no_meta_df.grid == grid, "datev"])
        )
        for dateo, datev in unique_date_pairs:
            filtered_p0_df = p0_df.loc[combined_filter & (p0_df.dateo == dateo) & (p0_df.datev == datev)]
            # Check for emptiness before appending to avoid problem with boolean cols
            if not filtered_p0_df.empty:
                df_list.append(filtered_p0_df)

    if len(df_list) > 0:
        p0_fields_df = safe_concatenate(df_list)

    p0_fields_df.drop_duplicates(
        subset=[
            "grtyp",
            "nomvar",
            "typvar",
            "ni",
            "nj",
            "nk",
            "ip1",
            "ip2",
            "ip3",
            "deet",
            "npas",
            "nbits",
            "ig1",
            "ig2",
            "ig3",
            "ig4",
            "datev",
            "dateo",
            "datyp",
        ],
        inplace=True,
        ignore_index=True,
    )

    return p0_fields_df


def get_pt_fields(df: pd.DataFrame, no_meta_df: pd.DataFrame, sigma_ips: list) -> pd.DataFrame:
    """
    Cherche si on a un ou plusieurs champs PT associe(s) aux niveaux sigma du dataframe (meme grid, dateo et datev);
    si oui on conserve ces champs PT.  Les doublons sont supprimes.

    :param df: DataFrame
    :type df: pd.DataFrame
    :param no_meta_df: DataFrame without metadata information.
    :type no_meta_df: pd.DataFrame
    :param sigma_ips: List of sigma levels in the DataFrame.
    :type sigma_ips: list
    :return: DataFrame containing appropriate PT
    :rtype: pd.DataFrame
    """
    pt_df = df.loc[df.nomvar == "PT"]

    pt_fields_df = pd.DataFrame(dtype=object)

    sigma_grids = set()
    for ip1 in sigma_ips:
        matching_rows = no_meta_df[no_meta_df["ip1"] == ip1]
        if not matching_rows.empty:
            grids = set(matching_rows["grid"])
            for grid in grids:
                sigma_grids.add(grid)

    df_list = []
    for grid in list(sigma_grids):
        ni = no_meta_df.loc[no_meta_df.grid == grid].ni.unique()[0]
        nj = no_meta_df.loc[no_meta_df.grid == grid].nj.unique()[0]

        combined_filter = (pt_df.grid == grid) & (pt_df.ni == ni) & (pt_df.nj == nj)

        unique_date_pairs = set(
            zip(no_meta_df.loc[no_meta_df.grid == grid, "dateo"], no_meta_df.loc[no_meta_df.grid == grid, "datev"])
        )
        for dateo, datev in unique_date_pairs:
            filtered_pt_df = pt_df.loc[combined_filter & (pt_df.dateo == dateo) & (pt_df.datev == datev)]
            # Check for emptiness before appending to avoid problem with boolean cols
            if not filtered_pt_df.empty:
                df_list.append(filtered_pt_df)

    if len(df_list):
        pt_fields_df = pd.safe_concat(df_list)

    pt_fields_df.drop_duplicates(
        subset=[
            "grtyp",
            "nomvar",
            "typvar",
            "ni",
            "nj",
            "nk",
            "ip1",
            "ip2",
            "ip3",
            "deet",
            "npas",
            "nbits",
            "ig1",
            "ig2",
            "ig3",
            "ig4",
            "datev",
            "dateo",
            "datyp",
        ],
        inplace=True,
        ignore_index=True,
    )

    return pt_fields_df


def convert_cols_to_boolean_dtype(df: pd.DataFrame, list_of_cols=None) -> pd.DataFrame:
    """Convert columns to dtype boolean

    :param df          : the dataFrame from which to convert columns
    :type df           : pd.DataFrame
    :param list_of_cols: a list of columns to convert
    :type list_of_cols : list
    """
    if list_of_cols is None:
        bool_cols = [
            "surface",
            "follow_topography",
            "ascending",
            "masks",
            "masked",
            "multiple_modifications",
            "zapped",
            "filtered",
            "interpolated",
            "unit_converted",
            "bounded",
            "missing_data",
            "ensemble_extra_info",
        ]
    else:
        bool_cols = list_of_cols

    # Ensure all specified columns exist in the DataFrame
    valid_cols = [col for col in bool_cols if col in df.columns]

    # Conversion des colonnes a boolean pour eviter warning "object-dtype columns with all-bool values ..."
    for col_name in valid_cols:
        # Check if the column name is in the list
        if col_name in df.columns:
            # print(f'\n\n col_name = {col_name}  {df[[col_name]].dtypes} \n\n')
            df[[col_name]] = df[[col_name]].applymap(lambda x: x if pd.notna(x) else False)
            df[[col_name]] = df[[col_name]].astype("bool")

    return df


def check_column_dtype(df: pd.DataFrame, list_of_cols=None) -> None:
    """Check columns dtype

    :param df          : the dataFrame from which to check columns
    :type df           : pd.DataFrame
    :param list_of_cols: a list of columns to check
    :type list_of_cols : list
    """
    if list_of_cols is None:
        bool_cols = [
            "surface",
            "follow_topography",
            "ascending",
            "masks",
            "masked",
            "multiple_modifications",
            "zapped",
            "filtered",
            "interpolated",
            "unit_converted",
            "bounded",
            "missing_data",
            "ensemble_extra_info",
        ]
    else:
        bool_cols = list_of_cols

    # Ensure all specified columns exist in the DataFrame
    valid_cols = [col for col in bool_cols if col in df.columns]
    print(f"Colonnes valides:  {valid_cols=}")

    # Verification si les colonnes sont de type object au lieu de boolean
    for col_name in valid_cols:
        # Check if the column name is in the list
        if col_name in df.columns:
            series = df[col_name]

            if series.dtype == "object":
                print(f"Column '{col_name}' has dtype 'object'")
            elif series.dtype == "bool":
                print(f"Column '{col_name}' has dtype 'boolean'")
            else:
                print(f"Column '{col_name}' has dtype {series.dtype}")


def print_df_debug(df: pd.DataFrame, max_cols=True, max_rows=True, max_colwidth=False):
    """
    Prints a DataFrame with enhanced visibility for debugging purposes.

    This function configures pandas to display all available columns,
    all available rows, and allows for the full display of long strings.

    After calling this function, pandas will print the DataFrame object with:
    - All columns displayed
    - All rows displayed
    - Long strings shown in full, without truncation

    :param df: pandas DataFrame to print
    :type df: pd.DataFrame
    :param max_cols: Whether to display all columns
    :type max_cols: bool
    :param max_rows: Whether to display all rows
    :type max_rows: bool
    :param max_colwidth: Whether to show full width of columns
    :type max_colwidth: bool
    :return: None
    :rtype: None
    """
    # Store original options
    original_max_cols = pd.get_option("display.max_columns")
    original_max_rows = pd.get_option("display.max_rows")
    original_max_colwidth = pd.get_option("display.max_colwidth")

    # Set new options
    set_pandas_debug_options(max_cols, max_rows, max_colwidth)

    print(df)

    # Restore original options
    pd.set_option("display.max_columns", original_max_cols)
    pd.set_option("display.max_rows", original_max_rows)
    pd.set_option("display.max_colwidth", original_max_colwidth)


def set_pandas_debug_options(max_cols=True, max_rows=True, max_colwidth=False):
    """
    Sets pandas print options to maximize visibility for debugging purposes.

    This function configures pandas to display all available columns,
    all available rows, and allows for the full display of long strings.

    After calling this function, pandas will print DataFrame objects with:
    - All columns displayed
    - All rows displayed
    - Long strings shown in full, without truncation

    Note: These settings are applied globally and persist until changed again.
           They may affect performance when working with large datasets.

    :return: None
    :rtype: None

    Example usage:
        >>> import pandas as pd
        >>> df = pd.DataFrame({'A': [1, 2], 'B': ['hello', 'world']})
        >>> set_pandas_debug_options()
        >>> print(df)
    """

    if max_cols:
        pd.set_option("display.max_columns", None)
    if max_rows:
        pd.set_option("display.max_rows", None)
    if max_colwidth:
        pd.set_option("display.max_colwidth", None)
