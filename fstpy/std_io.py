# -*- coding: utf-8 -*-
import copy
import datetime
import logging
import multiprocessing as mp
import os.path
import pathlib


from typing import Dict, List, Tuple, Union, Type

import numpy as np
import pandas as pd
from dask import array as da

from . import _LOCK
from .rmn_interface import RmnInterface


def get_dataframe_from_file(path: str, query: str = None):
    from .dataframe import add_grid_column

    df = get_basic_dataframe(path)

    df = add_grid_column(df)

    hy_df = df.loc[df.nomvar == "HY"]

    df = df.loc[df.nomvar != "HY"]

    if not (query is None):
        query_result_df = df.query(query)

        # get metadata
        df = add_metadata_to_query_results(df, query_result_df, hy_df)

    # check HY count
    df = process_hy(hy_df, df)

    df = add_dask_column(df)

    df = df.drop(["key", "path", "shape", "swa", "lng"], axis=1, errors="ignore")

    return df


def open_fst(path: str, mode: str, caller_class: str, error_class: Type):
    file_id = RmnInterface.open_file(path, mode)
    logging.info(f"{caller_class} - opening file {path}")
    return file_id


def close_fst(file_id: int, path: str, caller_class: str):
    logging.info(f"{caller_class} - closing file {path}")
    RmnInterface.close_file(file_id)


def add_metadata_to_query_results(df, query_result_df, hy_df) -> pd.DataFrame:
    if df.empty:
        return df
    meta_df = df.loc[df.nomvar.isin(["^>", ">>", "^^", "!!", "!!SF", "P0", "PT", "E1"])]

    query_result_metadata_df = meta_df.loc[meta_df.grid.isin(list(query_result_df.grid.unique()))]

    if (not query_result_df.empty) and (not query_result_metadata_df.empty):
        df = pd.safe_concat([query_result_df, query_result_metadata_df])
    elif (not query_result_df.empty) and (query_result_metadata_df.empty):
        df = query_result_df
    elif query_result_df.empty:
        df = query_result_df

    if (not df.empty) and (not hy_df.empty):
        df = pd.safe_concat([df, hy_df])

    return df


def process_hy(hy_df: pd.DataFrame, df: pd.DataFrame) -> pd.DataFrame:
    """Assign HY to every grid with hybrid coordinates except if toctoc is present for the grid;
       add HY to the dataframe and set its grid.

    :param hy_df: dataframe of all hy fields
    :type hy_df: pd.DataFrame
    :param df: original dataframe without hy
    :type df: pd.DataFrame
    :return: modified dataframe with one HY field
    :rtype: pd.DataFrame
    """
    if hy_df.empty or df.empty:
        return df

    # On prend le 1er HY car de toute facon on ne peut pas determiner a quelle grille
    # les HY sont associes
    hy_df = pd.DataFrame([hy_df.iloc[0].to_dict()])

    # Group by 'grid' and apply a function to each group
    df = df.groupby("grid", group_keys=True).apply(lambda group: assign_hy(group, hy_df)).reset_index(drop=True)

    return df


def assign_hy(grid_df: pd.DataFrame, hy_df: pd.DataFrame) -> pd.DataFrame:
    """Assign HY to a group of data when appropriate."""

    from .dataframe_utils import get_hybrid_ips

    hybrid_ips = get_hybrid_ips(grid_df)

    if len(hybrid_ips):
        df_with_hyb_levels = grid_df.loc[grid_df.ip1.isin(hybrid_ips)]

        hyb_levels_grid = df_with_hyb_levels.grid.unique()

        toctoc_df = grid_df.loc[grid_df.nomvar == "!!"]

        # Est-ce que le toctoc est associe aux niveaux hybrid?
        for grid in hyb_levels_grid:
            hyb_toctoc_df = toctoc_df.loc[
                (toctoc_df.grid == grid)
                & (toctoc_df.ig1.isin([5001, 5002, 5003, 5004, 5005, 5100, 5999, 21001, 21002]))
            ]

            # Pas de toctoc pour la grille, on ajoute un HY
            if hyb_toctoc_df.empty:
                hy_df["grid"] = grid
                grid_df = pd.safe_concat([grid_df, hy_df])

    return grid_df


# written by Micheal Neish creator of fstd2nc
# Lightweight test for FST files.
# Uses the same test for fstd98 random files from wkoffit.c (librmn 16.2).
#
# The 'isFST' test from rpnpy calls c_wkoffit, which has a bug when testing
# many small (non-FST) files.  Under certain conditions the file handles are
# not closed properly, which causes the application to run out of file handles
# after testing ~1020 small non-FST files.


def maybeFST(filename: Union[str, pathlib.Path]) -> bool:
    """Lightweight test to check if file is of FST type (Micheal Neish - fstd2nc)

    :param filename: file to test
    :type filename: Union[str, pathlib.Path]
    :return: True if isfile and of FST type, else False
    :rtype: bool
    """
    if not os.path.isfile(filename):
        return False
    with open(filename, "rb") as f:
        buf = f.read(16)
        if len(buf) < 16:
            return False
        # Same check as c_wkoffit in librmn
        return buf[12:] == b"STDR"


def get_data(path, key, dtype, shape, cache={}):
    with _LOCK:
        # Check if file needs to be opened.
        # if path not in cache:
        # Allow for a small number of files to remain open for speedier access.
        # if len(cache) > 10:
        # for _, unit in cache.items():
        # RmnInterface.close_file(unit)
        # cache.clear()
        iun = RmnInterface.open_file(path)
        # iun = cache[path]
        data = RmnInterface.read_record(key)["d"]
        RmnInterface.close_file(iun)
        return data


def add_dask_column(df: pd.DataFrame) -> pd.DataFrame:
    """Adds the 'd' column as dask arrays to a basic dataframe of meta data only, path and key columns have to be present in the DataFrame

    :param df: input dataframe
    :type df: pd.DataFrame
    :return: modified Dataframe with added 'd' column
    :rtype: pd.DataFrame
    """
    if df.empty:
        return df
    arrays = []
    for row in df.itertuples():
        path = row.path
        key = row.key
        datyp = row.datyp
        nbits = row.nbits
        name = "".join([path, ":", str(key)])
        shape = row.shape
        dtype = RmnInterface.get_numpy_dtype(datyp, nbits, row.ni, row.nk)
        dsk = {(name, 0, 0): (get_data, path, key, dtype, shape)}
        chunks = [(s,) for s in shape]
        arrays.append(da.Array(dsk, name, chunks, dtype))
    d = np.zeros(len(arrays), dtype=object)
    for i in range(len(d)):
        d[i] = arrays[i]
    df["d"] = d
    return df


class GetBasicDataFrameError(Exception):
    pass


def get_basic_dataframe(path: str) -> pd.DataFrame:
    file_id = RmnInterface.open_file(path)
    keys = RmnInterface.find_records(file_id)
    records = [RmnInterface.get_record_metadata(key) for key in keys]
    df = pd.DataFrame(records)
    df["d"] = None
    df["path"] = path
    RmnInterface.close_file(file_id)

    df = df.loc[df.dltf == 0]
    df = df.drop(labels=["dltf", "ubc"], axis=1)

    df["shape"] = df["shape"].apply(lambda x: x[:2] if len(x) == 3 and x[2] == 1 else x)
    df["nomvar"] = df["nomvar"].str.strip()
    df["etiket"] = df["etiket"].str.strip()
    df["typvar"] = df["typvar"].str.strip()
    df["grtyp"] = df["grtyp"].str.strip()

    df = df[
        [
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
            "datyp",
            "nbits",
            "grtyp",
            "ig1",
            "ig2",
            "ig3",
            "ig4",
            "datev",
            "lng",
            "swa",
            "key",
            "path",
            "shape",
        ]
    ]

    # Convert int64 columns to int32
    int_columns = [
        "ni",
        "nj",
        "nk",
        "dateo",
        "ip1",
        "ip2",
        "ip3",
        "deet",
        "npas",
        "datyp",
        "nbits",
        "ig1",
        "ig2",
        "ig3",
        "ig4",
        "datev",
        "lng",
        "swa",
        "key",
    ]
    df[int_columns] = df[int_columns].astype("int32")

    return df


class DecodeIpError(Exception):
    pass


def decode_ip123(nomvar: str, ip1: int, ip2: int, ip3: int) -> Tuple[dict, dict, dict]:
    ip_info = {"v1": 0.0, "kind": -1, "kinds": ""}

    if nomvar in [">>", "^^", "^>", "!!"]:
        ip1_info = copy.deepcopy(ip_info)
        ip1_info["v1"] = float(ip1)
        ip1_info["kind"] = 100

        ip2_info = copy.deepcopy(ip_info)
        ip2_info["v1"] = float(ip2)
        ip2_info["kind"] = 100

        ip3_info = copy.deepcopy(ip_info)
        ip3_info["v1"] = float(ip3)
        ip3_info["kind"] = 100

    else:
        ip1_info = copy.deepcopy(ip_info)
        ip1_info["v1"], ip1_info["kind"] = RmnInterface.convert_ip(RmnInterface.CONVIP_DECODE, ip1)
        ip1_info["kinds"] = RmnInterface.kind_to_string(ip1_info["kind"])

        ip2_info = copy.deepcopy(ip_info)
        ip2_info["v1"], ip2_info["kind"] = RmnInterface.convert_ip(RmnInterface.CONVIP_DECODE, ip2)
        ip2_info["kinds"] = RmnInterface.kind_to_string(ip2_info["kind"])
        if ip2 >= 32768:  # Verifie si IP2 est encode
            if ip2_info["kind"] != 10:
                raise DecodeIpError(f"Invalid kind value for ip2 {ip2_info['kind']} != 10")
        else:
            ip2_info["kind"] = 10
            ip2_info["kinds"] = RmnInterface.kind_to_string(ip2_info["kind"])

        ip3_info = copy.deepcopy(ip_info)
        ip3_info["v1"], ip3_info["kind"] = RmnInterface.convert_ip(RmnInterface.CONVIP_DECODE, ip3)
        ip3_info["kinds"] = RmnInterface.kind_to_string(ip3_info["kind"])
        if ip3 < 32768:  # Verifie si IP3 est encode
            ip3_info["kind"] = 100
            ip3_info["kinds"] = RmnInterface.kind_to_string(ip3_info["kind"])

        if nomvar not in [">>", "^^", "^>", "!!", "HY", "P0", "PT"]:
            # Nous n'avons pas de champs speciaux
            if ip3 >= 32768:
                if ip3_info["kind"] == ip2_info["kind"]:  # On a un intervalle de temps
                    v1 = ip3_info["v1"]
                    v2 = ip2_info["v1"]
                    # Borne superieure de l'intervalle de temps est dans le ip2
                    # Borne inferieure de l'intervalle de temps est dans le ip3
                    ip2_info["v1"] = v2  # Borne sup
                    ip2_info["v2"] = v1  # Borne inf
                elif ip3_info["kind"] == ip1_info["kind"]:  # On a un intervalle sur les hauteurs
                    v1 = ip1_info["v1"]
                    v2 = ip3_info["v1"]
                    ip1_info["v1"] = v1
                    ip1_info["v2"] = v2

    return ip1_info, ip2_info, ip3_info
