# -*- coding: utf-8 -*-
import ctypes
import logging
import math
from typing import Tuple, List
from .utils import to_numpy

import numpy as np
import pandas as pd
from .rmn_interface import RmnInterface

from .dataframe import add_path_and_key_columns


def check_grid_equality(input_grid: int, output_grid: int) -> bool:
    """
    Compare the parameters of two grids to determine if they are equal,
    excluding the 'id' parameter.

    :param input_grid: The original grid to be compared.
    :type input_grid: int
    :param output_grid: The grid to compare against the input grid.
    :type output_grid: int
    :return: True if the grids are equal, False otherwise.
    :rtype: bool

    """
    in_params = RmnInterface.get_grid_parameters(input_grid)
    in_params.pop("id")
    out_params = RmnInterface.get_grid_parameters(output_grid)
    out_params.pop("id")

    return in_params == out_params


def get_df_from_grid(grid_params: dict) -> pd.DataFrame:
    """For grid types Z, Y or U, produces a DataFrame of >>,^^ or ^> fields

    :param grid_params: horizontal grid definition dictionnary
    :type grid_params: dict
    :return: DataFrame of >>,^^ or ^> fields
    :rtype: pd.DataFrame

    """
    g = grid_params
    if (g["grtyp"] == "Z") or (g["grtyp"] == "Y"):
        meta_df = pd.DataFrame(
            [
                {
                    "nomvar": ">>",
                    "typvar": "X",
                    "etiket": "",
                    "ni": g["ni"],
                    "nj": 1,
                    "nk": 1,
                    "dateo": 0,
                    "ip1": g["ig1"],
                    "ip2": g["ig2"],
                    "ip3": 0,
                    "deet": 0,
                    "npas": 0,
                    "datyp": 5,
                    "nbits": 32,
                    "grtyp": g["grref"],
                    "ig1": g["ig1ref"],
                    "ig2": g["ig2ref"],
                    "ig3": g["ig3ref"],
                    "ig4": g["ig4ref"],
                    "datev": 0,
                    "d": g["ax"],
                },
                {
                    "nomvar": "^^",
                    "typvar": "X",
                    "etiket": "",
                    "ni": 1,
                    "nj": g["nj"],
                    "nk": 1,
                    "dateo": 0,
                    "ip1": g["ig1"],
                    "ip2": g["ig2"],
                    "ip3": 0,
                    "deet": 0,
                    "npas": 0,
                    "datyp": 5,
                    "nbits": 32,
                    "grtyp": g["grref"],
                    "ig1": g["ig1ref"],
                    "ig2": g["ig2ref"],
                    "ig3": g["ig3ref"],
                    "ig4": g["ig4ref"],
                    "datev": 0,
                    "d": g["ay"],
                },
            ]
        )

    elif g["grtyp"] == "U":
        meta_df = pd.DataFrame(
            [
                {
                    "nomvar": "^>",
                    "typvar": "X",
                    "etiket": "",
                    "ni": g["axy"].shape[0],
                    "nj": 1,
                    "nk": 1,
                    "dateo": 0,
                    "ip1": g["ig1"],
                    "ip2": g["ig2"],
                    "ip3": 0,
                    "deet": 0,
                    "npas": 0,
                    "datyp": 5,
                    "nbits": 32,
                    "grtyp": g["grref"],
                    "ig1": g["ig1ref"],
                    "ig2": g["ig2ref"],
                    "ig3": g["ig3ref"],
                    "ig4": g["ig4ref"],
                    "datev": 0,
                    "d": g["axy"],
                }
            ]
        )
    else:
        meta_df = pd.DataFrame(dtype=object)
    return meta_df


class GridDefinitionError(Exception):
    pass


def create_grid_set(input_grid: int, output_grid: int) -> None:
    """
    Defines a grid set by copying definitions from an input grid to an output grid.

    :param input_grid: The input grid from which definitions are copied.
    :type input_grid: int
    :param output_grid: The output grid where definitions are defined.
    :type output_grid: int

    """
    RmnInterface.create_grid_set(output_grid, input_grid)


def define_sub_grid_u(start_position, yy):
    pass


def get_grid_definition_params(df):
    """
    Extracts grid definition parameters from a given DataFrame and returns a structure containing the infos for the grid.

    This function processes a DataFrame to ensure it meets certain criteria for grid definitions,
    such as having unique paths and grids, and then extracts relevant parameters like `grtyp`,
    `ni`, `nj`, and `path`. Depending on the `grtyp` value, it performs different operations
    to determine the grid ID, including handling special cases for certain grid types.

    :param df: The DataFrame containing grid parameters.
    :type df: pd.DataFrame

    """
    if df.empty:
        raise GridDefinitionError("Empty DataFrame!")

    if "path" not in df.columns:
        df = add_path_and_key_columns(df)

    if df.path.unique().size > 1:
        raise GridDefinitionError("More than one path in DataFrame!")

    if df.grid.unique().size > 1:
        raise GridDefinitionError("More than one grid in DataFrame!")

    no_meta_df = df.loc[~df.nomvar.isin(["^^", ">>", "^>", "!!", "!!SF", "HY", "P0", "PT"])]
    if no_meta_df.grtyp.unique().size > 1:
        raise GridDefinitionError("More than one grtyp in DataFrame!")

    grtyp = no_meta_df.grtyp.unique()[0]
    ni = no_meta_df.ni.unique()[0]
    nj = no_meta_df.nj.unique()[0]
    path = no_meta_df.path.unique()[0]

    grid_id = -1
    grid_types = ["A", "B", "E", "G", "L", "N", "S", "U", "X", "Y", "Z", "#"]

    if grtyp not in grid_types:
        logging.error(f"{grtyp} Grid type is not supported")
    else:
        cgrtyp = ctypes.c_char_p(grtyp.encode("utf-8"))

        if (path is not None) and ((grtyp == "Y") or (grtyp == "Z") or (grtyp == "#") or (grtyp == "U")):
            file_id = RmnInterface.open_file(df.path.unique()[0], RmnInterface.FST_RO)
            try:
                rec = RmnInterface.get_record_metadata(int(no_meta_df.iloc[0].key))
            except Exception as e:
                RmnInterface.close_file(file_id)
                raise GridDefinitionError("Error while getting record data!")
            try:
                grid_id = RmnInterface.read_grid(file_id, rec)
            except Exception as e:
                RmnInterface.close_file(file_id)
                raise GridDefinitionError("Error while reading grid!")
            RmnInterface.close_file(file_id)
            return grid_id

        elif (grtyp == "Y") or (grtyp == "Z") or (grtyp == "#"):
            tictic_tactac_df = df.loc[df.nomvar.isin(["^^", ">>", "^>"])]
            ig1 = tictic_tactac_df.ig1.mode().iloc[0]
            ig2 = tictic_tactac_df.ig2.mode().iloc[0]
            ig3 = tictic_tactac_df.ig3.mode().iloc[0]
            ig4 = tictic_tactac_df.ig4.mode().iloc[0]
            grref = df.loc[df.nomvar.isin(["^^", ">>", "^>"]), "grtyp"].mode().iloc[0]
            cgrref = ctypes.c_char_p(grref.encode("utf-8"))
            lat = df.loc[df.nomvar == ">>"].reset_index().at[0, "d"]
            lon = df.loc[df.nomvar == "^^"].reset_index().at[0, "d"]

            if type(lat) != np.ndarray:
                lat = np.asarray(lat)
            if type(lon) != np.ndarray:
                lon = np.asarray(lon)

            grid_id = RmnInterface.proto_define_grid_fmem(ni, nj, cgrtyp, cgrref, ig1, ig2, ig3, ig4, lat, lon)

        elif grtyp == "U":
            grref = df.loc[~df.nomvar == "^>", "grtyp"].mode().iloc[0]
            vercode = 1
            nsubgrids = 2
            next_position = 5
            yy = df.loc[df.nomvar == "^^", "d"]
            subgrid_id1, next_position, grid_ni, grid_nj = define_sub_grid_u(next_position, yy)
            subgrid_id2, next_position, grid_ni, grid_nj = define_sub_grid_u(next_position, yy)
            subgrid_ids = [subgrid_id1, subgrid_id2]

            grid_id = RmnInterface.proto_define_supergrid(
                grid_ni, grid_nj, cgrtyp, grref, vercode, nsubgrids, subgrid_ids
            )

        else:
            ig1 = no_meta_df.ig1.mode().iloc[0]
            ig2 = no_meta_df.ig2.mode().iloc[0]
            ig3 = no_meta_df.ig3.mode().iloc[0]
            ig4 = no_meta_df.ig4.mode().iloc[0]
            grid_id = RmnInterface.proto_define_grid(ni, nj, cgrtyp, ig1, ig2, ig3, ig4, 0)

        # Code necessaire; les fonctions suivantes creent un structure contenant l'information pour la grille.
        try:
            grid_id = RmnInterface.decode_grid(grid_id)
        except Exception as e:
            grid_id = RmnInterface.get_grid_parameters(grid_id)

    return grid_id


def define_input_grid(grtyp: str, source_df: pd.DataFrame, meta_df: pd.DataFrame) -> tuple:
    """
    Define the input grid based on grid type and metadata.

    This function sets grid parameters based on the input grid type and metadata.
    It returns the input grid and, depending on the metadata, additional subgrids.

    :param grtyp: The type of the grid to be defined.
    :type grtyp: str
    :param source_df: The source DataFrame containing grid parameters.
    :type source_df: pd.DataFrame
    :param meta_df: The metadata DataFrame containing additional grid information.
    :type meta_df: pd.DataFrame
    :return: A tuple containing the input grid and, depending on the metadata, additional subgrids.
    :rtype: tuple
    """

    if not meta_df.empty:
        if (">>" in meta_df.nomvar.to_list()) and ("^^" in meta_df.nomvar.to_list()):
            (ni, nj, grref, ax, ay, ig1, ig2, ig3, ig4) = get_grid_parameters_from_positional_records(meta_df)
            infos_grid = define_grid(grtyp, grref, ni, nj, ig1, ig2, ig3, ig4, ax, ay, None)
            input_grid, *other = infos_grid

            return (input_grid,)

        elif "^>" in meta_df.nomvar.to_list():
            infos_grid = define_u_grid(meta_df, grtyp)
            if len(infos_grid) != 3:
                raise GridDefinitionError(f"Problem with definition of grid of type U")

            input_grid, subgrid1, subgrid2 = infos_grid
            return input_grid, subgrid1, subgrid2

    else:
        ni, nj, ig1, ig2, ig3, ig4 = set_grid_parameters(source_df)
        infos_grid = define_grid(grtyp, " ", ni, nj, ig1, ig2, ig3, ig4, None, None, None)

        input_grid, *other = infos_grid

        return (input_grid,)


def define_grid(
    grtyp: str,
    grref: str,
    ni: int,
    nj: int,
    ig1: int,
    ig2: int,
    ig3: int,
    ig4: int,
    ax: np.ndarray,
    ay: np.ndarray,
    tictac: np.ndarray,
) -> tuple:
    """
    Defines a grid based on the provided grid type and parameters.

    This function creates a grid of a specified type (grtyp) with given dimensions and indices.
    It supports various grid types, including 'A', 'B', 'E', 'G', 'L', 'N', 'S', 'U', 'X', 'Y', 'Z', and '#'.
    For grid types 'Y', 'Z', and '#', it uses the ezgdef_fmem function from the rmn module to define the grid.
    For grid type 'U', it creates sub-grids and uses the ezgdef_supergrid function from the rmn module.

    :param grtyp: The grid type, one of 'A', 'B', 'E', 'G', 'L', 'N', 'S', 'U', 'X', 'Y', 'Z', '#'.
    :type grtyp: str
    :param grref: The grid reference.
    :type grref: str
    :param ni: The number of grid points in the i-direction.
    :type ni: int
    :param nj: The number of grid points in the j-direction.
    :type nj: int
    :param ig1: The first index for the grid.
    :type ig1: int
    :param ig2: The second index for the grid.
    :type ig2: int
    :param ig3: The third index for the grid.
    :type ig3: int
    :param ig4: The fourth index for the grid.
    :type ig4: int
    :param ax: The x-coordinates of the grid points.
    :type ax: np.ndarray
    :param ay: The y-coordinates of the grid points.
    :type ay: np.ndarray
    :param tictac: The tic-tac array for grid type 'U'.
    :type tictac: np.ndarray
    :return: For grid types 'Y', 'Z', '#', returns a tuple containing the grid ID.
             For grid type 'U', returns a tuple containing the grid ID and the IDs of the sub-grids.
    :rtype: tuple
    :raises GridUtilsError: If the grid type is not recognized.
    """

    grid_types = ["A", "B", "E", "G", "L", "N", "S", "U", "X", "Y", "Z", "#"]
    grid_id = -1

    if grtyp not in grid_types:
        raise GridDefinitionError(f"Grtyp {grtyp} not in {grid_types}")

    if grtyp in ["Y", "Z", "#"]:
        grid_params = {
            "grtyp": grtyp,
            "grref": grref,
            "ni": int(ni),
            "nj": int(nj),
            "ay": ay,
            "ax": ax,
            "ig1": int(ig1),
            "ig2": int(ig2),
            "ig3": int(ig3),
            "ig4": int(ig4),
        }
        grid_id = RmnInterface.define_grid_fmem(grid_params)

        return (grid_id,)

    elif grtyp == "U":
        ni, nj, sub_grid_id_1, sub_grid_id_2 = define_sub_grids_u(tictac)

        vercode = 1
        grtyp = "U"
        grref = ""
        grid_params = {
            "grtyp": grtyp,
            "grref": grref,
            "ni": int(ni),
            "nj": int(2 * nj),
            "vercode": vercode,
            "subgridid": (sub_grid_id_1, sub_grid_id_2),
        }
        grid_id = RmnInterface.define_supergrid(grid_params)

        return grid_id, sub_grid_id_1, sub_grid_id_2

    else:
        grid_params = {
            "grtyp": grtyp,
            "ni": int(ni),
            "nj": int(nj),
            "ig1": int(ig1),
            "ig2": int(ig2),
            "ig3": int(ig3),
            "ig4": int(ig4),
            "iunit": 0,
        }
        grid_id = RmnInterface.define_grid(grid_params)

        return (grid_id,)


def define_u_grid(meta_df: pd.DataFrame, grtyp: str) -> Tuple[int, int, int]:
    """
    Defines a U-grid based on the provided metadata DataFrame and grid type.

    This function processes the metadata DataFrame to extract specific information
    required for defining a U-grid. It then calls the `define_grid` function with
    the extracted information and the specified grid type. If the grid definition
    does not result in three elements as expected, it raises a `GridUtilsError`.

    :param meta_df: The metadata DataFrame containing information for defining a U-grid.
    :type meta_df: pd.DataFrame
    :param grtyp: The type of the grid to be defined.
    :type grtyp: str
    :return: A tuple containing the input grid and two subgrids.
    :rtype: Tuple[int, int, int]
    """

    tictac_df = meta_df.loc[meta_df.nomvar == "^>"].reset_index(drop=True)
    tictac_df.at[0, "d"] = to_numpy(tictac_df.at[0, "d"])

    infos_grid = define_grid(grtyp, "", 0, 0, 0, 0, 0, 0, None, None, tictac_df.at[0, "d"])
    if len(infos_grid) != 3:
        raise GridDefinitionError(f"Problem with definition of grid of type U")

    input_grid, subgrid1, subgrid2 = infos_grid
    return input_grid, subgrid1, subgrid2


def define_sub_grids_u(tictac: np.ndarray) -> tuple:
    """
    Creates two sub-grids based on the input grid parameters and returns their identifiers.

    :param tictac: The input tic-tac grid represented as a numpy array.
    :type tictac: numpy.ndarray
    :return: A tuple containing ni and nj parameters and the identifiers of the two sub-grids.
    :rtype: tuple
    """
    start_pos = 5
    tictac = tictac.ravel(order="F")

    (ni, nj, ig1, ig2, ig3, ig4, ay, ax, next_pos) = get_grid_parameters_from_tictac_offset(tictac, start_pos)

    grid_params = {
        "grtyp": "Z",
        "grref": "E",
        "ni": ni,
        "nj": nj,
        "ig1": ig1,
        "ig2": ig2,
        "ig3": ig3,
        "ig4": ig4,
        "ay": np.array(ay),
        "ax": np.array(ax),
    }

    # Definition de la 1ere sous-grille
    sub_grid_id_1 = RmnInterface.define_grid_fmem(grid_params)

    start_pos = next_pos
    (ni, nj, ig1, ig2, ig3, ig4, ay, ax, _) = get_grid_parameters_from_tictac_offset(tictac, start_pos)

    grid_params = {
        "grtyp": "Z",
        "grref": "E",
        "ni": ni,
        "nj": nj,
        "ig1": ig1,
        "ig2": ig2,
        "ig3": ig3,
        "ig4": ig4,
        "ay": np.array(ay),
        "ax": np.array(ax),
    }

    # Definition de la 2eme sous-grille
    sub_grid_id_2 = RmnInterface.define_grid_fmem(grid_params)

    return ni, nj, sub_grid_id_1, sub_grid_id_2


class GetGridParamError(Exception):
    pass


def get_grid_parameters_from_tictac_offset(
    tictac: np.ndarray, start_position: int
) -> Tuple[int, int, int, int, int, int, List[int], List[int], int]:
    """
    Extracts grid parameters from a tictac array based on a starting position.

    :param tictac: The tictac array from which grid parameters are extracted.
    :type tictac: np.ndarray
    :param start_position: The starting position in the tictac array from which to extract grid parameters.
    :type start_position: int
    :return: A tuple containing ni, nj, ig1, ig2, ig3, ig4, ax, ay, and the next position in the tictac array.
    :rtype: Tuple[int, int, int, int, int, int, List[int], List[int], int]
    """

    ni = int(tictac[start_position])
    nj = int(tictac[start_position + 1])

    # Valeurs des ig1,ig2,ig3 et ig4 a start_postion + 6,+7,+8 et +9
    encoded_igs = tictac[start_position + 6 : start_position + 10]
    sub_grid_ref = "E"
    ig1, ig2, ig3, ig4 = RmnInterface.grid_definition_to_ig(sub_grid_ref, *encoded_igs)

    position_ax = start_position + 10
    position_ay = position_ax + ni
    next_position = position_ay + nj

    ax = tictac[position_ax:position_ay]
    ay = tictac[position_ay:next_position]

    return ni, nj, ig1, ig2, ig3, ig4, ay.tolist(), ax.tolist(), next_position


def get_grid_parameters_from_latlon_fields(lat_lon_df: pd.DataFrame) -> tuple:
    """
    Extracts grid parameters from dataframe containing latitude and longitude fields.

    :param lat_lon_df: The dataframe containing latitude and longitude fields.
    :type lat_lon_df: pd.DataFrame
    :raises GridUtilsError: If either the latitude or longitude dataframe is empty.
    :return: A tuple containing the grid parameters extracted from the latitude and longitude dataframe.
    :rtype: tuple
    """
    lat_df = lat_lon_df.loc[lat_lon_df.nomvar == "LAT"].reset_index(drop=True)
    lon_df = lat_lon_df.loc[lat_lon_df.nomvar == "LON"].reset_index(drop=True)
    if lat_df.empty or lon_df.empty:
        raise GetGridParamError("Missing LAT and/or LON fields in the dataframe! ")

    return get_grid_parameters(lat_df, lon_df)


def get_grid_parameters_from_positional_records(meta_df: pd.DataFrame) -> tuple:
    """
    Extracts grid parameters from metadata dataframe.

    :param meta_df: The dataframe containing metadata ^^,>>.
    :type meta_df: pd.DataFrame
    :raises GridUtilsError: If either of the positional records dataframe is empty.
    :return: A tuple containing the grid parameters extracted from the positional record dataframes.
    :rtype: tuple
    """

    lon_df = meta_df.loc[meta_df.nomvar == ">>"].reset_index(drop=True)
    lat_df = meta_df.loc[meta_df.nomvar == "^^"].reset_index(drop=True)
    if lat_df.empty or lon_df.empty:
        raise GetGridParamError("Positional records missing in the dataframe: ^^ and/or >> !")

    return get_grid_parameters(lat_df, lon_df)


def get_grid_parameters(lat_df: pd.DataFrame, lon_df: pd.DataFrame) -> tuple:
    """
    Extracts grid parameters from latitude and longitude dataframes.

    :param lat_df: The dataframe containing latitude data.
    :type lat_df: pandas.DataFrame
    :param lon_df: The dataframe containing longitude data.
    :type lon_df: pandas.DataFrame
    :raises GridUtilsError: If either lat_df or lon_df is empty.
    :return: A tuple containing the number of points in the i-direction (ni),
    the number of points in the j-direction (nj), the grid reference type (grref),
    the x-coordinate and y-coordinate of the grid origin (ax and ay), and the four grid indices (ig1, ig2, ig3, ig4).
    :rtype: tuple
    """

    if lat_df.empty or lon_df.empty:
        raise GetGridParamError("No data in lat_df or lon_df")
    lat_df = lat_df.reset_index(drop=True)
    lon_df = lon_df.reset_index(drop=True)
    nj = lat_df.iloc[0]["nj"]
    ni = lon_df.iloc[0]["ni"]
    grref = lat_df.iloc[0]["grtyp"]
    lat_df.at[0, "d"] = to_numpy(lat_df.at[0, "d"])
    lon_df.at[0, "d"] = to_numpy(lon_df.at[0, "d"])

    ay = lat_df.at[0, "d"]
    ax = lon_df.at[0, "d"]
    ig1 = lat_df.iloc[0]["ig1"]
    ig2 = lat_df.iloc[0]["ig2"]
    ig3 = lat_df.iloc[0]["ig3"]
    ig4 = lat_df.iloc[0]["ig4"]

    return ni, nj, grref, ax, ay, ig1, ig2, ig3, ig4


class GetSubGridsError(Exception):
    pass


def get_subgrids(grid_params: dict):
    """
    Extracts subgrids from the given grid parameters.

    :param grid_params: A dictionary containing grid parameters. Must include a 'subgrid' key with a list of exactly two subgrids.
    :type grid_params: dict
    :raises GetSubGridsError: If the 'subgrid' key is missing or if the list of subgrids does not contain exactly two items.
    :return: A tuple containing the two subgrids extracted from the grid parameters.
    :rtype: tuple
    """
    if "subgrid" not in grid_params:
        raise GetSubGridsError("No subgrids found!")

    subgrids = grid_params["subgrid"]
    if len(subgrids) != 2:
        raise GetSubGridsError("For U type grid, there should only be 2 subgrids!")

    return subgrids[0], subgrids[1]


class Get2DLatLonError(Exception):
    pass


def get_2d_lat_lon_arr(grid_params: dict) -> "list[Tuple[np.ndarray, np.ndarray]]":
    """
    Retrieves a 2D array of latitude and longitude coordinates from the given grid parameters.

    :param grid_params: A dictionary containing grid parameters. Must include a 'subgrid' key with a list of exactly two subgrids, or be a valid grid definition.
    :type grid_params: dict
    :raises Get2DLatLonError: If the grid parameters are not a dictionary, if the 'subgrid' key is missing or if the list of subgrids does not contain exactly two items.
    :return: A list of tuples, where each tuple contains two numpy arrays representing the concatenated latitude and longitude coordinates.
    :rtype: list[Tuple[np.ndarray, np.ndarray]]
    """
    if not isinstance(grid_params, dict):
        raise Get2DLatLonError("grid_id must be a valid grid definition as type dict")

    if "subgrid" in grid_params:
        if len(grid_params["subgrid"]) != 2:
            raise Get2DLatLonError("For U type grid, there should only be 2 subgrids!")

        gd1, gd2 = grid_params["subgrid"]

        lat1 = RmnInterface.get_lat_lon_from_grid(gd1)["lat"]
        lon1 = RmnInterface.get_lat_lon_from_grid(gd1)["lon"]
        lat2 = RmnInterface.get_lat_lon_from_grid(gd2)["lat"]
        lon2 = RmnInterface.get_lat_lon_from_grid(gd2)["lon"]
        lats = np.concatenate([lat1, lat2], axis=1)
        lons = np.concatenate([lon1, lon2], axis=1)
        latlons = (lats, lons)
    else:
        g = RmnInterface.get_lat_lon_from_grid(grid_params)
        latlons = (g["lat"], g["lon"])

    return latlons


def get_2d_lat_lon_df(df: pd.DataFrame) -> pd.DataFrame:
    """Gets the latitudes and longitudes as 2d arrays associated with the supplied grids

    :return: a pandas Dataframe object containing the lat and lon meta data of the grids
    :rtype: pd.DataFrame
    :raises Get2DLatLonError: no records to process
    """
    if df.empty:
        raise Get2DLatLonError("Empty DataFrame!")

    if "path" not in df.columns:
        df = add_path_and_key_columns(df)

    df_list = []

    path_groups = df.groupby("path", dropna=False)

    for _, path_df in path_groups:
        grid_groups = path_df.groupby("grid")
        for _, grid_df in grid_groups:
            no_meta_df = grid_df.loc[
                ~grid_df.nomvar.isin(["^^", ">>", "^>", "!!", "!!SF", "HY", "P0", "PT"])
            ].reset_index(drop=True)

            if no_meta_df.empty:
                continue

            tictic_df = pd.DataFrame([no_meta_df.iloc[0].to_dict()])
            tactac_df = pd.DataFrame([no_meta_df.iloc[0].to_dict()])

            grtyp = no_meta_df.grtyp.mode().iloc[0]
            if grtyp == "X":
                logging.warning(f"{grtyp} is an unsupported grid type!")
                continue

            grid_params = get_grid_definition_params(grid_df)
            (lat, lon) = get_2d_lat_lon_arr(grid_params)

            tictic_df["nomvar"] = "LA"
            tictic_df["d"] = [lat]
            tictic_df["ni"] = lat.shape[0]
            tictic_df["nj"] = lat.shape[1]

            tactac_df["nomvar"] = "LO"
            tactac_df["d"] = [lon]
            tactac_df["ni"] = lon.shape[0]
            tactac_df["nj"] = lon.shape[1]

            df_list.append(tictic_df)
            df_list.append(tactac_df)

    latlon_df = pd.safe_concat(df_list)
    return latlon_df


class GlobalGridError(Exception):
    pass


def is_global_grid(grid_params: dict, lon: np.ndarray, epsilon: float = 0.001) -> Tuple[bool, bool]:
    """Checks with the information received if the grid is a global grid as well as if the first longitude is repeated or not

    :param grid_params: grid parameters obtained from get_grid_definition_params
    :type grid_params: dict
    :param lon: 2d fortran order longitude matrix obtained with get_2d_lat_lon_arr
    :type lon: np.ndarray
    :param epsilon: Epsilon value for comparison operators, defaults to 0.001
    :type epsilon: float, optional
    :return: is a global grid, has  longitude repetition
    :rtype: (bool, bool)
    """

    global_grid = False
    repetition = False

    grtyp = grid_params["grtyp"]

    # Grilles de type A et B sont globales - pas besoin de faire de verification
    if (grtyp == "A") or (grtyp == "G"):
        global_grid = True

    elif grtyp == "B":
        global_grid = True
        repetition = True

    else:
        lon_data = lon.flatten(order="F")

        if grtyp == "Z":
            # Est-ce que la longitude se repete?
            # Si oui, grille globale avec point qui se repete
            # Si non, ce n'est pas une grille globale
            if _equal(lon_data[0], lon_data[-1], epsilon):
                global_grid = True
                repetition = True
        elif grtyp == "L":
            ni = grid_params["ni"]
            dlon = grid_params["dlon"]

            if math.fmod(360.0, dlon) != 0:
                nb_points = ni * dlon

                # On couvre plus que 360 deg et longitude[dernier point] <= (nb_points+dlon)-360
                if (_greater_or_equal(nb_points, 360.0, epsilon)) and (
                    _lower_or_equal(lon_data[-1], ((nb_points + dlon) - 360.0), epsilon)
                ):
                    # Cas 2 : on fait le tour MAIS la valeur du point qui se repete est differente du point 0
                    logging.warning(
                        "Global grid with the first longitude repeated at the end of the grid but with a different longitude!  will be treated as a non global grid!"
                    )
                    repetition = True
                elif (_greater_or_equal(nb_points, 360.0, epsilon)) and (_lower_than(lon_data[-1], 360.0, epsilon)):
                    # Cas 3 : on fait le tour -- dernier point ne se repete pas - pas de distance egale entre le dernier point et 0 degre
                    global_grid = True
                    repetition = False

            else:
                # Globale avec point qui ne se repete pas
                if _equal((ni * dlon), 360.0, epsilon):
                    global_grid = True
                # Globale avec point qui se repete
                elif _equal((ni * dlon), (360.0 + dlon), epsilon):
                    global_grid = True
                    repetition = True

    return global_grid, repetition


def _equal(value, threshold, epsilon=0.00001):
    return math.fabs(value - threshold) <= epsilon


def _greater_or_equal(value, threshold, epsilon=0.00001):
    return (value > threshold) or _equal(value, threshold, epsilon)


def _lower_than(value, threshold, epsilon=0.00001):
    return not _greater_or_equal(value, threshold, epsilon)


def _lower_or_equal(value, threshold, epsilon=0.00001):
    return value < threshold or _equal(value, threshold, epsilon)


def get_lat_lon_from_index(df: pd.DataFrame, x: list, y: list) -> pd.DataFrame:
    """Returns the lat-lon coordinates of data located at positions x-y

    :param df: a pandas Dataframe object containing at least one record with its metadata fields
    :type df: pd.DataFrame
    :param x: a list of x grid coords
    :type x: list
    :param y: a list of y grid coords
    :type y: list
    :raises Get2DLatLonError: Empty DataFrame
    :return: a dataframe of associated path, grid, grid_id, x, y, and lat lon results
    :rtype: pd.DataFrame
    """

    if df.empty:
        raise Get2DLatLonError("Empty DataFrame!")

    if not isinstance(x, list):
        raise Get2DLatLonError(f"x must be a list")

    if not isinstance(y, list):
        raise Get2DLatLonError(f"y must be a list")

    for elem in x:
        if not isinstance(elem, int):
            raise Get2DLatLonError(f"elements of x must integers")  # raise if any element is not an integer

    for elem in y:
        if not isinstance(elem, int):
            raise Get2DLatLonError(f"elements of y must integers")  # raise if any element is not an integer

    if len(x) != len(y):
        raise Get2DLatLonError("x and y must be lists of same size")

    if "path" not in df.columns:
        df = add_path_and_key_columns(df)

    dfs = []
    path_groups = df.groupby("path")
    for path, path_df in path_groups:
        grid_groups = path_df.groupby("grid")
        for grid, grid_df in grid_groups:
            no_meta_df = grid_df.loc[
                ~grid_df.nomvar.isin(["^^", ">>", "^>", "!!", "!!SF", "HY", "P0", "PT"])
            ].reset_index(drop=True)

            if no_meta_df.empty:
                continue

            grtyp_groups = no_meta_df.groupby("grtyp")
            for grtyp, grtyp_df in grtyp_groups:
                if grtyp == "X":
                    logging.warning(f"{grtyp} is an unsupported grid type!")
                    continue
                max_x_val = grtyp_df.iloc[0].ni - 1
                max_y_val = grtyp_df.iloc[0].nj - 1

                for elem in x:
                    if not (0 <= elem <= max_x_val):
                        raise Get2DLatLonError(f"elements of x must inside the following range: 0 to {max_x_val}")

                for elem in y:
                    if not (0 <= elem <= max_y_val):
                        raise Get2DLatLonError(f"elements of y must inside the following range: 0 to {max_y_val}")

                grid_params = get_grid_definition_params(grtyp_df)
                lalo = RmnInterface.get_lat_lon_from_xy(
                    grid_params["id"], [e + 1 for e in x], [e + 1 for e in y]
                )  # add 1 to all index for fortran compat
                paths = [path for _ in range(len(x))]
                grids = [grid for _ in range(len(x))]
                grtyps = [grtyp for _ in range(len(x))]
                dfs.extend(
                    [
                        {"path": pat, "grid": gd, "grid": gr, "x": xi, "y": yi, "lat": la, "lon": lo}
                        for pat, gd, gr, xi, yi, la, lo in zip(paths, grids, grtyps, x, y, lalo["lat"], lalo["lon"])
                    ]
                )

    return pd.DataFrame(dfs)


def get_index_from_lat_lon(df: pd.DataFrame, lat: list, lon: list) -> pd.DataFrame:
    """Returns the x-y coordinates of data located at lat-lon

    :param df: a pandas Dataframe object containing at least one record with its metadata fields
    :type df: pd.DataFrame
    :param lat: a list of latitudes
    :type lat: list
    :param lon: a list of longitudes
    :type lon: list
    :raises Get2DLatLonError: Empty DataFrame
    :return: a dataframe of associated path, grid, grid_id, x, y, and lat lon results
    :rtype: pd.DataFrame
    """

    if df.empty:
        raise Get2DLatLonError("Empty DataFrame!")

    if not isinstance(lat, list):
        raise Get2DLatLonError(f"lat must be a list")

    if not isinstance(lon, list):
        raise Get2DLatLonError(f"lon must be a list")

    if len(lat) != len(lon):
        raise Get2DLatLonError("lat and lon must be lists of same size")

    if "path" not in df.columns:
        df = add_path_and_key_columns(df)

    dfs = []
    path_groups = df.groupby("path")
    for path, path_df in path_groups:
        grid_groups = path_df.groupby("grid")
        for grid, grid_df in grid_groups:
            no_meta_df = grid_df.loc[
                ~grid_df.nomvar.isin(["^^", ">>", "^>", "!!", "!!SF", "HY", "P0", "PT"])
            ].reset_index(drop=True)

            if no_meta_df.empty:
                continue

            grtyp_groups = no_meta_df.groupby("grtyp")
            for grtyp, grtyp_df in grtyp_groups:
                if grtyp == "X":
                    logging.warning(f"{grtyp} is an unsupported grid type! skipping")
                    continue

                grid_params = get_grid_definition_params(grtyp_df)
                xy = RmnInterface.get_xy_from_lat_lon(grid_params["id"], lat, lon)
                paths = [path for _ in range(len(lat))]
                grids = [grid for _ in range(len(lat))]
                grtyps = [grtyp for _ in range(len(lat))]
                dfs.extend(
                    [
                        {"path": pat, "grid": gd, "grid": gr, "x": xi, "y": yi, "lat": la, "lon": lo}
                        for pat, gd, gr, xi, yi, la, lo in zip(
                            paths, grids, grtyps, [e - 1 for e in xy["x"]], [e - 1 for e in xy["y"]], lat, lon
                        )
                    ]
                )

    return pd.DataFrame(dfs)


def set_grid_parameters(df: pd.DataFrame) -> tuple:
    """
    Extracts grid parameters from a dataframe and returns them as a tuple.

    :param df: The dataframe containing the grid parameters.
    :type df: pd.DataFrame
    :raises GridUtilsError: If the dataframe is empty.
    :return: A tuple containing the grid parameters (ni, nj, ig1, ig2, ig3, ig4).
    :rtype: tuple
    """

    if df.empty:
        raise GridDefinitionError("No data in df")
    ni = df.iloc[0]["ni"]
    nj = df.iloc[0]["nj"]
    ig1 = df.iloc[0]["ig1"]
    ig2 = df.iloc[0]["ig2"]
    ig3 = df.iloc[0]["ig3"]
    ig4 = df.iloc[0]["ig4"]

    return ni, nj, ig1, ig2, ig3, ig4


def set_new_grid_identifiers(
    res_df: pd.DataFrame, grtyp: str, ni: int, nj: int, ig1: int, ig2: int, ig3: int, ig4: int
) -> pd.DataFrame:
    """
    Sets new grid identifiers for a DataFrame based on provided grid type and identifiers.

    :param res_df: The DataFrame containing the grid information.
    :type res_df: pd.DataFrame
    :param grtyp: The grid type to be set in the DataFrame.
    :type grtyp: str
    :param ni: The first dimension of the grid to be set in the DataFrame.
    :type ni: int
    :param nj: The second dimension of the grid to be set in the DataFrame.
    :type nj: int
    :param ig1: The first identifier to be set in the DataFrame.
    :type ig1: int
    :param ig2: The second identifier to be set in the DataFrame.
    :type ig2: int
    :param ig3: The third identifier to be set in the DataFrame.
    :type ig3: int
    :param ig4: The fourth identifier to be set in the DataFrame.
    :type ig4: int
    :return: A DataFrame with updated grid identifiers or an empty DataFrame if no matching rows were found.
    :rtype: pd.DataFrame
    """

    other_res_df = res_df.loc[res_df.nomvar != "!!"].reset_index(drop=True)
    if other_res_df.empty:
        return pd.DataFrame(dtype=object)
    shape_list = [(ni, nj) for _ in range(len(other_res_df.index))]
    other_res_df["shape"] = shape_list
    other_res_df["ni"] = ni
    other_res_df["nj"] = nj
    other_res_df["grtyp"] = grtyp
    other_res_df["interpolated"] = True
    other_res_df["ig1"] = ig1
    other_res_df["ig2"] = ig2
    other_res_df["ig3"] = ig3
    other_res_df["ig4"] = ig4
    other_res_df["grid"] = "".join([str(ig1), str(ig2)])

    return other_res_df


def set_new_grid_identifiers_for_toctoc(res_df: pd.DataFrame, ig1: int, ig2: int) -> pd.DataFrame:
    """
    Sets new grid identifiers for toctoc based on provided identifiers.

    :param res_df: The DataFrame containing the grid information.
    :type res_df: pd.DataFrame
    :param ig1: The first identifier to be set in the DataFrame.
    :type ig1: int
    :param ig2: The second identifier to be set in the DataFrame.
    :type ig2: int
    :return: A DataFrame with updated grid identifiers or an empty DataFrame if no matching rows were found.
    :rtype: pd.DataFrame
    """

    toctoc_res_df = res_df.loc[res_df.nomvar == "!!"].reset_index(drop=True)
    if toctoc_res_df.empty:
        return pd.DataFrame(dtype=object)
    toctoc_res_df["ip1"] = ig1
    toctoc_res_df["ip2"] = ig2
    toctoc_res_df["grid"] = "".join([str(ig1), str(ig2)])

    return toctoc_res_df


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
