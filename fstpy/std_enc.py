# -*- coding: utf-8 -*-
import datetime

from fstpy.rmn_interface import RmnInterface

from fstpy import DATYP_DICT


def create_encoded_standard_etiket(
    label: str,
    run: str,
    implementation: str,
    ensemble_member: str,
    etiket_format: str = "",
    ignore_extended: bool = False,
    override_pds_label: bool = False,
) -> str:
    """Creates a new etiket based on label, run, implementation and ensemble member attributes

    :param label: label string
    :type label: str
    :param run: model run string
    :type run: str
    :param implementation: implementation string
    :type implementation: str
    :param ensemble_member: ensemble member number as string
    :type ensemble_member: str
    :param etiket_format: flag with number of character in run, label, implementation and ensemble_member and K or D to indicate if we keep or discard this format (ex: "2,5,1,3,K")
    :type etiket_format: str
    :param ignore_extended: flag to indicate that the etiket should just be the label
    :type ignore_extended: bool
    :param override_pds_label: flag to indicate that the etiket should just be the label
    :type override_pds_label: bool
    :return: an etiket composed of supplied parameters
    :rtype: str
    """

    if override_pds_label or ignore_extended:
        return label

    if implementation not in ["X", "N", "P"]:
        implementation = "X"

    if implementation != "X" and len(label) > 6:
        raise Exception("LE PDSLABEL EST TROP LONG, LA LONGUEUR ACCEPTEE EST MAXIMUM 6! - '{}'".format(label))

    keep_format = False
    if type(etiket_format) == str and etiket_format != "":
        length = etiket_format.split(",")
        if len(length) == 5:
            length_run = int(length[0])
            length_label = int(length[1])
            length_implementation = int(length[2])
            length_ensemble = int(length[3])
            keep_format = length[4] == "K"

    if not keep_format:
        length_run = 2
        length_label = 6
        length_implementation = 1
        length_ensemble = 3

    if (length_run + length_implementation + length_ensemble + length_label) > 12:
        print("The etiket is too long and might get cut in writer")

    label = label + "____________"

    if ensemble_member == "None" or ensemble_member is None:
        ensemble_member = ""

    if run is None or len(run) == 0 or run == "None":
        run = "__"

    run = run[:length_run]
    label = label[:length_label]
    implementation = implementation[:length_implementation]
    ensemble_member = ensemble_member[:length_ensemble]
    etiket = run + label + implementation + ensemble_member

    return etiket


def create_encoded_etiket(label: str, run: str, implementation: str, ensemble_member: str) -> str:
    """Creates a new etiket based on label, run, implementation and ensemble member attributes

    :param label: label string
    :type label: str
    :param run: model run string
    :type run: str
    :param implementation: implementation string
    :type implementation: str
    :param ensemble_member: ensemble member number as string
    :type ensemble_member: str
    :return: an etiket composed of supplied parameters
    :rtype: str
    """
    etiket = label

    if run != "None":
        etiket = run + label
    if implementation != "None":
        etiket = etiket + implementation
    if ensemble_member != "None":
        etiket = etiket + ensemble_member

    return etiket


def create_encoded_dateo(date_of_observation: datetime.datetime) -> int:
    """Create a RMNDate int from a datetime object

    :param date_of_observation: date of observation as a datetime object
    :type date_of_observation: datetime.datetime
    :return: dateo as a RMNDate int
    :rtype: int
    """

    return RmnInterface.create_rpn_date(date_of_observation, dt=0, nstep=0).dateo


def create_encoded_npas_and_ip2(forecast_hour: datetime.timedelta, deet: int) -> tuple:
    """Creates npas and ip2 from the forecast_hour and deet attributes

    :param forecast_hour: forecast hours in seconds
    :type forecast_hour: datetime.timedelta
    :param deet: length of a time step in seconds - usually invariable - relates to model ouput times
    :type deet: int
    :return: new calculated npas and ip2
    :rtype: tuple
    """
    # ip2 = 6, deet = 300, np = 72
    # fhour = 21600
    # npas = hours/deet
    seconds = forecast_hour.total_seconds()
    npas = int(seconds / deet)
    ip2 = seconds / 3600.0
    ip2_code = create_encoded_ip2(ip2, RmnInterface.KIND_HOURS)
    return npas, ip2_code


def create_encoded_ip1(level: float, ip1_kind: int, mode: int = RmnInterface.CONVIP_ENCODE) -> int:
    """returns an encoded ip1 from level and kind

    :param level: level value
    :type level: float
    :param ip1_kind: kind value as int
    :type ip1_kind: int
    :return: encoded ip1
    :rtype: int
    """

    return RmnInterface.convert_ip(mode, level, ip1_kind)


def create_encoded_ip2(level: float, ip2_kind: int) -> int:
    """returns an encoded ip2 from level and kind

    :param level: level value
    :type level: float
    :param ip2_kind: kind value as int
    :type ip2_kind: int
    :return: encoded ip2
    :rtype: int
    """
    rp1 = RmnInterface.convert_to_float_ip(0, 0, RmnInterface.KIND_ARBITRARY)
    rp2 = RmnInterface.convert_to_float_ip(level, level, ip2_kind)
    return RmnInterface.encode_ip(rp1, rp2, rp1)[1]


def create_encoded_ips(
    level: float, ip1_kind: int, ip2_dec: float, ip2_kind: int, ip3_dec: float, ip3_kind: int
) -> tuple:
    """Returns encoded ip1,ip2 and ip3 from values and kinds

    :param level: level value
    :type level: float
    :param ip1_kind: ip1 kind value
    :type ip1_kind: int
    :param ip2_dec: decoded ip2 value
    :type ip2_dec: float
    :param ip2_kind: ip2 kind  value
    :type ip2_kind: int
    :param ip3_dec: decoded ip3 valued
    :type ip3_dec: float
    :param ip3_kind: ip3 kind value
    :type ip3_kind: int
    :return: encoded ip1,ip2 and ip3 values
    :rtype: tuple
    """
    ip1 = create_encoded_ip1(level, ip1_kind)
    ip2 = create_encoded_ip1(ip2_dec, ip2_kind)
    ip3 = create_encoded_ip1(ip3_dec, ip3_kind)
    return ip1, ip2, ip3


def create_encoded_datyp(data_type_str: str) -> int:
    """creates an encoded datyp value from a data type string

    :param data_type_str: possible values 'X','R','I','S','E','F','A','Z','i','e','f'
    :type data_type_str: str
    :return: an ecoded datyp value
    :rtype: int
    """
    new_dict = {v: k for k, v in DATYP_DICT.items()}
    return new_dict[data_type_str]


def modifiers_to_typvar2(
    zapped: bool,
    filtered: bool,
    interpolated: bool,
    unit_converted: bool,
    bounded: bool,
    ensemble_extra_info: bool,
    multiple_modifications: bool,
) -> str:
    """Creates the second lette of the typvar from the supplied flags"""
    number_of_modifications = 0
    typvar2 = ""
    if zapped == True:
        number_of_modifications += 1
        typvar2 = "Z"
    if filtered == True:
        number_of_modifications += 1
        typvar2 = "F"
    if interpolated == True:
        number_of_modifications += 1
        typvar2 = "I"
    if unit_converted == True:
        number_of_modifications += 1
        typvar2 = "U"
    if bounded == True:
        number_of_modifications += 1
        typvar2 = "B"
    if ensemble_extra_info == True:
        number_of_modifications += 1
        typvar2 = "!"
    if multiple_modifications == True:
        number_of_modifications += 1
        typvar2 = "M"
    if number_of_modifications > 1:
        # more than one modification has been done. Force M
        typvar2 = "M"
    return typvar2


def encode_ip2_and_ip3_as_time_interval(df):
    for row in df.itertuples():
        if row.nomvar in [">>", "^^", "^>", "!!", "P0", "PT"]:
            continue
        ip2 = row.ip2
        ip3 = row.ip3
        (ip2, ip3) = one_encode_ip2_and_ip3_as_time_interval(ip2, ip3)
        df.at[row.Index, "ip2"] = ip2
        df.at[row.Index, "ip3"] = ip3
    return df


def one_encode_ip2_and_ip3_as_time_interval(ip2, ip3):
    rp1a = RmnInterface.convert_to_float_ip(0.0, 0.0, RmnInterface.LEVEL_KIND_PMB)
    rp2a = RmnInterface.convert_to_float_ip(ip2, ip3, RmnInterface.TIME_KIND_HR)
    rp3a = RmnInterface.convert_to_float_ip(ip2 - ip3, 0, RmnInterface.TIME_KIND_HR)
    (_, ip2, ip3) = RmnInterface.encode_ip(rp1a, rp2a, rp3a)
    return ip2, ip3
