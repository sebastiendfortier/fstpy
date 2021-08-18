
from datetime import datetime, timedelta
from fstpy import std_enc
from rpnpy.librmn import all as rmn

def test_1():
    """Test #1 : Test creation of encoded npas and ip2"""
    # test creation of encoded npas and ip2
    #ip2 = 6, deet = 300, np = 72
    #fhour = 21600
    #npas = hours/deet

    fhour = 6
    deet = 300
    npas = 72

    dt = timedelta(hours=fhour)

    assert npas * deet == dt.total_seconds()

    ip2 = std_enc.create_encoded_ip2(fhour, rmn.KIND_HOURS)

    npas_act, ip2_act = std_enc.create_encoded_npas_and_ip2(dt, deet)

    assert isinstance(npas_act, int)

    assert isinstance(ip2_act, int)

    assert (npas_act, ip2_act) == (npas, ip2)

    # check big fh

    for h in range(300):
        dt = timedelta(hours=h)
        ip2 = std_enc.create_encoded_ip2(h, rmn.KIND_HOURS)
        assert std_enc.create_encoded_npas_and_ip2(dt, deet)== (dt.total_seconds() / deet, ip2)


def test_2():
    """Test #2 : Test dateo encoding"""
    # test dateo encoding
    t = datetime.now()
    t_enc = std_enc.create_encoded_dateo(t)

    assert isinstance(t_enc, int)
