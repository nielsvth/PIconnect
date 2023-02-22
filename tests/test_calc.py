"""Unit Tests for calc.py Module""" ""

import PIconnect
import pandas as pd


def test_connection():
    """Test to check for connected servers"""
    assert (
        len(PIconnect.PIServer.servers) >= 1
    ), "Should be larger or equal to 1"


def test_calc_recorded(af_connect, calc_timerange):
    """Test functionalty of Calculation class: recorded values"""
    starttime = calc_timerange[0]
    endtime = calc_timerange[1]

    server = af_connect[1]
    tag = server.find_tags("SINUSOID")[0]

    calc1 = PIconnect.calc.calc_recorded(
        starttime,
        endtime,
        r"IF ('\\ITSBEBEPIHISCOL\SINUSOID' > 70) THEN (Abs('\\ITSBEBEPIHISCOL\SINUSOID')) ELSE (0)",
    )
    assert calc1.shape == (8, 1), "shape should be (8,1)"
    assert (
        calc1["calculation"].iloc[0] == tag.interpolated_value(starttime)[1]
    ), "Should be 91.82201"


def test_calc_interpol(calc_timerange):
    """Test functionalty of Calculation class: interpolated values"""
    starttime = calc_timerange[0]
    endtime = calc_timerange[1]

    calc2 = PIconnect.calc.calc_interpolated(
        starttime,
        endtime,
        "1h",
        r"IF ('\\ITSBEBEPIHISCOL\SINUSOID' > 70) THEN (Abs('\\ITSBEBEPIHISCOL\SINUSOID')+10) ELSE (0)",
    )
    assert calc2.shape == (9, 1), "shape should be (9,1)"
    assert (
        calc2["calculation"].iloc[0] == 101.82200659857855
    ), "Should be 101.82200659857855"

    # https://docs.osisoft.com/bundle/pi-server/page/tagtot.html
    # returns totalized value per minute, do *1440 to get per day
    calc3 = PIconnect.calc.calc_interpolated(
        starttime,
        starttime,
        "1h",
        r"TagTot('\\ITSBEBEPIHISCOL\SINUSOID', '01-Oct-2022 14:00:00', '03-Oct-2022 14:00:00')",
    )
    assert len(calc3) == 2, "Table length should be 2"

    calc3 = PIconnect.calc.calc_summary(
        starttime="1-10-2022 14:00",
        endtime="1-10-2022 22:00",
        interval="100h",
        summary_types=4 | 8,
        expression=r"('\\ITSBEBEPIHISCOL\SINUSOID')-('\\ITSBEBEPIHISCOL\SINUSOIDU')",
    )
    assert len(calc3) == 2, "Table length should be 2"
