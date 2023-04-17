"""Unit Tests for PI.py Module""" ""

import PIconnect
import datetime
import pandas as pd
from PIconnect.PIConsts import SummaryType

def test_connection():
    """Test to check for connected servers"""
    assert (
        len(PIconnect.PIServer.servers) >= 1
    ), "Should be larger or equal to 1"


def test_find_tags(af_connect):
    """Test to find Tags on AFDatabase"""
    # Use default PIserver
    # SINUSOID is a default tag available on any PIServer
    server = af_connect[1]
    taglist = server.find_tags("SINUSOID")
    assert len(taglist) == 1, "Should be 1"
    tag = taglist[0]
    assert tag.name == "SINUSOID", "should be 'SINUSOID'"
    assert len(tag.raw_attributes) == 58, "should be 58"
    assert (
        tag.description == "12 Hour Sine Wave"
    ), "tag description should be '12 Hour Sine Wave'"


def test_tags(af_connect, pi_timerange):
    """Test functionalty of the Tag/Pipoint class"""
    starttime = pi_timerange[0]
    endtime = pi_timerange[1]

    server = af_connect[1]
    taglist = server.find_tags("SINUSOID")
    tag = taglist[0]

    # interpolated value
    assert (
        type(tag.interpolated_value(time=starttime)[0]) == datetime.datetime
    ), "type should be datetime.datetime"
    assert (
        round(tag.interpolated_value(time=starttime)[1], 2) == 49.45
    ), "result should be 49.45"

    # interpolated values
    assert (
        len(
            tag.interpolated_values(
                starttime=starttime, endtime=endtime, interval="1h"
            )
        )
        == 217
    ), "length of result should be 217"

    # recorded values
    assert (
        len(tag.recorded_values(starttime=starttime, endtime=endtime)) == 146
    ), "length of result should be 146"

    # recorded values, with filter expression
    assert (
        len(
            tag.recorded_values(
                starttime=starttime,
                endtime=endtime,
                filter_expression="'%tag%' > 30",
            )
        )
        == 74
    ), "length of result should be 74"

    assert (
        round(
            tag.recorded_values(
                starttime=starttime,
                endtime=starttime,
                filter_expression="'%tag%' > 30",
            )["SINUSOID"].min(),
            2,
        )
        == 49.45
    ), "minimum value should be 49.45"

    # plot values
    assert (
        len(
            tag.plot_values(
                starttime=starttime, endtime=endtime, nr_of_intervals=10
            )
        )
        == 39
    ), "length of result should be 39"

    # summary values
    assert (
        len(
            tag.summary(
                starttime=starttime,
                endtime=endtime,
                summary_types=SummaryType.Average
                | SummaryType.Minimum
                | SummaryType.Maximum,
            )
        )
        == 3
    ), "length of result should be 3"

    # summaries values
    assert (
        len(
            tag.summaries(
                starttime=starttime,
                endtime=endtime,
                interval="1d",
                summary_types=SummaryType.Average
                | SummaryType.Minimum
                | SummaryType.Maximum,
            )
        )
        == 27
    ), "length of result should be 27"

    # filtered_summaries
    assert (
        len(
            tag.filtered_summaries(
                starttime=starttime,
                endtime=endtime,
                interval="1d",
                summary_types=SummaryType.Average
                | SummaryType.Minimum
                | SummaryType.Maximum,
                filter_expression="'SINUSOID' > 20",
            )
        )
        == 27
    ), "length of result should be 27"

    assert (
        round(
            tag.filtered_summaries(
                starttime=starttime,
                endtime=endtime,
                interval="1d",
                summary_types=SummaryType.Average
                | SummaryType.Minimum
                | SummaryType.Maximum,
                filter_expression="'SINUSOID' > 20",
            )["Value"].min(),
            2,
        )
        == 24.01
    ), "minimum value should be 24.01"


def test_taglist(af_connect, pi_timerange):
    """Test functionalty of the TagList class"""
    starttime = pi_timerange[0]
    endtime = pi_timerange[1]

    server = af_connect[1]
    taglist1 = server.find_tags("SINUSOID")
    taglist2 = server.find_tags("SINUSOIDU")
    taglist = taglist1 + taglist2
    assert len(taglist) == 2, "TagList should contain 2 Tags"

    # current value
    assert (
        len(taglist.current_value().columns) == 2
    ), "Table should have 2 columns"

    # interpolated value
    assert (
        type(taglist.interpolated_value(time="1-1-2022")) == pd.DataFrame
    ), "Output is of type dataframe"

    # interpolated values
    assert taglist.interpolated_values(
        starttime=starttime, endtime=endtime, interval="1h"
    ).shape == (217, 2), "shape of result should be (217, 2)"

    # recorded values
    assert (
        type(taglist.recorded_values(starttime=starttime, endtime=endtime))
        == dict
    ), "returns a dict object"
    assert taglist.recorded_values(starttime=starttime, endtime=endtime)[
        "SINUSOID"
    ].shape == (146, 1), "shape of 'SINUSOID' table is (146,1)"

    # plot values
    assert list(
        taglist.plot_values(
            starttime=starttime, endtime=endtime, nr_of_intervals=10
        ).keys()
    ) == [
        "SINUSOID",
        "SINUSOIDU",
    ], "returns a dict object with keys ['SINUSOID', 'SINUSOIDU']"

    assert taglist.plot_values(
        starttime=starttime, endtime=endtime, nr_of_intervals=10
    )["SINUSOID"].shape == (39, 1), "shape of 'SINUSOID' table is (39,1)"

    # summary values
    assert (
        len(
            taglist.summary(
                starttime=starttime,
                endtime=endtime,
                summary_types=SummaryType.Average
                | SummaryType.Minimum
                | SummaryType.Maximum,
            )
        )
        == 6
    ), "length of result should be 6"

    # summaries values
    assert (
        len(
            taglist.summaries(
                starttime=starttime,
                endtime=endtime,
                interval="1d",
                summary_types=SummaryType.Average
                | SummaryType.Minimum
                | SummaryType.Maximum,
            )
        )
        == 54
    ), "length of result should be 54"

    # filtered_summaries
    assert (
        len(
            taglist.filtered_summaries(
                starttime=starttime,
                endtime=endtime,
                interval="1d",
                summary_types=SummaryType.Average
                | SummaryType.Minimum
                | SummaryType.Maximum,
                filter_expression="'SINUSOID' > 20",
            )
        )
        == 54
    ), "length of result should be 54"

    assert (
        round(
            taglist.filtered_summaries(
                starttime=starttime,
                endtime=endtime,
                interval="1d",
                summary_types=SummaryType.Average
                | SummaryType.Minimum
                | SummaryType.Maximum,
                filter_expression="'SINUSOID' > 20",
            )["Value"].min(),
            2,
        )
        == 24.01
    ), "minimum value should be 24.01"
