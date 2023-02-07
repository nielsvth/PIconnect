"""Unit Tests for PI.py Module""" ""

import PIconnect
import datetime
import pandas as pd

# Set up timezone info
PIconnect.PIConfig.DEFAULT_TIMEZONE = "Europe/Brussels"


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


def test_tags(af_connect):
    """Test functionalty of the Tag/Pipoint class"""
    server = af_connect[1]
    taglist = server.find_tags("SINUSOID")
    tag = taglist[0]

    # interpolated value
    assert (
        type(tag.interpolated_value(time="1-1-2022")[0]) == datetime.datetime
    ), "type should be datetime.datetime"
    assert (
        tag.interpolated_value(time="1-1-2022")[1] == 49.45119
    ), "result should be 49.45119"

    # interpolated values
    assert (
        len(
            tag.interpolated_values(
                starttime="1-1-2022", endtime="10-1-2022", interval="1h"
            )
        )
        == 217
    ), "length of result should be 217"

    # recorded values
    assert (
        len(tag.recorded_values(starttime="1-1-2022", endtime="10-1-2022"))
        == 146
    ), "length of result should be 146"

    # recorded values, with filter expression
    assert (
        len(
            tag.recorded_values(
                starttime="1-1-2022",
                endtime="10-1-2022",
                filter_expression="'%tag%' > 30",
            )
        )
        == 74
    ), "length of result should be 74"

    assert (
        tag.recorded_values(
            starttime="1-1-2022",
            endtime="10-1-2022",
            filter_expression="'%tag%' > 30",
        )["SINUSOID"].min()
        == 49.45119
    ), "minimum value should be 49.45119"

    # plot values
    assert (
        len(
            tag.plot_values(
                starttime="1-1-2022", endtime="10-1-2022", nr_of_intervals=10
            )
        )
        == 39
    ), "length of result should be 39"

    # summary values
    assert (
        len(
            tag.summary(
                starttime="1-1-2022",
                endtime="10-1-2022",
                summary_types=2 | 4 | 8,
            )
        )
        == 3
    ), "length of result should be 3"

    # summaries values
    assert (
        len(
            tag.summaries(
                starttime="1-1-2022",
                endtime="10-1-2022",
                interval="1d",
                summary_types=2 | 4 | 8,
            )
        )
        == 27
    ), "length of result should be 27"

    # filtered_summaries
    assert (
        len(
            tag.filtered_summaries(
                starttime="1-1-2022",
                endtime="10-1-2022",
                interval="1d",
                summary_types=2 | 4 | 8,
                filter_expression="'SINUSOID' > 20",
            )
        )
        == 27
    ), "length of result should be 27"

    assert (
        tag.filtered_summaries(
            starttime="1-1-2022",
            endtime="10-1-2022",
            interval="1d",
            summary_types=2 | 4 | 8,
            filter_expression="'SINUSOID' > 20",
        )["Value"].min()
        == 24.005191802978516
    ), "minimum value should be 24.005191802978516"


def test_taglist(af_connect):
    """Test functionalty of the TagList class"""
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
        starttime="1-1-2022", endtime="10-1-2022", interval="1h"
    ).shape == (217, 2), "shape of result should be (217, 2)"

    # recorded values
    assert (
        type(
            taglist.recorded_values(starttime="1-1-2022", endtime="10-1-2022")
        )
        == dict
    ), "returns a dict object"
    assert taglist.recorded_values(starttime="1-1-2022", endtime="10-1-2022")[
        "SINUSOID"
    ].shape == (146, 1), "shape of 'SINUSOID' table is (146,1)"

    # plot values
    assert list(
        taglist.plot_values(
            starttime="1-1-2022", endtime="10-1-2022", nr_of_intervals=10
        ).keys()
    ) == [
        "SINUSOID",
        "SINUSOIDU",
    ], "returns a dict object with keys ['SINUSOID', 'SINUSOIDU']"

    assert taglist.plot_values(
        starttime="1-1-2022", endtime="10-1-2022", nr_of_intervals=10
    )["SINUSOID"].shape == (39, 1), "shape of 'SINUSOID' table is (39,1)"

    # summary values
    assert (
        len(
            taglist.summary(
                starttime="1-1-2022",
                endtime="10-1-2022",
                summary_types=2 | 4 | 8,
            )
        )
        == 6
    ), "length of result should be 6"

    # summaries values
    assert (
        len(
            taglist.summaries(
                starttime="1-1-2022",
                endtime="10-1-2022",
                interval="1d",
                summary_types=2 | 4 | 8,
            )
        )
        == 54
    ), "length of result should be 54"

    # filtered_summaries
    assert (
        len(
            taglist.filtered_summaries(
                starttime="1-1-2022",
                endtime="10-1-2022",
                interval="1d",
                summary_types=2 | 4 | 8,
                filter_expression="'SINUSOID' > 20",
            )
        )
        == 54
    ), "length of result should be 54"

    assert (
        taglist.filtered_summaries(
            starttime="1-1-2022",
            endtime="10-1-2022",
            interval="1d",
            summary_types=2 | 4 | 8,
            filter_expression="'SINUSOID' > 20",
        )["Value"].min()
        == 24.005191802978516
    ), "minimum value should be 24.005191802978516"
