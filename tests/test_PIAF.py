"""Unit Tests for PIAF.py Module""" ""

import PIconnect
import datetime
import pandas as pd

# Set up timezone info
PIconnect.PIConfig.DEFAULT_TIMEZONE = "Europe/Brussels"


def test_connection():
    """Test to check for connected servers and AF Databases"""
    assert (
        len(PIconnect.PIServer.servers) >= 1
    ), "Should be larger or equal to 1"
    assert (
        len(PIconnect.PIAFDatabase.servers) >= 1
    ), "Should be larger or equal to 1"


def test_find_assets(af_connect):
    """Test to find Assets on AFDatabase"""
    afdatabase = af_connect[0]
    assetlist = afdatabase.find_assets(query="Equipment")
    assert len(assetlist) == 11, "Should be 11"
    asset = assetlist[0].children[0]
    assert asset.name == "B-334", "should be 'B-334'"
    assert len(asset.attributes) == 21, "Should be 21"
    assert (
        asset.get_attribute_values()["Plant"] == "Wichita"
    ), "Should be 'Wichita'"


def test_find_events(af_connect):
    """Test to find events on AFDatabase"""
    afdatabase = af_connect[0]
    eventlist = afdatabase.find_events(
        query="*", starttime="2/10/2022 11:29:12", endtime="3/10/2022 17:18:44"
    )
    assert len(eventlist) == 6, "Should be 6"
    event = eventlist[0]
    assert event.name == "Unit 1", "Should be 'Unit 1'"
    assert event.parent.name == "Batch A", "Should be 'Batch A'"
    assert (
        type(event.starttime) == datetime.datetime
    ), "Should be of type datetime.datetime"
    assert (
        type(event.duration) == datetime.timedelta
    ), "Should be of type datetime.timedelta"
    assert event.template_name == "Unit_template", "Should be 'Unit_template'"
    assert len(event.attributes) == 2, "Should be 2"
    assert event.ref_elements[0] == "P-560", "Should be 'P-560'"
    assert (
        event.get_attribute_values()["Manufacturer"] == "Sterns"
    ), "Should be 'Sterns'"


def test_attributes(af_connect):
    """Test for attribute class"""
    afdatabase = af_connect[0]
    assetlist = afdatabase.find_assets(query="Equipment")
    asset = assetlist[0].children[0]
    assert len(asset.attributes) == 21, "Should be 21"
    attribute = asset.attributes[3]
    assert attribute.name == "Water Flow", "Should be 'Water Flow'"
    assert attribute.source_type == "PI Point", "Should be 'PI Point'"
    assert attribute.parent.name == "B-334", "should be 'B-334'"


def test_attribute_extracts(af_connect):
    """Test extraction functionalty for Attributes"""
    afdatabase = af_connect[0]
    assetlist = afdatabase.find_assets(query="Equipment")
    asset = assetlist[0].children[0]

    # Attribute is pipoint
    attribute_tag = asset.attributes[3].pipoint
    result = attribute_tag.interpolated_values(
        starttime="*-10d", endtime="*", interval="1h"
    )
    assert type(result) == pd.DataFrame, "Output type should be pd.DataFrame"
    assert result.shape == (241, 1), "Shape should be (241,1)"

    # Attribute is Formula
    result = asset.attributes[-8].current_value()
    assert type(result) == float, "Output type should be a float"

    # Attrobite is a Table lookup
    assert (
        type(asset.attributes[-11].current_value()) == datetime.datetime
    ), "Output type should be datetime.datetime"


def test_event_extracts(af_connect):
    """Test extraction functionalty for Events"""
    afdatabase, server = af_connect
    eventlist = afdatabase.find_events(
        query="*", starttime="2/10/2022 11:29:12", endtime="3/10/2022 17:18:44"
    )
    event = eventlist[0]

    # interpolated
    result = event.interpolated_values(
        tag_list=["SINUSOID"], interval="1h", dataserver=server
    )
    assert type(result) == pd.DataFrame, "Output type should be pd.DataFrame"
    assert result.shape == (16, 1), "Shape should be (16,1)"

    # recorded
    result = event.recorded_values(
        tag_list=["SINUSOID"],
        filter_expression="'SINUSOID' >= 0",
        dataserver=server,
    )
    assert (
        type(result) == dict
    ), "Output type should be a dict containing a pd.DataFrame"
    assert (
        type(result["SINUSOID"]) == pd.DataFrame
    ), "Output type should be a dict containing a pd.DataFrame"
    assert result["SINUSOID"].shape == (
        10,
        1,
    ), "Output shape should not (10,1)"

    # gives error when non-existent tag is used
    try:
        result = event.recorded_values(
            tag_list=["SINUSOIiD"],
            filter_expression="'SINUSOID' >= 0",
            dataserver=server,
        )
    except Exception as e:
        assert str(e) == "No tags were found for query: SINUSOIiD"

    # summary
    result = event.summary(
        tag_list=["SINUSOID"], summary_types=4 | 8 | 128, dataserver=server
    )
    assert (
        result.loc[(result["Summary"] == "MAXIMUM"), "Value"].iloc[0]
        == 99.1600341796875
    ), "Max shoud be 99.1600341796875"
    assert (
        result.loc[(result["Summary"] == "COUNT"), "Value"].iloc[0]
        == event.duration.total_seconds()
    )

    # filtered summaries
    # EventWeighted instead of TimeWeighted avoids issues with return values outside of filter range
    # But, returns no result if no events within the interval range
    result = event.filtered_summaries(
        tag_list=["SINUSOID"],
        interval="4h",
        summary_types=4 | 8,
        filter_expression="('SINUSOID' >= 20)",
        dataserver=server,
        calculation_basis=1,
    )
    assert type(result) == pd.DataFrame, "Output type should be pd.DataFrame"
    assert result.shape == (6, 4), "Shape should be (6,4)"
    assert result["Value"].min() == 24.235179901123047


def test_eventhierarchy(af_connect):
    """Test functionalty for EventHierarchy class"""
    afdatabase, server = af_connect
    eventlist = afdatabase.find_events(
        query="*", starttime="2/10/2022 11:29:12", endtime="3/10/2022 17:18:44"
    )
    eventhierarchy = eventlist.get_event_hierarchy(depth=2)
    assert (
        type(eventhierarchy) == pd.DataFrame
    ), "Output type should be pd.DataFrame"
    assert eventhierarchy.shape == (11, 7), "Shape should be (11,7)"

    # add attributes
    eventhierarchy = eventhierarchy.ehy.add_attributes(
        attribute_names_list=["Equipment", "Manufacturer"],
        template_name="Unit_template",
    )
    assert eventhierarchy.shape == (11, 9), "Shape should be (11,9)"
    assert (
        len(eventhierarchy["Equipment [Unit_template]"].unique()) == 3
    ), "Column should contain 3 unique values"

    # add referenced elements
    eventhierarchy = eventhierarchy.ehy.add_ref_elements(
        template_name="Operation_template"
    )
    assert eventhierarchy.shape == (11, 10), "Shape should be (11,10)"
    assert (
        len(eventhierarchy["Referenced_el [Operation_template](0)"].unique())
        == 3
    ), "Column should contain 3 unique values"

    # interpol extract - specify tag from list
    eventhierarchy_1 = eventhierarchy.copy()
    interpol_values_1 = eventhierarchy_1.ehy.interpol_discrete_extract(
        tag_list=["SINUSOID"], interval="1h", dataserver=server
    )
    assert interpol_values_1.shape == (154, 12), "shape should be (154, 12)"

    # interpol extract - specify tag from column
    eventhierarchy_2 = eventhierarchy.copy()
    eventhierarchy_2["Tag"] = "SINUSOID"

    interpol_values_2 = eventhierarchy_2.ehy.interpol_discrete_extract(
        tag_list=["Tag"], interval="1h", dataserver=server, col=True
    )
    assert (
        interpol_values_1.shape[0] == interpol_values_2.shape[0]
    ), "should have same length"

    # interpol extract - including non-existent tag, will return an Error
    eventhierarchy_3 = eventhierarchy.copy()
    eventhierarchy_3["Tag"] = "SINUSOID"
    eventhierarchy_3["Tag"].iloc[4] = "SINUSOIiD"  # non existing tag

    try:
        interpol_values_3 = eventhierarchy_3.ehy.interpol_discrete_extract(
            tag_list=["Tag"], interval="1h", dataserver=server, col=True
        )
    except Exception as e:
        assert str(e) == "No tags were found for query: SINUSOIiD"

    # summary extract - specify tag from list
    summary_values = eventhierarchy_1.ehy.summary_extract(
        tag_list=["SINUSOID"],
        summary_types=4 | 8 | 32,
        dataserver=server,
        col=False,
    )
    assert summary_values.shape == (33, 14), "shape should be (33, 14)"

    # summary extract - specify tag from column
    summary_values_2 = eventhierarchy_2.ehy.summary_extract(
        tag_list=["Tag"],
        summary_types=4 | 8 | 32,
        dataserver=server,
        col=True,
    )
    assert (
        summary_values.shape[0] == summary_values_2.shape[0]
    ), "should have same length"

    # interpol extract - including non-existent tag, will return an Error
    try:
        summary_values_3 = eventhierarchy_3.ehy.summary_extract(
            tag_list=["Tag"],
            summary_types=4 | 8 | 32,
            dataserver=server,
            col=True,
        )
    except Exception as e:
        assert str(e) == "No tags were found for query: SINUSOIiD"


def test_condensed(af_connect):
    """Test functionalty for CondensedHierarchy class"""
    afdatabase, server = af_connect
    eventlist = afdatabase.find_events(
        query="*", starttime="2/10/2022 11:29:12", endtime="3/10/2022 17:18:44"
    )
    eventhierarchy = eventlist.get_event_hierarchy(depth=2)

    # add attributes
    eventhierarchy = eventhierarchy.ehy.add_attributes(
        attribute_names_list=["Equipment", "Manufacturer"],
        template_name="Unit_template",
    )

    # add referenced elements
    eventhierarchy = eventhierarchy.ehy.add_ref_elements(
        template_name="Operation_template"
    )

    # create condensed dataframe
    condensed = eventhierarchy.ehy.condense()

    # interpol-disecrete extract, including filter expression, specify tag from list
    disc_interpol_values = condensed.ecd.interpol_discrete_extract(
        tag_list=["SINUSOID"],
        interval="1m",
        filter_expression="'SINUSOID' > 40",
        dataserver=server,
        col=False,
    )
    assert disc_interpol_values.shape == (1163, 4), "shape should be (1163,4)"
    assert (
        disc_interpol_values["SINUSOID"].min() == 40.02945
    ), "filtered minimum value should be 40.02945"

    # interpol-disecrete extract, including non-existent tag, will return an Error
    try:
        disc_interpol_values = condensed.ecd.interpol_discrete_extract(
            tag_list=["SINUSOIiD"],
            interval="1m",
            filter_expression="'SINUSOID' > 40",
            dataserver=server,
            col=False,
        )
    except Exception as e:
        assert str(e) == "No tags were found for query: SINUSOIiD"

    # add Tag columns
    condensed["Tag"] = "SINUSOID"
    condensed["Tag"].iloc[0] = "SINUSOIDU"

    # interpol-disecrete extract, including filter expression, specify tag from col
    disc_interpol_values = condensed.ecd.interpol_discrete_extract(
        tag_list=["Tag"],
        interval="1m",
        filter_expression="'SINUSOID' > 40",
        dataserver=server,
        col=True,
    )
    assert disc_interpol_values.shape == (1163, 5), "shape should be (1163,5)"
    assert (
        disc_interpol_values["Value"].min() == 3.387192
    ), "filtered minimum value should be 3.387192"

    # contin-disecrete extract, including filter expression
    disc_interpol_values = condensed.ecd.interpol_continuous_extract(
        tag_list=["SINUSOID"],
        interval="1m",
        filter_expression="'SINUSOID' > 40",
        dataserver=server,
    )
    assert disc_interpol_values.shape == (1161, 4), "shape should be (1161,4)"
    assert (
        disc_interpol_values["SINUSOID"].min() == 40.02945
    ), "filtered minimum value should be 40.02945"

    # recorded extract, including filter expression
    rec_values = condensed.ecd.recorded_extract(
        tag_list=["SINUSOID"],
        filter_expression="'SINUSOID' > 40",
        dataserver=server,
    )
    assert rec_values["EventFrames[Batch A]"]["SINUSOID"].shape == (
        13,
        3,
    ), "shape should be (13,3)"
    assert (
        rec_values["EventFrames[Batch A]"]["SINUSOID"]["Data"].min()
        == 67.43513
    ), "filtered minimum value should be 67.43513"

    # plot extract
    plot_values = condensed.ecd.plot_continuous_extract(
        tag_list=["SINUSOID"],
        nr_of_intervals=5,
        dataserver=server,
    )
    assert plot_values["EventFrames[Batch A]"]["SINUSOID"].shape == (
        15,
        3,
    ), "shape should be (13,3)"
    assert (
        plot_values["EventFrames[Batch A]"]["SINUSOID"]["Data"].min()
        == 0.7622223
    ), "minimum value should be 0.7622223"

    # summary extract, tags from taglist
    summary_values = condensed.ecd.summary_extract(
        tag_list=["SINUSOID"],
        summary_types=4 | 8 | 32,
        dataserver=server,
        col=False,
    )
    assert summary_values.shape == (9, 6), "shape should be (9,7)"
    assert (
        summary_values["Value"].min() == 0.7622223496437073
    ), "minimum value should be 0.7622223496437073"

    # ad Tag column to condensed dataframe
    condensed["Tag"] = "SINUSOID"

    # summary extract, tags from column
    summary_values = condensed.ecd.summary_extract(
        tag_list=["Tag"],
        summary_types=4 | 8 | 32,
        dataserver=server,
        col=True,
    )
    assert summary_values.shape == (9, 7), "shape should be (9,7)"
    assert (
        summary_values["Value"].min() == 0.7622223496437073
    ), "minimum value should be 0.7622223496437073"
