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
    assert len(event.attributes) == 2, "Should be 2"
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
    afdatabase = af_connect[0]
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
    afdatabase = af_connect[0]
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

    # specify tag from list
    eventhierarchy_1 = eventhierarchy.ehy.interpol_discrete_extract(
        tag_list=["SINUSOID"], interval="1h", dataserver=server
    )
    assert eventhierarchy_1.shape == (154, 12), "shape should be (154, 12)"

    # specify tag from column
    eventhierarchy_2 = eventhierarchy.copy()
    eventhierarchy_2["Tag"] = "SINUSOID"

    eventhierarchy_2 = eventhierarchy_2.ehy.interpol_discrete_extract(
        tag_list=["Tag"], interval="1h", dataserver=server, col=True
    )
    assert eventhierarchy_1.shape[0] == eventhierarchy_2[0], "should have same length"

    # Including non existent tag, will return no Error but a value Nan and time NaT
    eventhierarchy_3 = eventhierarchy.copy()
    eventhierarchy_3["Tag"] = "SINUSOID"
    eventhierarchy_3["Tag"].iloc[10] = "SINUSOIiD"

    eventhierarchy_3 = eventhierarchy_3.ehy.interpol_discrete_extract(
        tag_list=["Tag"], interval="1h", dataserver=server, col=True
    )
    assert eventhierarchy_3.shape == (140, 13), "shape should be (140, 13)"
    assert pd.isnull(eventhierarchy_3['Value'].iloc[-1]) == True



# created AFDatabase & EventDatabase from '.XML' files and use default PIserver
# Every PIserver should have default SINUSOID Tag for testing purposes
with PIconnect.PIAFDatabase(
    server="ITSBEBEWSP06182 DEV", database="NuGreen"
) as afdatabase, PIconnect.PIServer() as server:

    eventlist = afdatabase.find_events(
        query="*", starttime="2/10/2022 11:29:12", endtime="3/10/2022 17:18:44"
    )

    eventhierarchy = eventlist.get_event_hierarchy(depth=2)

    eventhierarchy = eventhierarchy.ehy.add_attributes(
        attribute_names_list=["Equipment", "Manufacturer"],
        template_name="Unit_template",
    )

    eventhierarchy = eventhierarchy.ehy.add_ref_elements(
        template_name="Operation_template"
    )

    # specify tag from list
    eventhierarchy_1 = eventhierarchy.ehy.interpol_discrete_extract(tag_list=["SINUSOID"], interval="1h", dataserver=server)
    assert eventhierarchy_1.shape == (154, 12), "shape should be (154, 12)"

    # specify tag from column
    eventhierarchy_2 = eventhierarchy.copy()
    eventhierarchy_2["Tag"] = "SINUSOID"

    eventhierarchy_2 = eventhierarchy_2.ehy.interpol_discrete_extract(
        tag_list=["Tag"], interval="1h", dataserver=server, col=True
    )
    print(eventhierarchy_2.shape)
    
    #non existent tag, will return no Error but a value Nan and time NaT
    eventhierarchy_2["Tag"].iloc[10] = "SINUSOIiD"

    eventhierarchy_2 = eventhierarchy_2.ehy.interpol_discrete_extract(
        tag_list=["Tag"], interval="1h", dataserver=server, col=True
    )
    assert eventhierarchy_2.shape == (140, 13)
    assert pd.isnull(eventhierarchy_2['Value'].iloc[-1]) == True



    ##condense check if template available in hierarchy, or use level
    # or remove option
