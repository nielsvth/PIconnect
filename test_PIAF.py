# -*- coding: utf-8 -*-
"""
Created on Tue Sep 27 09:43:44 2022

@author: NVanthil
"""

import PIconnect
import datetime
import ctypes

ctypes.WinDLL(
    r"C:\Program Files\Common Files\OSIsoft\PIInterop\2.0.4.0\PIInterop.dll"
)

# Set up timezone info
PIconnect.PIConfig.DEFAULT_TIMEZONE = "Europe/Brussels"

# Connection tests
assert len(PIconnect.PIServer.servers) >= 1
assert len(PIconnect.PIAFDatabase.servers) >= 1

# created AFDatabase & EventDatabase from '.XML' files and use default PIserver
# Every PIserver should have default SINUSOID Tag for testing purposes
with PIconnect.PIAFDatabase(
    server="ITSBEBEWSP06182 DEV", database="NuGreen"
) as afdatabase, PIconnect.PIServer() as server:

    # Asset Tests
    # Need to upload Assets & Templates seperatly as XML file
    assetlist = afdatabase.find_assets(query="Equipment")
    assert len(assetlist) == 11
    asset = assetlist[0].children[0]
    assert (
        str(asset)
        == r"Asset:\\ITSBEBEWSP06182 DEV\NuGreen\NuGreen\Wichita\Extruding Process\Equipment\B-334"
    )
    assert len(asset.attributes) == 21
    assert asset.get_attribute_values()["Plant"] == "Wichita"

    # Event tests
    # Need to upload events seperatly as XML file
    eventlist = afdatabase.find_events(
        query="*", start_time="*-1000d", end_time="*"
    )
    assert len(eventlist) == 6
    event = eventlist[0]
    assert event.name == "Unit 1"
    assert event.parent.name == "Batch A"
    assert type(event.starttime) == datetime.datetime
    assert type(event.duration) == datetime.timedelta
    assert event.template_name == "Unit_template"
    assert len(event.attributes) == 2
    assert event.ref_elements[0] == "P-560"
    assert event.get_attribute_values()["Manufacturer"] == "Sterns"

    # Attribute tests
    assert len(asset.attributes) == 21
    assert len(event.attributes) == 2
    attribute = asset.attributes[3]
    assert attribute.name == "Water Flow"
    assert attribute.source_type == "PI Point"
    assert (
        str(attribute.parent)
        == r"Asset:\\ITSBEBEWSP06182 DEV\NuGreen\NuGreen\Wichita\Extruding Process\Equipment\B-334"
    )

    # PIPoint Tests
    # Might be necessary to develop another method to find tags linked to an asset
    # Asset name potentially distinct from tag name
    taglist = server.find_tags("SINUSOID")
    assert len(taglist) == 1
