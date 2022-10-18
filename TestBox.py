# -*- coding: utf-8 -*-
"""
Created on Tue Sep 27 09:43:44 2022

@author: NVanthil
"""

# numpy
# pandas

from zipfile import ZIP_BZIP2
import PIconnect

# from JanssenPI.PI import Tag
import ctypes

ctypes.WinDLL(
    r"C:\Program Files\Common Files\OSIsoft\PIInterop\2.0.4.0\PIInterop.dll"
)

# Set up timezone info
PIconnect.PIConfig.DEFAULT_TIMEZONE = "Europe/Brussels"

# created AFDatabase from XML and used default PIserver
# Every PIserver should have default SINUSOID Tag for testing purposes
with PIconnect.PIAFDatabase(
    server="ITSBEBEWSP06182 DEV", database="NuGreen"
) as afdatabase, PIconnect.PIServer() as server:

    assetlist = afdatabase.find_assets(query="Equipment")
    asset = assetlist[0].children[0]

    # --> need for an Attribute class
    # print(asset.attributes)
    # print(asset.attributes[3].pipoint)

    # print(asset.attributes[3].af_attribute.DataReference.Name)
    # print(asset.attributes[3].af_attribute.DataReferencePlugIn.Name)

    print(
        asset.attributes[3].af_attribute.Element
    )  # asset object (could also be argument?+-67 5)

    eventlist = afdatabase.find_events(
        query="*", start_time="*-1000d", end_time="*"
    )
    event = eventlist[0]

    print(event.attributes[1])

    print(event.attributes[1].parent)  # eventframe object
    print(asset.attributes[3].parent)

    [x.current_value() for x in event.attributes]
