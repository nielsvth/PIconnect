# -*- coding: utf-8 -*-
"""
Created on Tue Sep 27 09:43:44 2022

@author: NVanthil
"""

#numpy
#pandas

import PIconnect
#from JanssenPI.PI import Tag
import ctypes
ctypes.WinDLL(r'C:\Program Files\Common Files\OSIsoft\PIInterop\2.0.4.0\PIInterop.dll')

#Set up timezone info
PIconnect.PIConfig.DEFAULT_TIMEZONE = 'Europe/Brussels'

#created AFDatabase from XML and used default PIserver
#Every PIserver should have default SINUSOID Tag for testing purposes
with PIconnect.PIAFDatabase(server='ITSBEBEWSP06182 DEV', database='NuGreen') as afdatabase, PIconnect.PIServer() as server:
    
    assetlist = afdatabase.find_assets(query="Equipment")
    print(len(assetlist))
    asset = assetlist[0].children[0]
    print(asset)
    print(asset.attributes)
    print(asset.get_attribute_values())
    
    #Might be necessary to develop another method to find tags linked to an asset
    #Asset name potentially distinct from tag name
    taglist = server.find_tags("SINUSOID")
    
    #Need to upload events seperatly as XML
    eventlist = afdatabase.find_events(query="*", start_time="*-1000d", end_time="*")
    print(eventlist)
    event = eventlist[0]
    print(event.name)
    print(event.parent.name)
    print(event.starttime)
    print(event.duration)
    print(event.template_name)
    print(event.attributes)
    print(event.ref_elements)
    print(event.get_attribute_values())
        