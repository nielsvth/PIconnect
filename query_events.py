# -*- coding: utf-8 -*-
"""
Created on Thu Mar 23 14:20:36 2023

@author: NVanthil
"""

import PIconnect
import pandas as pd

#Set up timezone info
PIconnect.PIConfig.DEFAULT_TIMEZONE = "Europe/Brussels"

#Initiate connection to PIserver & PI AF database
with PIconnect.PIAFDatabase(
        server='PIMS_EU_BEERSE_AF_PE', database='DeltaV-Events'
        ) as afdatabase, PIconnect.PIServer(server='ITSBEBEPIHISCOL') as server:
    
    # https://docs.aveva.com/bundle/af-sdk/page/html/T_OSIsoft_AF_Asset_AFSearchMode.htm
    #eventlist = afdatabase.find_events(query='*', 
    #                                   template_name='Phase',
    #                                   search_mode=PIconnect.PIConsts.SearchMode.OVERLAPPED)
    
    eventlist = afdatabase.find_events(query='HR102164G4-01*', 
                                       starttime='*-30d',
                                       endtime='*',
                                       template_name='Phase',
                                       search_mode=PIconnect.PIConsts.SearchMode.INCLUSIVE)
    
    
    # get hierarchy
    eventhierarchy = eventlist.get_event_hierarchy(depth=0)
