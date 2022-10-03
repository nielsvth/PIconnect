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
ctypes.WinDLL (r'C:\Program Files\Common Files\OSIsoft\PIInterop\2.0.4.0\PIInterop.dll')

#Set up timezone info
PIconnect.PIConfig.DEFAULT_TIMEZONE = 'Europe/Brussels'

with PIconnect.PIAFDatabase(server='PIMS_EU_BEERSE_AF_PE', database='DeltaV-Events') as afdatabase, PIconnect.PIServer(server='ITSBEBEPIHISCOL') as server:
    
    eventlist = afdatabase.find_events(query = 'RT000034*', start_time='*-10000d', search_full_hierarchy=True)
    eventhierarchy = eventlist.get_event_hierarchy(depth=3)
    
    eventhierarchy = eventhierarchy.ehy.add_attributes(['B_PH_INFO'], 'Phase')
    eventhierarchy = eventhierarchy.ehy.add_ref_elements('UnitProcedure')
    
    viewable1 = PIconnect.PI.view(eventhierarchy)
    
    condensed = eventhierarchy.ehy.condense()
    df_cond = condensed[(condensed['B_PH_INFO [Phase]'] >= 30020) & 
                        (condensed['B_PH_INFO [Phase]'] <= 40020) &
                        (condensed['Referenced_el [UnitProcedure](0)'].str.contains('R\d',regex=True))]
   
    df_cond['Tags'] = '100_' + df_cond['Referenced_el [UnitProcedure](0)'] + '_ST01'
    
    viewable2 = PIconnect.PI.view(df_cond)
    
    df_cond = df_cond.ecd.summary_extract(['Tags'], 4|8|16, server, col=True)
    
    viewable3 = PIconnect.PI.view(df_cond)