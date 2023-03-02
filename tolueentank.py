# -*- coding: utf-8 -*-
"""
Created on Wed Mar  1 10:23:09 2023

@author: NVanthil
"""

import PIconnect
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt


# Initiate connection to PI data server & PI AF database
with PIconnect.PIAFDatabase(
    server="PIMS_EU_BEERSE_AF_PE", database="DeltaV-Events"
) as afdatabase, PIconnect.PIServer(server="ITSBEBEPIHISCOL") as server:
        
    # Level Tag for GTP tolueen tank
    GTP_T15 = server.find_tags("100080T015LT01")[0]
    
    # WT Tags for GTP tolueen tank
    ALL_List = server.find_tags("100100T0*WT*")
    
    # Combine GTP and PTP tags in TagList
    ALL_List.append(GTP_T15)
    
    # frame for use with a rolling frame approach, frames of 1h
    frame_list = [
        "1/1/2021 00:00",
        "1/1/2021 00:05",
        "1/1/2021 00:10",
        "1/1/2021 00:15",
        "1/1/2021 00:20",
        "1/1/2021 00:25",
        "1/1/2021 00:30",
        "1/1/2021 00:35",
        "1/1/2021 00:40",
        "1/1/2021 00:45",
        "1/1/2021 00:50",
        "1/1/2021 00:55",
        ]
    
    table = pd.DataFrame()
    
    # moving frame by frame to capture all results
    for frame in frame_list:
    
        # Get one year of interpolated data at 1m intervals
        ALL_List_data = ALL_List.interpolated_values(starttime=frame, endtime="*", interval="1h")
        
        # remove string data (ERRORS)
        ALL_List_data = ALL_List_data.apply(pd.to_numeric, errors='coerce')
        
        # Density tolueen = 0.8669 kg/m³
        # convert volume to weight for comparison purposes
        ALL_List_data['100080T015LT01'] = ALL_List_data['100080T015LT01'] * 0.8669
        
        # get 15 min differences in level/weight
        diff = pd.DataFrame()
        for col in ALL_List_data.columns:
            new_name = col + '_diff'
            diff[new_name] = ALL_List_data[col].diff()
        
        
        # select relevant (>500L) increases in T15
        diff = diff[diff['100080T015LT01_diff'] >= 500]
        
        # Keep rows that loose a similar weight as is added in T15 in same 1h interval
        keep = []
        for i, row in diff.iterrows():
            for value in row[:-1]:
                if ((-value >= (row['100080T015LT01_diff']-200)) and
                    (-value <= (row['100080T015LT01_diff']+200))):
                    
                    #table with results
                    df = pd.DataFrame([[i, row[row==value].index[0], value, row['100080T015LT01_diff']]], 
                                      columns=['Time', 'Tank', 'PTP_out', 'GTP_in'])
                    
                    #concatenate
                    table = pd.concat([table, df], ignore_index=True)
                    
                    #print result
                    print(value, row['100080T015LT01_diff'], row[row==value].index[0], i)
                    
                    keep.append(i)
                
        res = diff.loc[keep]

    # get rolling correlations
    #cor = pd.DataFrame()
    #for col in ALL_List_data.columns:
    #    new_name = col + '_cor'
    #    cor[new_name] = ALL_List_data[col].rolling(10).corr(ALL_List_data['100080T015LT01'])
    
    # by hour times only to avoid redundancy 
    table = table.assign(Time=table.Time.dt.round('H'))
    
    # keep only duplicates to filter noise
    table = table.loc[table.duplicated(subset=['Time', 'Tank'], keep='first')]
    
    #export
    table.to_csv('GTP15_events.csv')
    
    ALL_List_data.plot(figsize=(30, 6))
    plt.legend(bbox_to_anchor=(1.02, 1), loc="upper left")
    



        