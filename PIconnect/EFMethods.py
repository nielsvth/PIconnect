import pandas as pd
import numpy as np
from OSIsoft import AF
from datetime import datetime
import pytz
from JanssenPI.config import PIConfig

def attributes(eventframe):
    ''''Return list of attribute names for an eventframe'''
    return [attribute.Name for attribute in eventframe.Attributes]

def get_attribute_values(eventframe, attribute_list):
    ''''Return dict of attribute values for specified attributes'''
    attribute_dct={}
    for attribute in eventframe.Attributes:
        if attribute.Name in attribute_list:
            attribute_dct[attribute.Name] = attribute.GetValue().Value
    return attribute_dct

def attributes_dct(frame):
    ''''Return list of attribute names for an eventframe'''
    dct = {}
    for attribute in frame.Attributes:
        try:
            #convert to Name or string takes a LOT of time
            dct[attribute.Name] = attribute.PIPoint
        except:
            pass
    return dct


def lambda_attrib(x, attrib):
    try:
        attribute_value = [attribute.GetValue().Value for attribute in x.Attributes if attribute.Name == attrib][0]
        return attribute_value
    except:
        return np.nan

def add_attributes(event_dataframe, attribute_list, template):
    ''''Return dataframe with added columns for specified attributes and for specified level'''
    print('Fetching attribute(s)...')
    if type(template) == int:
        template = event_dataframe.loc[event_dataframe['Level']==template, 'Template'].iloc[0]
        
    for i, attribute in enumerate(attribute_list):
        event_dataframe[attribute+' ['+str(template)+']'] = event_dataframe.loc[event_dataframe['Template']==template, 'AFEvent'].apply(lambda x: lambda_attrib(x, attribute)) 
    
    for colname in event_dataframe.columns:
        try:
            event_dataframe[colname] = event_dataframe[colname].astype(float)
        except:
            pass
    return event_dataframe

def add_referenced_elements(event_dataframe, template):
    print('Fetching referenced element(s)...')
    ''''Return dataframe with added columns for references elements for specified level'''
    if type(template) == int:
        template = event_dataframe.loc[event_dataframe['Level']==template, 'Template'].iloc[0]

    ref_el = event_dataframe.loc[event_dataframe['Template']==template, 'AFEvent'].apply(lambda x: [ref_el.Name for ref_el in x.ReferencedElements]).apply(pd.Series)
    for col in ref_el.columns:
        event_dataframe['Referenced_el'+' ['+str(template)+']'+'('+str(col)+')'] = ref_el[col]
    return event_dataframe       

def condense(event_dataframe, level=10):
    ''''Return condensed dataframe for event dataframe, up to specified level'''
    print('Condensing...')
    if type(level) == str:
        level = event_dataframe.loc[event_dataframe['Template']==level, 'Level'].iloc[0]
    
    event_dataframe = event_dataframe[event_dataframe['Level']<=level].copy()
    #merge level by level
    for level, df_level in event_dataframe.groupby('Level'):
        #remove empty columns
        df_level.dropna(how='all', axis=1, inplace=True)
        #get template name
        temp = df_level['Template'].unique()[0] 
        #add auxiliary columns for merge based on path
        cols = [x for x in range(int(level)+1)]
        df_level[cols] = df_level['Path'].str.split('\\', expand=True).loc[:, 4:]
        #remove Path columns
        df_level.drop(['Path'], 1, inplace=True)
        #rename columns, ignore columns with number names
        df_level.columns = [col_name + ' [' + temp + ']' if not ((type(col_name) == int) or ('[' in col_name)) else col_name for col_name in df_level.columns]
        #merge with previous level
        if level == 0:
            df_condensed = df_level
        else:
            df_condensed = pd.merge(df_condensed, df_level, how='right', left_on=cols[:-1], right_on=cols[:-1])
    #drop auxiliary columns 
    df_condensed.drop([col_name for col_name in df_condensed.columns if type(col_name) == int], 1, inplace=True)
    #remove duplicates
    df_condensed = df_condensed.drop_duplicates(keep='first')
    
    #address NaT times (copy value from parent layer)
    endtime_cols = [col_name for col_name in df_condensed.columns if col_name.startswith('Endtime')]
    for i, col in enumerate(endtime_cols):
        if i == 0: #Handle naT in top layer: current time
            local_tz = pytz.timezone(PIConfig.DEFAULT_TIMEZONE)
            now = datetime.utcnow().replace(tzinfo=pytz.utc).astimezone(local_tz)
            df_condensed[col].fillna(now, inplace=True)
        else: #handle naT in lower layers by inheriting from parent
            df_condensed[col].fillna(df_condensed[endtime_cols[i-1]], inplace=True)
    return df_condensed

def view(event_dataframe):
    #if column name starts with AF/PI > make content string
    '''returns viewable version of event dataframe'''
    event_dataframe = event_dataframe.copy() #needs to return a copy
    for colname in event_dataframe.loc[:, ~event_dataframe.columns.isin(['Starttime','Endtime'])]:
        event_dataframe[colname] = event_dataframe[colname].astype(str)
        try:
            event_dataframe[colname] = event_dataframe[colname].astype(float)
        except:
            pass
    return event_dataframe
        