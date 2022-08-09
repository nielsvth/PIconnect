""" PIAF
    Core containers for connections to the PI Asset Framework.
"""
# pragma pylint: disable=unused-import, redefined-builtin
from __future__ import absolute_import, division, print_function, unicode_literals

from builtins import (
    ascii,
    bytes,
    chr,
    dict,
    filter,
    hex,
    input,
    int,
    list,
    map,
    next,
    object,
    oct,
    open,
    pow,
    range,
    round,
    str,
    super,
    zip,
)

import clr
clr.AddReference('System.Collections')
from System.Collections.Generic import List
import pandas as pd
import numpy as np

from collections import UserList

try:
    from __builtin__ import str as BuiltinStr
except ImportError:
    BuiltinStr = str
# pragma pylint: enable=unused-import, redefined-builtin
from warnings import warn

from JanssenPI._utils import classproperty
from JanssenPI.AFSDK import AF
from JanssenPI.PIConsts import EventFrameSearchMode, SearchMode, SortField, SortOrder, SummaryType, CalculationBasis, TimestampCalculation, BoundaryType, ExpressionSampleType, SearchField
from JanssenPI.time import timestamp_to_index, add_timezone
from JanssenPI.config import PIConfig
from JanssenPI.PI import Tag, generate_pipointlist, convert_to_TagList

from pytz import timezone, utc
from datetime import datetime, timedelta

_NOTHING = object()


class PIAFDatabase(object):
    """PIAFDatabase

    Context manager for connections to the PI Asset Framework database.
    """

    version = "0.2.0"

    _servers = _NOTHING
    _default_server = _NOTHING

    def __init__(self, server=None, database=None):
        self.server = None
        self.database = None
        self._initialise_server(server)
        self._initialise_database(database)

    @classproperty
    def servers(self):
        if self._servers is _NOTHING:
            i, j, failed_servers, failed_databases = 0, 0, 0, 0
            self._servers = {}
            from System import Exception as dotNetException  # type: ignore

            for i, s in enumerate(AF.PISystems(), start=1):
                try:
                    self._servers[s.Name] = {"server": s, "databases": {}}
                    for j, d in enumerate(s.Databases, start=1):
                        try:
                            self._servers[s.Name]["databases"][d.Name] = d
                        except Exception:
                            failed_databases += 1
                        except dotNetException:
                            failed_databases += 1
                except Exception:
                    failed_servers += 1
                except dotNetException:
                    failed_servers += 1
            if failed_servers or failed_databases:
                warn(
                    "Failed loading {}/{} servers and {}/{} databases".format(
                        failed_servers, i, failed_databases, j
                    )
                )
        return self._servers

    @classproperty
    def default_server(self):
        if self._default_server is _NOTHING:
            self._default_server = None
            if AF.PISystems().DefaultPISystem:
                self._default_server = self.servers[AF.PISystems().DefaultPISystem.Name]
            elif len(self.servers) > 0:
                self._default_server = self.servers[list(self.servers)[0]]
            else:
                self._default_server = None
        return self._default_server

    def _initialise_server(self, server):
        if server and server not in self.servers:
            message = 'Server "{server}" not found, using the default server.'
            warn(message=message.format(server=server), category=UserWarning)
        server = self.servers.get(server, self.default_server)
        self.server = server["server"]

    def _initialise_database(self, database):
        server = self.servers.get(self.server.Name)
        if not server["databases"]:
            server["databases"] = {x.Name: x for x in self.server.Databases}
        if database and database not in server["databases"]:
            message = 'Database "{database}" not found, using the default database.'
            warn(message=message.format(database=database), category=UserWarning)
        default_db = self.server.Databases.DefaultDatabase
        self.database = server["databases"].get(database, default_db)

    def __enter__(self):
        self.server.Connect()
        return self

    def __exit__(self, *args):
        pass
        # Disabled disconnecting because garbage collection sometimes impedes
        # connecting to another server later
        # self.server.Disconnect()
        self.server.Refresh()
        self.database.Refresh()

    def __repr__(self):
        return "%s(\\\\%s\\%s)" % (
            self.__class__.__name__,
            self.server_name,
            self.database_name,
        )

    @property
    def server_name(self):
        """Return the name of the connected PI AF server."""
        return self.server.Name

    @property
    def database_name(self):
        """Return the name of the connected PI AF database."""
        return self.database.Name

    @property
    def children(self):
        """Return a dictionary of the direct child elements of the database."""
        return {c.Name: c for c in self.database.Elements}

    def descendant(self, path):
        """Return a descendant of the database from an exact path."""
        return PIAFElement(self.database.Elements.get_Item(path))
    
    #https://docs.osisoft.com/bundle/af-sdk/page/html/M_OSIsoft_AF_EventFrame_AFEventFrame_FindEventFrames_1.htm
    #https://docs.osisoft.com/bundle/af-sdk/page/html/T_OSIsoft_AF_AFSearchField.htm could be implemented    
    #https://pisquare.osisoft.com/s/Blog-Detail/a8r1I000000GvThQAK/using-the-afeventframesearch-class #> attributequery
    
    #add option to input list like PIpoint search -------------------------
    
    def find_events(self,
        query=None,
        asset='*',
        start_time=None,
        end_time='*',
        template_name = None,
        start_index=0,
        max_count=1000000,
        search_mode=SearchMode.OVERLAPPED,
        search_full_hierarchy=True,
        sortField=SortField.STARTTIME,
        sortOrder=SortOrder.ASCENDING):
        '''Return a EventList of Events the meet query'''
        
        if template_name:
            try:
                afsearchField = SearchField.NAME
                template = AF.Asset.AFElementTemplate.FindElementTemplates(self.database,
                                                                           template_name,
                                                                            afsearchField,
                                                                            sortField,
                                                                            sortOrder,
                                                                            max_count)[0]
            except:
                raise AttributeError('Template name was not found')
        else:
            template = None
        
        if not start_time:
            start_time = AF.Time.AFTime.Now
               
        lst = AF.EventFrame.AFEventFrame.FindEventFrames(
                self.database,
                None,
                search_mode,
                start_time,
                end_time,
                query,
                asset,
                None,
                template,
                None,
                None,
                search_full_hierarchy,
                sortField,
                sortOrder,
                start_index,
                max_count)
                
        return EventList([Event(event) for event in lst])

    #find events by path, attribute, referenced element(done) ---------------
    
    #https://docs.osisoft.com/bundle/af-sdk/page/html/M_OSIsoft_AF_Asset_AFElement_FindElements_2.htm
    def find_assets(self,
                query = None,
                top_asset = None,
                searchField = SearchField.NAME,
                search_full_hierarchy = True, 
                sortField = SortField.STARTTIME, 
                sortOrder = SortOrder.ASCENDING, 
                Int32 = 10000000):
        '''Return list of Assets that meet query'''
        
        lst = AF.Asset.AFElement.FindElements(self.database, 
                                              top_asset, 
                                              query, 
                                              searchField, 
                                              search_full_hierarchy, 
                                              sortField, 
                                              sortOrder, 
                                              Int32)
        return [Asset(x) for x in lst]

    def all_assets(self, af_asset_list=[], depth=10):
        '''return dataframe with hierarchical AF Asset structure'''
        
        #None of these https://docs.osisoft.com/bundle/af-sdk/page/html/T_OSIsoft_AF_Asset_AFElement.htm FindElements methods works
        #use AFDatabase.children to select starting element(s) of interest
        #elements = self.database.Elements
        
        if af_asset_list == []:
            af_asset_list = self.children.values()
        
        clist = List[AF.Asset.AFElement]() #create generic list
        for af_asset in af_asset_list:
            clist.Add(af_asset)
        df_roots = pd.DataFrame([(Asset(y), y.GetPath()) for y in clist], columns=['Asset', 'Path'])
        
        if len(clist) > 0:
            print('Fetching AF Asset element data')
            #https://docs.osisoft.com/bundle/af-sdk/page/html/M_OSIsoft_AF_Asset_AFElement_LoadElementsToDepth.htm
            asset_depth = AF.Asset.AFElement.LoadElementsToDepth(clist, False, depth, 1000000)
        
            if len(asset_depth) > 0:
                df_assets = pd.DataFrame([(Asset(y), y.GetPath()) for y in asset_depth], columns=['Asset', 'Path'])
            else:
                df_assets = pd.DataFrame()
            
            #concatenate procedures and child event frames
            df_assets = pd.concat([df_roots, df_assets], ignore_index=True)  

            df_assets['Name'] = df_assets['Asset'].apply(lambda x: x.name if x else np.nan)
            df_assets['Template'] = df_assets['Asset'].apply(lambda x: x.template_name if x else np.nan)
            df_assets['Level'] = df_assets['Path'].str.count(r'\\').apply(lambda x: x-4)
            #print('This Asset Frame has structure of "\\\\Server\\Database\\{}"'.format('\\'.join([str(el) for el in df_assets['Template'].unique()])))
            return df_assets
        else:
            return pd.DataFrame(columns=['Asset', 'Path', 'Name', 'Template', 'Level']) 
    
    
    
class Event:
    '''Container for Event object'''
        
    def __init__(self, event):
        self.eventframe = event
        #self.afcontainer = AF.AFNamedCollectionList[AF.EventFrame.AFEventFrame]() #empty container
        #self.afcontainer.Add(self.eventframe)
    
    def __repr__(self):
        return ('Event:' + self.eventframe.GetPath() )
    
    def __str__(self):
        return ('Event:' + self.eventframe.GetPath() )
    
    #Properties
    @property
    def name(self):
        '''Return name of event'''
        return self.eventframe.Name
    @property
    def path(self):
        '''Return path'''
        return self.eventframe.GetPath() 
    @property
    def pisystem_name(self):
        '''Return PISystem name'''
        return self.eventframe.PISystem.Name
    @property
    def database_name(self):
        '''Return database name'''
        return self.eventframe.Database.Name
    @property
    def database(self):
        '''Return PIAFDatabase object'''
        return PIAFDatabase(server=self.pisystem_name, database=self.database_name)
    @property
    def af_eventframe(self):
        '''Return AFEventFrame object'''
        return self.eventframe
    @property
    def af_template(self):
        '''Return AFTemplate'''
        return self.eventframe.Template
    @property
    def template_name(self):
        '''Return template name'''
        return self.eventframe.Template.Name 
    @property
    def starttime(self):
        '''Return starttime of event'''
        return timestamp_to_index(self.eventframe.StartTime.UtcTime)
    @property
    def endtime(self):
        '''Return starttime of event'''
        return timestamp_to_index(self.eventframe.EndTime.UtcTime)
    @property
    def af_timerange(self):
        '''Return AFTimerange for event'''
        return self.eventframe.TimeRange
    @property
    def attributes(self):
        ''''Return list of attribute names for event'''
        return [attribute.Name for attribute in self.eventframe.Attributes]
    @property
    def af_attributes(self):
        ''''Return list of AFAttributes for event'''
        return [attribute for attribute in self.eventframe.Attributes]  
    @property
    def ref_elements(self):
        '''Return list of references elements for event'''
        return [ref_el.Name for ref_el in self.eventframe.ReferencedElements]
    @property
    def children(self):
        '''Return EventList of children for event'''
        return EventList([Event(event) for event in self.eventframe.EventFrames])
    @property
    def parent(self):
        '''Return parent event for event'''
        return Event(self.eventframe.Parent)
    @property
    def description(self):
        '''Return description for event'''
        return self.eventframe.Description
    @property
    def duration(self):
        '''Return duration of event as datetime.timedelta object'''
        try:
            return self.endtime - self.starttime
        except: #NaT endtime
            #return timedelta.max
            local_tz = timezone(PIConfig.DEFAULT_TIMEZONE)
            return (datetime.utcnow().replace(tzinfo=utc).astimezone(local_tz) - self.starttime)
         
    @property
    def procedure(self):
        '''Return top-level procedure name for this event'''
        return self.path.strip('\\').split('\\')[2]
    
    #Methods
    def plot_values(self, tag_list, nr_of_intervals, dataserver=None):
        '''Retrieves values over the specified time range suitable for plotting over the number of intervals (typically represents pixels)
        Returns a Dictionary of DataFrames for Tags in Taglist with values that will produce the most accurate plot over the time range while minimizing the amount of data returned.'''
        taglist = convert_to_TagList(tag_list, dataserver)
        return taglist.plot_values(self.starttime, self.endtime, nr_of_intervals)
    
    def interpolated_values(self, tag_list, interval, dataserver=None, filter_expression=''):
        ''''Return Dataframe of interpolated values for tags specified by list of tagnames or PIPoint, between starttime and endtime'''
        taglist = convert_to_TagList(tag_list, dataserver)
        if type(self.endtime) == float:
            local_tz = timezone(PIConfig.DEFAULT_TIMEZONE)
            endtime = datetime.utcnow().replace(tzinfo=utc).astimezone(local_tz)
            return taglist.interpolated_values(self.starttime, endtime, interval)
        return taglist.interpolated_values(self.starttime, self.endtime, interval)
        
    def recorded_values(self, tag_list, dataserver=None, filter_expression='', AFBoundaryType=BoundaryType.INSIDE):
        ''''Return Dataframe of recorded values for tags specified by list of tagnames or PIPoint, between starttime and endtime'''
        taglist = convert_to_TagList(tag_list, dataserver)
        return taglist.recorded_values(self.starttime, self.endtime)

    def summary(self, tag_list, summary_types, dataserver=None, calculation_basis=CalculationBasis.TIME_WEIGHTED, time_type=TimestampCalculation.AUTO):
        '''Return specified summary measure(s) for event
        
        Summary_types are defined as integers separated by '|'
        fe: to extract min and max >> event.summary(['tag_x'], dataserver, 4|8) 
        
        Integer values for all summary measures are specified below:
        
        - TOTAL = 1: A total over the time span
        - AVERAGE = 2: Average value over the time span
        - MINIMUM = 4: The minimum value in the time span
        - MAXIMUM = 8: The maximum value in the time span
        - RANGE = 16: The range of the values (max-min) in the time span
        - STD_DEV = 32 : The sample standard deviation of the values over the time span
        - POP_STD_DEV = 64: The population standard deviation of the values over the time span
        - COUNT = 128: The sum of the event count (when the calculation is event weighted). The sum of the event time duration (when the calculation is time weighted.)
        - PERCENT_GOOD = 8192: The percentage of the data with a good value over the time range. Based on time for time weighted calculations, based on event count for event weigthed calculations.
        - TOTAL_WITH_UOM = 16384: The total over the time span, with the unit of measurement that's associated with the input (or no units if not defined for the input)
        - ALL = 24831: A convenience to retrieve all summary types
        - ALL_FOR_NON_NUMERIC = 8320: A convenience to retrieve all summary types for non-numeric data'''
        
        taglist = convert_to_TagList(tag_list, dataserver)
        return taglist.summary(self.starttime, self.endtime, summary_types, calculation_basis, time_type)
    
    #summaries
    def summaries(self, tag_list, interval, summary_types, dataserver=None, calculation_basis=CalculationBasis.TIME_WEIGHTED, time_type=TimestampCalculation.AUTO):
        '''Return one or more summary values for Tags in Taglist, for each interval within the specified event duration'''
        taglist = convert_to_TagList(tag_list, dataserver)
        return taglist.summaries(self.starttime, self.endtime, interval, summary_types, calculation_basis, time_type)
        
    #filtered summaries
    def filtered_summaries(self, tag_list, interval,summary_types,
                           filter_expression,
                           dataserver=None,
                           calculation_basis=CalculationBasis.TIME_WEIGHTED, 
                           time_type=TimestampCalculation.AUTO, 
                           AFfilter_evaluation=ExpressionSampleType.EXPRESSION_RECORDED_VALUES, 
                           filter_interval=None):
        '''Return one or more summary values for Tags in Taglist, (Optional: for each interval) within event duration'''
        taglist = convert_to_TagList(tag_list, dataserver)
        return taglist.filtered_summaries(self.starttime, self.endtime, interval,summary_types, filter_expression,
                                          calculation_basis, time_type, AFfilter_evaluation, filter_interval)
    
    def get_attribute_values(self, attribute_names_list=[]):
        '''Return dict of attribute values for specified attributes'''
        attribute_dct={}
        if not attribute_names_list:
            for attribute in self.eventframe.Attributes:
                attribute_dct[attribute.Name] = attribute.GetValue().Value
            return attribute_dct
        else:
            for attribute in self.eventframe.Attributes:
                if attribute.Name in attribute_names_list:
                    attribute_dct[attribute.Name] = attribute.GetValue().Value
            return attribute_dct
    
    def get_event_hierarchy(self, depth=10): ##### lookup by template instead of number? - Level more constant then templates?
        '''Return dataframe of event Hierarchy'''
        df_procedures = pd.DataFrame([(self.eventframe, self.eventframe.GetPath())], columns=['Event', 'Path'])

        print('Fetching hierarchy data for Event...')
        #https://docs.osisoft.com/bundle/af-sdk/page/html/M_OSIsoft_AF_EventFrame_AFEventFrame_LoadEventFramesToDepth.htm
        event_depth = AF.EventFrame.AFEventFrame.LoadEventFramesToDepth(self.afcontainer, False, depth, 1000000)

        if len(event_depth) > 0:
            df_events = pd.DataFrame([(y, y.GetPath()) for y in event_depth], columns=['Event', 'Path'])
        else:
            df_events = pd.DataFrame()
        
        #concatenate procedures and child event frames
        df_events = pd.concat([df_procedures, df_events], ignore_index=True)    
        
        df_events['Event'] = df_events['Event'].apply(lambda x: Event(x))
        df_events['Name'] = df_events['Event'].apply(lambda x: x.name if x else np.nan)
        df_events['Template'] = df_events['Event'].apply(lambda x: x.template_name if x.af_template else np.nan)
        df_events['Level'] = df_events['Path'].str.count(r'\\').apply(lambda x: x-4)
        df_events['Starttime'] = df_events['Event'].apply(lambda x: x.starttime if x else np.nan)
        df_events['Endtime'] = df_events['Event'].apply(lambda x: x.endtime if x else np.nan)        

        print('This Event Hierarchy has structure of "\\\\Server\\Database\\{}"'.format('\\'.join([str(ev) for ev in df_events['Template'].unique()]))) #not really correct when different templates on a level
        return df_events



class EventList(UserList):
    '''Container for EventList object'''
    
    def __init__(self, data):
        self.data = data #list of Events
        #validation step---
        
    def __repr__(self):
        return str([event for event in self.data])
    
    def __str__(self):
        return str([event for event in self.data])

    #Methods
    def to_set(self):
        '''Return eventlist as list'''
        return set(self.data)
       
    def get_event_hierarchy(self, depth=10):
        '''Return dataframe of event Hierarchy'''
        #https://docs.osisoft.com/bundle/af-sdk/page/html/T_OSIsoft_AF_AFNamedCollectionList_1.htm
        afcontainer = AF.AFNamedCollectionList[AF.EventFrame.AFEventFrame]() #empty container
        for event in self.data:
            try:
                afcontainer.Add(event.af_eventframe)
            except:
                raise('Failed to process event {}'.format(event))
        
        if len(afcontainer) > 0:
            df_procedures = pd.DataFrame([(y, y.GetPath()) for y in afcontainer], columns=['Event', 'Path'])

            print('Fetching hierarchy data for {} Event(s)...'.format(len(afcontainer)))
            #https://docs.osisoft.com/bundle/af-sdk/page/html/M_OSIsoft_AF_EventFrame_AFEventFrame_LoadEventFramesToDepth.htm
            event_depth = AF.EventFrame.AFEventFrame.LoadEventFramesToDepth(afcontainer, False, depth, 1000000)

            if len(event_depth) > 0:
                df_events = pd.DataFrame([(y, y.GetPath()) for y in event_depth], columns=['Event', 'Path'])
            else:
                df_events = pd.DataFrame()
            
            #concatenate procedures and child event frames
            df_events = pd.concat([df_procedures, df_events], ignore_index=True)    
            
            df_events['Event'] = df_events['Event'].apply(lambda x: Event(x))
            df_events['Name'] = df_events['Event'].apply(lambda x: x.name if x else np.nan)
            df_events['Template'] = df_events['Event'].apply(lambda x: x.template_name if x.af_template else np.nan)
            df_events['Level'] = df_events['Path'].str.count(r'\\').apply(lambda x: x-4)
            df_events['Starttime'] = df_events['Event'].apply(lambda x: x.starttime if x else np.nan)
            df_events['Endtime'] = df_events['Event'].apply(lambda x: x.endtime if x else np.nan)

            print('This Event Hierarchy has structure of "\\\\Server\\Database\\{}"'.format('\\'.join([str(ev) for ev in df_events['Template'].unique()]))) #not completely correct if different templates on a single level
            return df_events

        else:
            return pd.DataFrame(columns=['Event', 'Path', 'Name', 'Level', 'Template', 'Starttime', 'Endtime']) 

try:
    #delete the accessor to avoid warning 
    del pd.DataFrame.ehy
except AttributeError:
    pass

#https://pandas.pydata.org/docs/development/extending.html
#DataFrames are not meant to be subclassed, but you can implement your own functionality via the extension API.
@pd.api.extensions.register_dataframe_accessor("ehy")
class EventHierarchy:
    '''Additional functionality for pd.DataFrame object, for working with EventHierarchies'''
    
    def __init__(self, df):
        self.validate(df)
        self.df = df
    
    @staticmethod
    def validate(df):
        '''Validate object meets requirements for EventHierarchy'''
        #verify that dataframe fits EventHierarchy requirements
        if not {'Event', 'Path', 'Name', 'Template', 'Level', 'Starttime', 'Endtime'}.issubset(set(df.columns)):
            raise AttributeError("This dataframe does not have the correct EventHierarchy format")
    
    #Methods
    def add_attributes(self, attribute_names_list, template_name):
        ''''Add attributtes to EventHierarchy for specified attributes and template/level'''
        print('Fetching attribute(s)...')
        if type(template_name) == int:
            template_name = self.df.loc[self.df['Level']==template_name, 'Template'].iloc[0]
            
        for attribute in attribute_names_list:
            self.df[attribute+' ['+str(template_name)+']'] = self.df.loc[self.df['Template']==template_name, 'Event'].apply(lambda x: x.get_attribute_values([attribute])[attribute]) 
        
        for colname in self.df.columns:
            try:
                self.df[colname] = self.df[colname].astype(float)
            except:
                pass
        return self.df
            
    def add_ref_elements(self, template_name):
        print('Fetching referenced element(s)...')
        ''''Add referenced elements to EventHierarchy for specified template/level'''
        if type(template_name) == int:
            template_name = self.df.loc[self.df['Level']==template_name, 'Template'].iloc[0]

        ref_el = self.df.loc[self.df['Template']==template_name, 'Event'].apply(lambda x: x.ref_elements).apply(pd.Series)
        
        if ref_el.empty:
            raise AttributeError('No results found for the specified template')
        
        for col in ref_el.columns:
            self.df['Referenced_el'+' ['+str(template_name)+']'+'('+str(col)+')'] = ref_el[col]
        return self.df
     
    def condense(self):
        ''''Return condensed dataframe based on events in EventHierarchy'''
        print('Condensing...')
        
        df = self.df.copy()
        
        #merge level by level
        for level in range(int(df['Level'].min()), int(df['Level'].max()+1)):
            #subdf per level
            df_level = df[df['Level'] == level]
            #remove empty columns
            df_level.dropna(how='all', axis=1, inplace=True)
            if df_level.empty:
                df_condensed[level] = 'TempValue'
            else:
                #add auxiliary columns for merge based on path
                cols = [x for x in range(level+1)]
                df_level[cols] = df_level['Path'].str.split('\\', expand=True).loc[:, 4:]
                #remove Path columns
                df_level.drop(['Path'], 1, inplace=True)
                #rename columns, ignore columns with number names
                df_level.columns = [col_name + ' [' + str(int(level)) + ']' if not ((type(col_name) == int) or ('[' in col_name)) else col_name for col_name in df_level.columns]
                #merge with previous level
                if level == int(df['Level'].min()):
                    df_condensed = df_level
                else:
                    df_condensed = pd.merge(df_condensed, df_level, how='outer', left_on=cols[:-1], right_on=cols[:-1])
        #drop auxiliary columns 
        df_condensed.drop([col_name for col_name in df_condensed.columns if type(col_name) == int], 1, inplace=True)
        #remove duplicates
        df_condensed = df_condensed.drop_duplicates(keep='first')
        
        #address NaT times (copy value from parent layer)
        endtime_cols = [col_name for col_name in df_condensed.columns if col_name.startswith('Endtime')]
        for i, col in enumerate(endtime_cols):
            if i == 0: #Handle naT in top layer: current time
                local_tz = timezone(PIConfig.DEFAULT_TIMEZONE)
                now = datetime.utcnow().replace(tzinfo=utc).astimezone(local_tz)
                df_condensed[col].fillna(now, inplace=True)
            else: #handle naT in lower layers by inheriting from parent
                df_condensed[col].fillna(df_condensed[endtime_cols[i-1]], inplace=True)
                
        return df_condensed
    
    def interpol_discrete_extract(self, tag_list, interval, dataserver=None, col=False):
        '''Return dataframe of interpolated data for discrete events of EventHierarchy'''
        print('building discrete extract table from EventHierachy...')
        df = self.df.copy()
        
        #performance checks
        maxi = max(df['Event'].apply(lambda x: x.duration))
        if maxi > pd.Timedelta("60 days"):
            print (f'Large Event(s) with duration up to {maxi} detected, Note that this might take some time...')
        if len(df) > 50:
            print(f'Extracts will be made for {len(df)} Events, Note that this might take some time...')
        
        if col == False:
            taglist = convert_to_TagList(tag_list, dataserver)
            #extract interpolated data for discrete events
            df['Time'] = df['Event'].apply(lambda x: list(x.interpolated_values(taglist, interval).to_records(index=True)))
        
        if col == True:
            if len(tag_list) > 1:
                raise AttributeError (f'You can only specify a single tag column at a time')
            if tag_list[0] in df.columns:
                event = df.columns.get_loc('Event')
                tags = df.columns.get_loc(tag_list[0])
                #extract interpolated data for discrete events
                df['Time'] = df.apply(lambda row: list(row[event].interpolated_values([row[tags]], interval).to_records(index=True)), axis=1)
            else:
                raise AttributeError (f'The column option was set to True, but {tag_list} is not a valid column')
        
        df = df.explode('Time') #explode list to rows
        df['Time'] = df['Time'].apply(lambda x: [el for el in x]) #numpy record to list
        df[['Time'] + [tag.name for tag in taglist]] = df['Time'].apply(pd.Series) #explode list to columns
        df['Time'] = df['Time'].apply(lambda x: add_timezone(x))
        df.reset_index(drop = True, inplace=True)

        return df

    def summary_extract(self, tag_list, summary_types, dataserver=None, calculation_basis=CalculationBasis.TIME_WEIGHTED, time_type=TimestampCalculation.AUTO, col=False):
        '''Return dataframe of summary measures for discrete events of EventHierarchy'''
        print('Building summary table from EventHierachy...')
        df =  self.df.copy()
        
        #performance checks
        maxi = max(df['Event'].apply(lambda x: x.duration))
        if maxi > pd.Timedelta("60 days"):
            print (f'Large Event(s) with duration up to {maxi} detected, Note that this might take some time...')
        if len(df) > 50:
            print(f'Summaries will be calculated for {len(df)} Events, Note that this might take some time...')
        
        if col == False:
            taglist = convert_to_TagList(tag_list, dataserver)
            #extract summary data for discrete events
            df['Time'] = df['Event'].apply(lambda x: list(x.summary(taglist, summary_types, dataserver, calculation_basis, time_type).to_records(index=False)))
        
        if col == True:
            if len(tag_list) > 1:
                raise AttributeError (f'You can only specify a single tag column at a time')
            if tag_list[0] in df.columns:
                event = df.columns.get_loc('Event')
                tags = df.columns.get_loc(tag_list[0])
                #extract summary data for discrete events
                df['Time'] = df.apply(lambda row: list(row[event].summary([row[tags]], summary_types, dataserver, calculation_basis, time_type).to_records(index=False)), axis=1)
            else:
                raise AttributeError (f'The column option was set to True, but {tag_list} is not a valid column')

        df = df.explode('Time') #explode list to rows
        df['Time'] = df['Time'].apply(lambda x: [el for el in x]) #numpy record to list
        df[['Tag', 'Summary', 'Value', 'Time']] = df['Time'].apply(pd.Series) #explode list to columns
        df['Time'] = df['Time'].apply(lambda x: add_timezone(x))
        df.reset_index(drop = True, inplace=True)
        
        return df
        

try:
    #delete the accessor to avoid warning 
    del pd.DataFrame.ecd
except AttributeError:
    pass
        
@pd.api.extensions.register_dataframe_accessor("ecd")    
class CondensedHierarchy:
    '''Additional functionality for pd.DataFrame object, for working with CondensedHierarchies'''
    
    def __init__(self, df):
        self.validate(df)
        self.df = df
        
    @staticmethod
    def validate(df):
        '''Validate input meets requirements for CondensedHierarchy'''
        if not {'Event', 'Name', 'Template', 'Level', 'Starttime', 'Endtime'}.issubset({x.split(' ')[0] for x in df.columns}):
            raise AttributeError("This dataframe does not have the correct EventHierarchy format") 
        for event in df.columns[df.columns.str.contains('Event\s\[.*]', regex=True)]:
            if not list(df[event].apply(lambda x: type(x)).unique()) == [Event]:
                raise AttributeError("This dataframe does not have the correct EventHierarchy format") 

    #Methods
    
    
    def interpol_discrete_extract(self, tag_list, interval, dataserver=None, col=False):
        '''Return dataframe of interpolated data for discrete events on bottom level of condensed hierarchy'''
        print('building discrete extract table from condensed hierachy...')
        #select events on bottem level of condensed hierarchy
        col_event = [col_name for col_name in self.df.columns if col_name.startswith('Event')][-1]
        
        #based on list of tags
        if col == False:
            df = self.df[[col_event]].copy()
            df.columns = ['Event']
            
            #performance checks
            maxi = max(df['Event'].apply(lambda x: x.duration))
            if maxi > pd.Timedelta("60 days"):
                print (f'Large Event(s) with duration up to {maxi} detected, Note that this might take some time...')
            if len(df) > 50:
                print(f'Summaries will be calculated for {len(df)} Events, Note that this might take some time...')
            
             #add procedure names
            df['Procedure'] = df['Event'].apply(lambda x: x.procedure)
            df = df[['Procedure', 'Event']]
            df.reset_index(drop = True, inplace=True)
        
            taglist = convert_to_TagList(tag_list, dataserver)
            #extract interpolated data for discrete events
            df['Time'] = df['Event'].apply(lambda x: list(x.interpolated_values(taglist, interval).to_records(index=True)))
        
        #based on column with tags
        if col == True:
            if len(tag_list) > 1:
                raise AttributeError (f'You can only specify a single tag column at a time')
            if tag_list[0] in self.df.columns:
                df = self.df[[col_event, tag_list[0]]].copy()
                df.columns = ['Event', 'Tags']
            else:
                raise AttributeError (f'The column option was set to True, but {tag_list} is not a valid column')
            
            #performance checks
            maxi = max(df['Event'].apply(lambda x: x.duration))
            if maxi > pd.Timedelta("60 days"):
                print (f'Large Event(s) with duration up to {maxi} detected, Note that this might take some time...')
            if len(df) > 50:
                print(f'Summaries will be calculated for {len(df)} Events, Note that this might take some time...')
                
            #add procedure names
            df['Procedure'] = df['Event'].apply(lambda x: x.procedure)
            df = df[['Procedure', 'Event', 'Tags']]
            df.reset_index(drop = True, inplace=True)
        
            event = df.columns.get_loc('Event')
            tags = df.columns.get_loc('Tags')
            #extract interpolated data for discrete events
            df['Time'] = df.apply(lambda row: list(row[event].interpolated_values([row[tags]], interval, dataserver).to_records(index=True)), axis=1)
            
            taglist = convert_to_TagList(list(df['Tags'].unique()), dataserver)

        df = df.explode('Time') #explode list to rows
        df['Time'] = df['Time'].apply(lambda x: [el for el in x]) #numpy record to list
        df[['Time'] + [tag.name for tag in taglist]] = df['Time'].apply(pd.Series) #explode list to columns
        df['Time'] = df['Time'].apply(lambda x: add_timezone(x))
        df.reset_index(drop = True, inplace=True)

        return df
    

    def interpol_continuous_extract(self, tag_list, interval, dataserver=None):
        '''Return dataframe of continous, interpolated data from the start of the first filtered event to the end of the last filtered event for each procedure on bottom level of condensed hierarchy'''
        taglist = convert_to_TagList(tag_list, dataserver)

        print('building continuous extract table from condensed hierachy...')
        #select events on bottem level of condensed hierarchy
        col_event = [col_name for col_name in self.df.columns if col_name.startswith('Event')][-1]

        df_base = self.df[[col_event]].copy()
        df_base.columns = ['Event']
        #add procedure names
        df_base['Procedure'] = df_base['Event'].apply(lambda x: x.procedure)
        df_base = df_base[['Procedure', 'Event']]
        df_base.reset_index(drop = True, inplace=True)

        #extract interpolated data for continuous events, per procedure
        df_cont = pd.DataFrame()
        for proc, df_proc in df_base.groupby('Procedure'):
            starttime = df_proc['Event'].iloc[0].starttime
            endtime = df_proc['Event'].iloc[-1].endtime
            values = list(taglist.interpolated_values(starttime, endtime, interval).to_records(index=True))
            df_cont = df_cont.append(pd.DataFrame([[proc, values]], columns=['Procedure', 'Time']), ignore_index=True)

        df_cont = df_cont.explode('Time') #explode list to rows
        df_cont['Time'] = df_cont['Time'].apply(lambda x: [el for el in x]) #numpy record to list
        #pd.DataFrame(df['b'].tolist(), index=df.index) instead of apply(pd.Series) could be faster
        df_cont[['Time'] + [tag.name for tag in taglist]] = df_cont['Time'].apply(pd.Series) #explode list to columns
        df_cont['Time'] = df_cont['Time'].apply(lambda x: add_timezone(x))

        #add Event info back
        df_cont['Event'] = np.nan
        for event in df_base['Event'] :
            df_cont['Event'].loc[(df_cont['Time'] >= event.starttime) & 
                                 (df_cont['Time'] <= event.endtime)] = event

        #format
        df_cont = df_cont[['Procedure', 'Event', 'Time'] + [tag.name for tag in taglist]]
        df_cont.sort_values(by=['Time'], ascending=True, inplace=True)
        df_cont.reset_index(drop = True, inplace=True)
        
        return df_cont

 
    def recorded_extract(self, tag_list, dataserver=None):
        '''Return nested dictionary (level 1: Procedures, Level 2: Tags) of recorded data extracts from the start of the first filtered event to the end of the last filtered event for each procedure on bottom level of condensed hierarchy'''
        taglist = convert_to_TagList(tag_list, dataserver)

        print('building recorded extract dict from condensed hierachy...')
        #select events on bottem level of condensed hierarchy
        col_event = [col_name for col_name in self.df.columns if col_name.startswith('Event')][-1]

        df_base = self.df[[col_event]].copy()
        df_base.columns = ['Event']
        #add procedure names
        df_base['Procedure'] = df_base['Event'].apply(lambda x: x.procedure)
        df_base = df_base[['Procedure', 'Event']]
        df_base.reset_index(drop = True, inplace=True)

        #extract recorded data for continuous events, per procedure
        dct={}
        for proc, df_proc in df_base.groupby('Procedure'):
            starttime = df_proc['Event'].iloc[0].starttime
            endtime = df_proc['Event'].iloc[-1].endtime
            values = taglist.recorded_values(starttime, endtime)
            for tag, df_rec in values.items():
                #add Event info back
                df_rec['Event'] = np.nan
                df_rec['Time'] = df_rec.index
                for event in df_base['Event'] :
                    df_rec['Event'].loc[(df_rec['Time'] >= event.starttime) & 
                                        (df_rec['Time'] <= event.endtime)] = event
                    df_rec.reset_index(drop = True, inplace=True)
                values[tag] = df_rec[['Event', 'Time', 'Data',]]
            dct[proc] = values
        return dct
    
    def plot_continuous_extract(self, tag_list, nr_of_intervals, dataserver=None):
        '''Return nested dictionary (level 1: Procedures, Level 2: Tags) of continuous plot data extracts from the start of the first filtered event to the end of the last filtered event for each procedure on bottom level of condensed hierarchy'''
        taglist = convert_to_TagList(tag_list, dataserver)

        print('building continuous plot extract dict from condensed hierachy...')
        #select events on bottem level of condensed hierarchy
        col_event = [col_name for col_name in self.df.columns if col_name.startswith('Event')][-1]

        df_base = self.df[[col_event]].copy()
        df_base.columns = ['Event']
        #add procedure names
        df_base['Procedure'] = df_base['Event'].apply(lambda x: x.procedure)
        df_base = df_base[['Procedure', 'Event']]
        df_base.reset_index(drop = True, inplace=True)

        #extract plot data for continuous events, per procedure
        dct={}
        for proc, df_proc in df_base.groupby('Procedure'):
            starttime = df_proc['Event'].iloc[0].starttime
            endtime = df_proc['Event'].iloc[-1].endtime
            values = taglist.plot_values(starttime, endtime, nr_of_intervals)
            for tag, df_rec in values.items():
                #add Event info back
                df_rec['Event'] = np.nan
                df_rec['Time'] = df_rec.index
                for event in df_base['Event'] :
                    df_rec['Event'].loc[(df_rec['Time'] >= event.starttime) & 
                                        (df_rec['Time'] <= event.endtime)] = event
                    df_rec.reset_index(drop = True, inplace=True)
                values[tag] = df_rec[['Event', 'Time', 'Data',]]
            dct[proc] = values
        return dct
    
    
    #option to specify column with tagnames to refer to instead of taglist? -----------------
    
    def summary_extract(self, tag_list, summary_types, dataserver=None, calculation_basis=CalculationBasis.TIME_WEIGHTED, time_type=TimestampCalculation.AUTO, col=False):
        '''Return dataframe of summary data for events on bottom level of condensed hierarchy'''
 
        print('building discrete extract table from condensed hierachy...')
        #select events on bottem level of condensed hierarchy
        col_event = [col_name for col_name in self.df.columns if col_name.startswith('Event')][-1]
        
        #based on list of tags
        if col == False:
            df = self.df[[col_event]].copy()
            df.columns = ['Event']
            
            #performance checks
            maxi = max(df['Event'].apply(lambda x: x.duration))
            if maxi > pd.Timedelta("60 days"):
                print(f'Large Event(s) with duration up to {maxi} detected, Note that this might take some time...')
            if len(df) > 50:
                print(f'Summaries will be calculated for {len(df)} Events, Note that this might take some time...')
            
             #add procedure names
            df['Procedure'] = df['Event'].apply(lambda x: x.procedure)
            df = df[['Procedure', 'Event']]
            df.reset_index(drop = True, inplace=True)
        
            taglist = convert_to_TagList(tag_list, dataserver)
            #extract summary data for discrete events
            df['Time'] = df['Event'].apply(lambda x: list(x.summary(taglist, summary_types, calculation_basis, time_type).to_records(index=False)))
            
        #based on column with tags
        if col == True:
            if len(tag_list) > 1:
                raise AttributeError (f'You can only specify a single tag column at a time')
            if tag_list[0] in self.df.columns:
                df = self.df[[col_event, tag_list[0]]].copy()
                df.columns = ['Event', 'Tags']
            else:
                raise AttributeError (f'The column option was set to True, but {tag_list} is not a valid column')
            
            #performance checks
            maxi = max(df['Event'].apply(lambda x: x.duration))
            if maxi > pd.Timedelta("60 days"):
                print (f'Large Event(s) with duration up to {maxi} detected, Note that this might take some time...')
            if len(df) > 50:
                print(f'Summaries will be calculated for {len(df)} Events, Note that this might take some time...')
                
            #add procedure names
            df['Procedure'] = df['Event'].apply(lambda x: x.procedure)
            df = df[['Procedure', 'Event', 'Tags']]
            df.reset_index(drop = True, inplace=True)

            event = df.columns.get_loc('Event')
            tags = df.columns.get_loc('Tags')
            #extract summary data for discrete events
            df['Time'] = df.apply(lambda row: list(row[event].summary([row[tags]], summary_types, dataserver, calculation_basis, time_type).to_records(index=False)), axis=1)

        df = df.explode('Time') #explode list to rows
        df['Time'] = df['Time'].apply(lambda x: [el for el in x]) #numpy record to list
        df[['Tag', 'Summary', 'Value', 'Time']] = df['Time'].apply(pd.Series) #explode list to columns
        df['Time'] = df['Time'].apply(lambda x: add_timezone(x))
        df.reset_index(drop = True, inplace=True)
        
        
        
        return df
        
        
        
        
        
        
        
        
        
        
        
        
        
        
class Asset:
    '''Container for Event object'''
        
    def __init__(self, asset):
        self.asset = asset
    
    def __repr__(self):
        return ('Asset:' + self.asset.GetPath() )
    
    def __str__(self):
        return ('Asset:' + self.asset.GetPath() )
    
    #Properties
    @property
    def name(self):
        '''Return name of Asset'''
        return self.asset.Name
    @property
    def path(self):
        '''Return path'''
        return self.asset.GetPath() 
    @property
    def pisystem_name(self):
        '''Return PISystem name'''
        return self.asset.PISystem.Name
    @property
    def database_name(self):
        '''Return database name'''
        return self.asset.Database.Name
    @property
    def database(self):
        '''Return PIAFDatabase object'''
        return PIAFDatabase(server=self.pisystem_name, database=self.database_name)
    @property
    def af_asset(self):
        '''Return AFEventFrame object'''
        return self.asset
    @property
    def af_template(self):
        '''Return AFTemplate'''
        return self.asset.Template
    @property
    def template_name(self):
        '''Return template name'''
        if self.asset.Template:
            return self.asset.Template.Name  
        else:
            return None
    @property
    def attributes(self):
        ''''Return list of attribute names for Asset'''
        return [attribute.Name for attribute in self.asset.Attributes]
    @property
    def af_attributes(self):
        ''''Return list of AFAttributes for Asset'''
        return [attribute for attribute in self.asset.Attributes]  
    def children(self):
        '''Return List of children for Asset'''
        return list([Asset(asset) for asset in self.asset.children])
    @property
    def parent(self):
        '''Return parent Asset for Asset'''
        return Asset(self.asset.Parent)
    @property
    def description(self):
        '''Return description for Asset'''
        return self.asset.Description
    
    #methods
    def get_attribute_values(self, attribute_names_list=[]):
        '''Return dict of attribute values for specified attributes'''
        attribute_dct={}
        if not attribute_names_list:
            for attribute in self.eventframe.Attributes:
                attribute_dct[attribute.Name] = attribute.GetValue().Value
            return attribute_dct
        else:
            for attribute in self.eventframe.Attributes:
                if attribute.Name in attribute_names_list:
                    attribute_dct[attribute.Name] = attribute.GetValue().Value
            return attribute_dct
    
    def get_events(self,
                   query=None,
                   start_time=None,
                   end_time='*',
                   template_name = None,
                   start_index=0,
                   max_count=1000000,
                   search_mode=SearchMode.OVERLAPPED,
                   search_full_hierarchy=True,
                   sortField=SortField.STARTTIME,
                   sortOrder=SortOrder.ASCENDING):
        '''Return EventList of Events on Asset within specified time period'''
        asset=self.name
        return self.database.find_events(query, asset, start_time, end_time, template_name, start_index, max_count, search_mode, search_full_hierarchy, sortField, sortOrder)

try:
    #delete the accessor to avoid warning 
    del pd.DataFrame.ahy
except AttributeError:
    pass

@pd.api.extensions.register_dataframe_accessor("ahy")
class AssetHierarchy:
    '''Additional functionality for pd.DataFrame object, for working with EventHierarchies'''
    
    def __init__(self, df):
        self.validate(df)
        self.df = df
    
    @staticmethod
    def validate(df):
        '''Validate object meets requirements for EventHierarchy'''
        #verify that dataframe fits EventHierarchy requirements
        if not {'Asset', 'Path', 'Name', 'Template', 'Level'}.issubset(set(df.columns)):
            raise AttributeError("This dataframe does not have the correct AssetHierarchy format")
    
    #methods
    def add_attributes(self, attribute_names_list, level):
        ''''Add attributtes to AssetHierarchy for specified attributes and level'''
        print('Fetching attribute(s)...')

        for attribute in attribute_names_list:
            self.df[attribute+' ['+str(level)+']'] = self.df.loc[self.df['Level']==level, 'Event'].apply(lambda x: x.get_attribute_values([attribute])[attribute]) 
        
        for colname in self.df.columns:
            try:
                self.df[colname] = self.df[colname].astype(float)
            except:
                pass
        return self.df

    def condense(self):
        ''''Return condensed dataframe based on Assets in AssetHierarchy'''
        print('Condensing...')
        
        df = self.df.copy()
        #merge level by level
        for level in range(int(df['Level'].min()), int(df['Level'].max()+1)):
            #subdf per level
            df_level = df[df['Level'] == level]
            #remove empty columns
            df_level.dropna(how='all', axis=1, inplace=True)
            if df_level.empty:
                df_condensed[level] = 'TempValue'
            else:
                #add auxiliary columns for merge based on path
                cols = [x for x in range(level+1)]
                df_level[cols] = df_level['Path'].str.split('\\', expand=True).loc[:, 4:]
                #remove Path columns
                df_level.drop(['Path'], 1, inplace=True)
                #rename columns, ignore columns with number names
                df_level.columns = [col_name + ' [' + str(int(level)) + ']' if not ((type(col_name) == int) or ('[' in col_name)) else col_name for col_name in df_level.columns]
                #merge with previous level
                if level == int(df['Level'].min()):
                    df_condensed = df_level
                else:
                    df_condensed = pd.merge(df_condensed, df_level, how='outer', left_on=cols[:-1], right_on=cols[:-1])
        #drop auxiliary columns 
        df_condensed.drop([col_name for col_name in df_condensed.columns if type(col_name) == int], 1, inplace=True)
        #remove duplicates
        df_condensed = df_condensed.drop_duplicates(keep='first')
        
        return df_condensed