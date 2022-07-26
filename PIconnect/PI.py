""" PI
    Core containers for connections to PI databases
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

try:
    from __builtin__ import str as BuiltinStr
except ImportError:
    BuiltinStr = str
# pragma pylint: enable=unused-import, redefined-builtin
from warnings import warn
from JanssenPI._utils import classproperty
from JanssenPI.AFSDK import AF
from JanssenPI.PIConsts import AuthenticationMode, BoundaryType, SummaryType, CalculationBasis, TimestampCalculation, PIPointType, ExpressionSampleType
from JanssenPI.time import timestamp_to_index, to_af_time_range, add_timezone
from JanssenPI.EFMethods import view 

from collections import UserList

import pandas as pd
pd.options.mode.chained_assignment = None  # default='warn'
import numpy as np
_NOTHING = object()


class PIServer(object):  # pylint: disable=useless-object-inheritance
    """PIServer is a connection to an OSIsoft PI Server

    Args:
        server (str, optional): Name of the server to connect to, defaults to None
        username (str, optional): can be used only with password as well
        password (str, optional): -//-
        todo: domain, auth
        timeout (int, optional): the maximum seconds an operation can take

    .. note::
        If the specified `server` is unknown a warning is thrown and the connection
        is redirected to the default server, as if no server was passed. The list
        of known servers is available in the `PIServer.servers` dictionary.
    """

    version = "0.2.2"

    #: Dictionary of known servers, as reported by the SDK
    _servers = _NOTHING
    #: Default server, as reported by the SDK
    _default_server = _NOTHING

    def __init__(
        self,
        server=None,
        username=None,
        password=None,
        domain=None,
        authentication_mode=AuthenticationMode.PI_USER_AUTHENTICATION,
        timeout=None,
    ):
        if server and server not in self.servers:
            message = 'Server "{server}" not found, using the default server.'
            warn(message=message.format(server=server), category=UserWarning)
        if bool(username) != bool(password):
            raise ValueError(
                "When passing credentials both the username and password must be specified."
            )
        if domain and not username:
            raise ValueError(
                "A domain can only specified together with a username and password."
            )
        if username:
            from System.Net import NetworkCredential
            from System.Security import SecureString

            secure_pass = SecureString()
            for c in password:
                secure_pass.AppendChar(c)
            cred = [username, secure_pass] + ([domain] if domain else [])
            self._credentials = (NetworkCredential(*cred), int(authentication_mode))
        else:
            self._credentials = None

        self.connection = self.servers.get(server, self.default_server)

        if timeout:
            from System import TimeSpan

            # System.TimeSpan(hours, minutes, seconds)
            self.connection.ConnectionInfo.OperationTimeOut = TimeSpan(0, 0, timeout)

    @classproperty
    def servers(self):
        if self._servers is _NOTHING:
            i, failures = 0, 0
            self._servers = {}
            from System import Exception as dotNetException  # type: ignore

            for i, server in enumerate(AF.PI.PIServers(), start=1):
                try:
                    self._servers[server.Name] = server
                except Exception:
                    failures += 1
                except dotNetException:
                    failures += 1
            if failures:
                warn(
                    "Could not load {} PI Server(s) out of {}".format(failures, i),
                    ResourceWarning,
                )
        return self._servers

    @classproperty
    def default_server(self):
        if self._default_server is _NOTHING:
            self._default_server = None
            try:
                self._default_server = AF.PI.PIServers().DefaultPIServer
            except Exception:
                warn("Could not load the default PI Server", ResourceWarning)
        return self._default_server

    def __enter__(self):
        if self._credentials:
            self.connection.Connect(*self._credentials)
        else:
            # Don't force to retry connecting if previous attempt failed
            force_connection = False
            self.connection.Connect(force_connection)
        return self

    def __exit__(self, *args):
        self.connection.Disconnect()

    def __repr__(self):
        return "%s(\\\\%s)" % (self.__class__.__name__, self.server_name)

    @property
    def server_name(self):
        """server_name

        Name of the connected server
        """
        return self.connection.Name

    def search(self, query, source=None):
        """search

        Search PIPoints on the PIServer

        Args:
            query (str or [str]): String or list of strings with queries
            source (str, optional): Defaults to None. Point source to limit the results

        Returns:
            list: A list of :class:`PIPoint` objects as a result of the query

        .. todo::

            Reject searches while not connected
        """
        if isinstance(query, list):
            return TagList([y for x in query for y in self.search(x, source)])
        elif not isinstance(query, str):
             raise TypeError('Argument query must be either a string or a list of strings,' +
                             'got type ' + str(type(query)))
        return TagList([Tag(pi_point)for pi_point in AF.PI.PIPoint.FindPIPoints(self.connection, BuiltinStr(query), source, None)])

    
class Tag:
    '''Container for Tag object'''
    def __init__(self, tag):
        self.validate(tag)
        self.tag = tag
        self.__attributes_loaded = False

    def __repr__(self):
        return 'Tag:' + self.name
    
    def __str__(self):
        return 'Tag:' + self.name
  
    @staticmethod
    def validate(tag):
        if not (type(tag) == AF.PI.PIPoint):
            raise AttributeError('Can not convert this type of input to Tag object')
    
    def __load_attributes(self):
        """Load the raw attributes of the PI Point from the server"""
        if not self.__attributes_loaded:
            self.tag.LoadAttributes([])
            self.__attributes_loaded = True
    
    #Properties
    @property
    def name(self):
        return self.tag.Name
    
    @property
    def pipoint(self):
        return self.tag
    
    @property
    def server(self):
        return PIServer(self.tag.Server.Name)
    
    @property
    def raw_attributes(self):
        """Return a dictionary of the raw attributes of the PI Point."""
        self.__load_attributes()
        return {att.Key: att.Value for att in self.tag.GetAttributes([])}
    
    @property
    def last_update(self):
        """Return the time at which the last value for this PI Point was recorded."""
        return timestamp_to_index(self.tag.CurrentValue().Timestamp.UtcTime)

    @property
    def units_of_measurement(self):
        """Return the units of measument in which values for this PI Point are reported."""
        return self.raw_attributes["engunits"]

    @property
    def description(self):
        """Return the description of the PI Point."""
        return self.raw_attributes["descriptor"]

    @property
    def created(self):
        """Return the creation datetime of a point."""
        return timestamp_to_index(self.raw_attributes["creationdate"])

    #Methods
    def current_value(self):
        """Return the last recorded value for this PI Point (internal use only)."""
        return self.tag.CurrentValue().Value
    
    def interpolated_values(self, starttime, endtime, interval, filter_expression=''):
        ''''Return Dataframe of interpolated values for Tag, between starttime and endtime'''
        AFInterval = AF.Time.AFTimeSpan.Parse(interval)
        AFTimeRange = to_af_time_range(starttime, endtime)
        filter_expression = filter_expression.replace("%tag%", self.name)

        #Could have issues with quering multiple PI Data Archives simultanously, see documentation
        #https://docs.osisoft.com/bundle/af-sdk/page/html/M_OSIsoft_AF_PI_PIPointList_InterpolatedValues.htm
        result = self.tag.InterpolatedValues(AFTimeRange, AFInterval, filter_expression, False)

        if result:
            #process query results
            data = [list(result)]
            df = pd.DataFrame(data).T
            df.columns = [self.name]
            #https://docs.osisoft.com/bundle/af-sdk/page/html/T_OSIsoft_AF_Asset_AFValue.htm
            df.index = df[df.columns[0]].apply(lambda x: timestamp_to_index(x.Timestamp.UtcTime))
            df.index.name = 'Index'
            df = df.applymap(lambda x: x.Value)
            return df
        else: #if no result, return empty dataframe
            return pd.DataFrame()


    def recorded_values(self, starttime, endtime, filter_expression='', AFBoundaryType=BoundaryType.INTERPOLATED):
        ''''Return Dataframe of recorded values for Tag, between starttime and endtime'''
        AFTimeRange = to_af_time_range(starttime, endtime)
        filter_expression = filter_expression.replace("%tag%", self.name)

        #Could have issues with quering multiple PI Data Archives simultanously, see documentation
        #maximum number of events that can be returned with a single call. As of PI 3.4.380, the default is set at 1.5M
        #https://docs.osisoft.com/bundle/af-sdk/page/html/M_OSIsoft_AF_PI_PIPointList_RecordedValues.htm
        result = self.tag.RecordedValues(AFTimeRange, AFBoundaryType, '', False)
        
        if result:
            #process query results
            data = [list(result)]
            df = pd.DataFrame(data).T
            df.columns = [self.name]
            #https://docs.osisoft.com/bundle/af-sdk/page/html/T_OSIsoft_AF_Asset_AFValue.htm
            df.index = df[df.columns[0]].apply(lambda x: timestamp_to_index(x.Timestamp.UtcTime))
            df.index.name = 'Index'
            df = df.applymap(lambda x: x.Value)
            return df
        else: #if no result, return empty dataframe
            return pd.DataFrame()
    
    def plot_values(self, starttime, endtime, nr_of_intervals):
        '''Retrieves values over the specified time range suitable for plotting over the number of intervals (typically represents pixels)
        Returns a Dataframe with values that will produce the most accurate plot over the time range while minimizing the amount of data returned.'''
        AFTimeRange = to_af_time_range(starttime, endtime)

        result = self.tag.PlotValues(AFTimeRange, nr_of_intervals)

        if result:
            #process query results
            data = [list(result)]
            df = pd.DataFrame(data).T
            df.columns = [self.name]
            #https://docs.osisoft.com/bundle/af-sdk/page/html/T_OSIsoft_AF_Asset_AFValue.htm
            df.index = df[df.columns[0]].apply(lambda x: timestamp_to_index(x.Timestamp.UtcTime))
            df.index.name = 'Index'
            df = df.applymap(lambda x: x.Value)
            return df
        else:
            return pd.DataFrame

    
    #CalculationBasis.EVENT_WEIGHTED avoids issues(?) with interpolation: ref. #Issue 1
    def summary(self, starttime, endtime, summary_types, calculation_basis=CalculationBasis.TIME_WEIGHTED, time_type=TimestampCalculation.AUTO):
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
        
        AFTimeRange = to_af_time_range(starttime, endtime)

        result = self.tag.Summary(AFTimeRange, summary_types, calculation_basis, time_type)    

        df_final = pd.DataFrame()
        for x in result: #per summary
            summary = SummaryType(x.Key).name
            value = x.Value
            timestamp = timestamp_to_index(x.Value.Timestamp.UtcTime)
            df = pd.DataFrame([[summary, value, timestamp]], columns = ['Summary', 'Value', 'Timestamp'])
            df_final = df_final.append(df, ignore_index=True)
            
        return df_final


    def summaries(self, starttime, endtime, interval, summary_types, calculation_basis=CalculationBasis.TIME_WEIGHTED, time_type=TimestampCalculation.AUTO):
        '''Return one or more summary values for each interval within a time range'''
        AFTimeRange = to_af_time_range(starttime, endtime)
        AFInterval = AF.Time.AFTimeSpan.Parse(interval)
        
        result = self.tag.Summaries(AFTimeRange, AFInterval, summary_types, calculation_basis, time_type)     

        df_final = pd.DataFrame()
        for x in result: #per summary
            summary = SummaryType(x.Key).name
            values = [(timestamp_to_index(value.Timestamp.UtcTime), value.Value) for value in x.Value]
            df = pd.DataFrame(values, columns = ['Timestamp', 'Value'])
            df['Summary'] = summary
            df_final = df_final.append(df, ignore_index=True)
        
        return df_final[['Summary', 'Timestamp', 'Value']]
 
    def filtered_summaries(self, starttime, endtime, interval,summary_types,
                           filter_expression,
                           calculation_basis=CalculationBasis.TIME_WEIGHTED, 
                           time_type=TimestampCalculation.AUTO, 
                           AFfilter_evaluation=ExpressionSampleType.EXPRESSION_RECORDED_VALUES, 
                           filter_interval=None):       
        
        '''Return one or more summary values for each interval within a filtered time range'''
        AFTimeRange = to_af_time_range(starttime, endtime)
        AFInterval = AF.Time.AFTimeSpan.Parse(interval)
        filter_expression = filter_expression.replace("%tag%", self.name)
        AFfilter_interval=AF.Time.AFTimeSpan.Parse(filter_interval)

        result = self.tag.FilteredSummaries(AFTimeRange, AFInterval, filter_expression, summary_types, calculation_basis, AFfilter_evaluation, AFfilter_interval, time_type)     

        df_final = pd.DataFrame()
        for x in result: #per summary
            summary = SummaryType(x.Key).name
            values = [(timestamp_to_index(value.Timestamp.UtcTime), value.Value) for value in x.Value]
            df = pd.DataFrame(values, columns = ['Timestamp', 'Value'])
            df['Summary'] = summary
            df_final = df_final.append(df, ignore_index=True)
        
        return df_final[['Summary', 'Value', 'Timestamp',]]

            

class TagList(UserList):
    '''Container for TagList object'''

    def __init__(self, data):
        self.validate(data)
        self.data = data

    def __repr__(self):
        return str([tag for tag in self.data])
    
    def __str__(self):
        return str([tag for tag in self.data])
    
    @staticmethod
    def validate(data):
        '''Validate input meets requirements for TagList'''
        try:
            for tag in data:
                if type(tag) == Tag:
                    pass
                else:
                    raise AttributeError('Can not convert tag of type {} to TagList object'.format(type(tag)))
        except:
            raise AttributeError('Can not convert this type of input to TagList object')
    
    def current_value(self):
        '''Return Dataframe of current values per tag'''
        PIPointlist = generate_pipointlist(self)
        result = PIPointlist.CurrentValue()
        if result:
            values = [x.Value for x in result]
            tags = [x.PIPoint.Name for x in result]
            return pd.DataFrame([values], columns=tags) 
        else:
            return pd.DataFrame 
    
    def plot_values(self, starttime, endtime, nr_of_intervals):
        '''Retrieves values over the specified time range suitable for plotting over the number of intervals (typically represents pixels)
        Returns a Dictionary of DataFrames for Tags in Taglist with values that will produce the most accurate plot over the time range while minimizing the amount of data returned.'''
        AFTimeRange = to_af_time_range(starttime, endtime)
        PIPointlist = generate_pipointlist(self)

        result = PIPointlist.PlotValues(AFTimeRange, nr_of_intervals, AF.PI.PIPagingConfiguration(AF.PI.PIPageType.TagCount, 1000))

        if result:
            #process query results
            data = list(result.ResultQueue.GetConsumingEnumerable())
            data = [list(series)for series in data]
            
            dct = {}
            tags = [tag.Name for tag in result.PointList]
            for i, lst in enumerate(data):
                df = pd.DataFrame([lst]).T
                df.columns = ['Data']
                #https://docs.osisoft.com/bundle/af-sdk/page/html/T_OSIsoft_AF_Asset_AFValue.htm
                df.index = df['Data'].apply(lambda x: timestamp_to_index(x.Timestamp.UtcTime))
                df.index.name = 'Index'
                df = df.applymap(lambda x: x.Value)
                dct[tags[i]] = df
            return dct
        else:
            return dict()
 
    def interpolated_values(self, starttime, endtime, interval, filter_expression=''):
        ''''Return Dataframe of interpolated values for Tags in TagList, between starttime and endtime'''
        PIPointlist = generate_pipointlist(self)
        AFInterval = AF.Time.AFTimeSpan.Parse(interval)
        AFTimeRange = to_af_time_range(starttime, endtime)
        
        #Could have issues with quering multiple PI Data Archives simultanously, see documentation
        #https://docs.osisoft.com/bundle/af-sdk/page/html/M_OSIsoft_AF_PI_PIPointList_InterpolatedValues.htm
        result = PIPointlist.InterpolatedValues(AFTimeRange, AFInterval, '', False, AF.PI.PIPagingConfiguration(AF.PI.PIPageType.TagCount, 1000))
        
        if result:
            #process query results
            data = list(result.ResultQueue.GetConsumingEnumerable())
            data = [list(series)for series in data]
            df = pd.DataFrame(data).T
            df.columns = [tag.Name for tag in result.PointList]
            #https://docs.osisoft.com/bundle/af-sdk/page/html/T_OSIsoft_AF_Asset_AFValue.htm
            df.index = df[df.columns[0]].apply(lambda x: timestamp_to_index(x.Timestamp.UtcTime))
            df.index.name = 'Index'
            df = df.applymap(lambda x: x.Value)
            return df
        else: #if no result, return empty dataframe
            return pd.DataFrame()

    def recorded_values(self, starttime, endtime, filter_expression='', AFBoundaryType=BoundaryType.INTERPOLATED):
        ''''Return dictionary of Dataframes of recorded values for Tags in TagList, between starttime and endtime'''
        PIPointlist = generate_pipointlist(self)
        AFTimeRange = to_af_time_range(starttime, endtime)

        #Could have issues with quering multiple PI Data Archives simultanously, see documentation
        #maximum number of events that can be returned with a single call. As of PI 3.4.380, the default is set at 1.5M
        #https://docs.osisoft.com/bundle/af-sdk/page/html/M_OSIsoft_AF_PI_PIPointList_RecordedValues.htm
        result = PIPointlist.RecordedValues(AFTimeRange, AFBoundaryType, '', False, AF.PI.PIPagingConfiguration(AF.PI.PIPageType.TagCount, 1000))
        
        if result:
            #process query results
            data = list(result.ResultQueue.GetConsumingEnumerable())
            data = [list(series)for series in data]
            
            dct = {}
            tags = [tag.Name for tag in result.PointList]
            for i, lst in enumerate(data):
                df = pd.DataFrame([lst]).T
                df.columns = ['Data']
                #https://docs.osisoft.com/bundle/af-sdk/page/html/T_OSIsoft_AF_Asset_AFValue.htm
                df.index = df['Data'].apply(lambda x: timestamp_to_index(x.Timestamp.UtcTime))
                df.index.name = 'Index'
                df = df.applymap(lambda x: x.Value)
                dct[tags[i]] = df
            return dct
        else: #if no result, return empty dictionary
            return dict()
    
    def summary(self, starttime, endtime, summary_types, calculation_basis=CalculationBasis.TIME_WEIGHTED, time_type=TimestampCalculation.AUTO):
        '''Return specified summary measure(s) for Tags in Taglist
        
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
        
        PIPointlist = generate_pipointlist(self)
        AFTimeRange = to_af_time_range(starttime, endtime)

        result = PIPointlist.Summary(AFTimeRange, summary_types, calculation_basis, time_type, AF.PI.PIPagingConfiguration(AF.PI.PIPageType.TagCount, 1000))    
        #to avoid queue emptying
        data = list(result)
        if data:
            df_final = pd.DataFrame()
            for x in data: #per tag
                point = [y.PIPoint.Name for y in x.Values][0]
                summaries = [SummaryType(y).name for y in x.Keys]
                values = [[y.Value, timestamp_to_index(y.Timestamp.UtcTime)] for y in x.Values]
                df = pd.DataFrame(values, columns = ['Value', 'Timestamp'])
                df['Tag'] = point
                df['Summary'] = summaries
                df_final = df_final.append(df, ignore_index=True)
                
            return df_final[['Tag', 'Summary','Value', 'Timestamp']]
        else:
            return pd.DataFrame()


    def summaries(self, starttime, endtime, interval, summary_types, calculation_basis=CalculationBasis.TIME_WEIGHTED, time_type=TimestampCalculation.AUTO):
        '''Return one or more summary values for Tags in Taglist, for each interval within a time range'''
        PIPointlist = generate_pipointlist(self)
        AFTimeRange = to_af_time_range(starttime, endtime)
        AFInterval = AF.Time.AFTimeSpan.Parse(interval)

        result = PIPointlist.Summaries(AFTimeRange, AFInterval, summary_types, calculation_basis, time_type, AF.PI.PIPagingConfiguration(AF.PI.PIPageType.TagCount, 1000))     
        data = list(result)
        if data:
            df_final = pd.DataFrame()
            for x in data: #per tag
                point = [y.PIPoint.Name for y in x.Values][0]
                summaries = [y for y in x.Keys]
                df = pd.DataFrame([[point, summary] for summary in summaries], columns = ['Tag', 'Summary'])
                df['Timestamp'] = df['Summary'].apply(lambda key: [(timestamp_to_index(value.Timestamp.UtcTime), value.Value) for value in x[key]])
                df['Summary'] = df['Summary'].apply(lambda x: SummaryType(x).name)
                df = df.explode('Timestamp')
                df[['Timestamp', 'Value']] = df['Timestamp'].apply(pd.Series) #explode list to columns
                df_final = df_final.append(df, ignore_index=True)
            
            return df_final[['Tag', 'Summary','Value', 'Timestamp']]
        else:
            return pd.DataFrame()
    
    def filtered_summaries(self, starttime, endtime, interval,summary_types,
                           filter_expression,
                           calculation_basis=CalculationBasis.TIME_WEIGHTED, 
                           time_type=TimestampCalculation.AUTO, 
                           AFfilter_evaluation=ExpressionSampleType.EXPRESSION_RECORDED_VALUES, 
                           filter_interval=None):       
        
        '''Return one or more summary values for Tags in Taglist, (Optional: for each interval) within a filtered time range'''
        PIPointlist = generate_pipointlist(self)
        AFTimeRange = to_af_time_range(starttime, endtime)
        AFInterval = AF.Time.AFTimeSpan.Parse(interval)
        AFfilter_interval=AF.Time.AFTimeSpan.Parse(filter_interval)
        result = PIPointlist.FilteredSummaries(AFTimeRange, AFInterval, filter_expression, summary_types, calculation_basis, AFfilter_evaluation, AFfilter_interval, time_type, AF.PI.PIPagingConfiguration(AF.PI.PIPageType.TagCount, 1000))     

        data = list(result)
        if data:
            df_final = pd.DataFrame()
            for x in data: #per tag
                point = [y.PIPoint.Name for y in x.Values][0]
                summaries = [y for y in x.Keys]
                df = pd.DataFrame([[point, summary] for summary in summaries], columns = ['Tag', 'Summary'])
                df['Timestamp'] = df['Summary'].apply(lambda key: [(timestamp_to_index(value.Timestamp.UtcTime), value.Value) for value in x[key]])
                df['Summary'] = df['Summary'].apply(lambda x: SummaryType(x).name)
                df = df.explode('Timestamp')
                df[['Timestamp', 'Value']] = df['Timestamp'].apply(pd.Series) #explode list to columns
                df_final = df_final.append(df, ignore_index=True)
            
            return df_final[['Tag', 'Summary','Value', 'Timestamp']]
        else:
            return pd.DataFrame()












    
        
    def get_interpolated_values_AFTimerange(self, tag, AFTimeRange, interval):
        ''''Return interpolated values for tag specified by its tagname or PIPoint, for AFTimerange'''
        if type(tag) == AF.PI.PIPoint:
            pass
        elif type(tag) == str:
            try:
                tag = self.search(tag)[0]
            except:
                raise Exception('Tag "' + tag + '" was not found') 
        elif type(tag) == list:
            raise Exception("Tag can not be a list, in case of list of tags use <server.get_interpolated_values_multiple()>")
                
        #https://docs.osisoft.com/bundle/af-sdk/page/html/T_OSIsoft_AF_Time_AFTimeSpan.htm
        AFInterval = AF.Time.AFTimeSpan.Parse(interval)
        
        pivalues = tag.InterpolatedValues(AFTimeRange, AFInterval, '', False)
        timestamps = []
        values = []
        for value in pivalues:
            timestamps.append(timestamp_to_index(value.Timestamp.UtcTime))
            values.append(value.Value)
        return pd.Series(index = timestamps, data = values).to_frame() 


    def get_interpolated_values(self, tag, starttime, endtime, interval):
        ''''Return Series of interpolated values for tag specified by its tagname or PIPoint, between starttime and endtime'''
        if type(tag) == AF.PI.PIPoint:
            pass
        elif type(tag) == str:
            try:
                tag = self.search(tag)[0]
            except:
                raise Exception('Tag "' + tag + '" was not found')  
        elif type(tag) == list:
            raise Exception("Tag can not be a list, in case of list of tags use <server.get_interpolated_values_multiple()>")
                
        #https://docs.osisoft.com/bundle/af-sdk/page/html/T_OSIsoft_AF_Time_AFTimeSpan.htm
        AFInterval = AF.Time.AFTimeSpan.Parse(interval)
        AFTimeRange = to_af_time_range(starttime, endtime)
        pivalues = tag.InterpolatedValues(AFTimeRange, AFInterval, '', False)
        timestamps = []
        values = []
        for value in pivalues:
            timestamps.append(timestamp_to_index(value.Timestamp.UtcTime))
            values.append(value.Value)
        return pd.Series(index = timestamps, data = values).to_frame() 


    def get_summary(self, tag, starttime, endtime, summary_types, calculation_basis=CalculationBasis.TIME_WEIGHTED, time_type=TimestampCalculation.AUTO):
        if type(tag) == AF.PI.PIPoint:
            pass
        elif type(tag) == str:
            try:
                tag = self.search(tag)[0]
            except:
                raise Exception('Tag "' + tag + '" was not found')  
        elif type(tag) == list:
            raise Exception("Tag can not be a list, in case of list of tags use <server.get_interpolated_values_multiple()>")
        AFTimeRange = to_af_time_range(starttime, endtime)
        result = tag.Summary(AFTimeRange, summary_types, calculation_basis, time_type)    
        df = pd.DataFrame([[x.Value.Value, timestamp_to_index(x.Value.Timestamp.UtcTime)] for x in result],
                            index = [SummaryType(x.Key).name for x in result], columns = ['Value', 'Timestamp'])
        return df


    def get_summary_multiple(self, tag_list, starttime, endtime, summary_types, calculation_basis=CalculationBasis.TIME_WEIGHTED, time_type=TimestampCalculation.AUTO):
        if type(tag_list) == str:
            raise Exception ("Tag needs to be a inside a list")
        PIPointlist = AF.PI.PIPointList()
        for tag in tag_list:
            if type(tag) == AF.PI.PIPoint:
                pass
            elif type(tag) == str:
                try:
                    tag = self.search(tag)[0]
                except:
                    print('Tag "' + tag + '" was not found')
                    continue
            PIPointlist.Add(tag)
        AFTimeRange = to_af_time_range(starttime, endtime)

        result = PIPointlist.Summary(AFTimeRange, summary_types, calculation_basis, time_type, AF.PI.PIPagingConfiguration(AF.PI.PIPageType.TagCount, 1000))    
        #to avoid queue emptying
        data = list(result)
        
        df_final = pd.DataFrame()
        for x in data:
            points = [y.PIPoint.Name for y in x.Values][0]
            summaries = [SummaryType(y).name for y in x.Keys]
            values = [[y.Value, timestamp_to_index(y.Timestamp.UtcTime)] for y in x.Values]
            df = pd.DataFrame(values, columns = ['Value', 'Timestamp'])
            df['Tag'] = points
            df['Summary'] = summaries
            df_final = df_final.append(df, ignore_index=True)
            
        df_final = df_final[['Tag', 'Summary','Value', 'Timestamp']]
        return df_final


    def get_interpolated_values_multiple_AFTimerange(self, tag_list, AFTimeRange, interval):
        ''''Return Dataframe of interpolated values for tags specified by list of tagnames or PIPoint, for AFTimerange'''
        if type(tag_list) == str:
            raise Exception ("Tag needs to be a inside a list")
        PIPointlist = AF.PI.PIPointList()
        for tag in tag_list:
            if type(tag) == AF.PI.PIPoint:
                pass
            elif type(tag) == str:
                try:
                    tag = self.search(tag)[0]
                except:
                    print('Tag "' + tag + '" was not found')
                    continue
            PIPointlist.Add(tag)
        AFInterval = AF.Time.AFTimeSpan.Parse(interval)
        
        #Could have issues with quering multiple PI Data Archives simultanously, see documentation
        #https://docs.osisoft.com/bundle/af-sdk/page/html/M_OSIsoft_AF_PI_PIPointList_InterpolatedValues.htm
        result = PIPointlist.InterpolatedValues(AFTimeRange, AFInterval, '', False, AF.PI.PIPagingConfiguration(AF.PI.PIPageType.TagCount, 1000))

        #process query results
        data = list(result.ResultQueue.GetConsumingEnumerable())
        data = [list(reeks)for reeks in data]
        df = pd.DataFrame(data).T
        df.columns = [tag.Name for tag in result.PointList]
        #https://docs.osisoft.com/bundle/af-sdk/page/html/T_OSIsoft_AF_Asset_AFValue.htm
        df.index = df[df.columns[0]].apply(lambda x: timestamp_to_index(x.Timestamp.UtcTime))
        df.index.name = 'Index'
        df = df.applymap(lambda x: x.Value)
        return df
                

    def get_interpolated_values_multiple(self, tag_list, starttime, endtime, interval):
        ''''Return Dataframe of interpolated values for tags specified by list of tagnames or PIPoint, between starttime and endtime'''
        if type(tag_list) == str:
            raise Exception ("Tag needs to be a inside a list")
        PIPointlist = AF.PI.PIPointList()
        for tag in tag_list:
            if type(tag) == AF.PI.PIPoint:
                pass
            elif type(tag) == str:
                try:
                    tag = self.search(tag)[0]
                except:
                    print('Tag "' + tag + '" was not found')
                    continue
            PIPointlist.Add(tag)
        AFInterval = AF.Time.AFTimeSpan.Parse(interval)
        AFTimeRange = to_af_time_range(starttime, endtime)
        
        #Could have issues with quering multiple PI Data Archives simultanously, see documentation
        #https://docs.osisoft.com/bundle/af-sdk/page/html/M_OSIsoft_AF_PI_PIPointList_InterpolatedValues.htm
        result = PIPointlist.InterpolatedValues(AFTimeRange, AFInterval, '', False, AF.PI.PIPagingConfiguration(AF.PI.PIPageType.TagCount, 1000))

        #process query results
        data = list(result.ResultQueue.GetConsumingEnumerable())
        data = [list(reeks)for reeks in data]
        df = pd.DataFrame(data).T
        df.columns = [tag.Name for tag in result.PointList]
        #https://docs.osisoft.com/bundle/af-sdk/page/html/T_OSIsoft_AF_Asset_AFValue.htm
        df.index = df[df.columns[0]].apply(lambda x: timestamp_to_index(x.Timestamp.UtcTime))
        df.index.name = 'Index'
        df = df.applymap(lambda x: x.Value)
        return df


    def get_recorded_values_AFTimerange(self, tag, AFTimeRange):
        ''''Return interpolated values for tag specified by its tagname or PIPoint, for AFTimerange'''
        if type(tag) == AF.PI.PIPoint:
            pass
        elif type(tag) == str:
            try:
                tag = self.search(tag)[0]
            except:
                raise Exception('Tag "' + tag + '" was not found') 
        elif type(tag) == list:
            raise Exception("Tag can not be a list, in case of list of tags use <server.get_interpolated_values_multiple()>")
        AFBoundaryType = BoundaryType.INSIDE
        pivalues = tag.RecordedValues(AFTimeRange, AFBoundaryType, '', False)
        timestamps = []
        values = []
        for value in pivalues:
            timestamps.append(timestamp_to_index(value.Timestamp.UtcTime))
            values.append(value.Value)
        return pd.Series(index = timestamps, data = values).to_frame() 


    def get_recorded_values(self, tag, starttime, endtime):
        ''''Return Series of interpolated values for tag specified by its tagname or PIPoint, between starttime and endtime'''
        if type(tag) == AF.PI.PIPoint:
            pass
        elif type(tag) == str:
            try:
                tag = self.search(tag)[0]
            except:
                raise Exception('Tag "' + tag + '" was not found')  
        elif type(tag) == list:
            raise Exception("Tag can not be a list, in case of list of tags use <server.get_interpolated_values_multiple()>")
                
        #https://docs.osisoft.com/bundle/af-sdk/page/html/T_OSIsoft_AF_Time_AFTimeSpan.htm
        AFTimeRange = to_af_time_range(starttime, endtime)
        AFBoundaryType = BoundaryType.INSIDE
        pivalues = tag.RecordedValues(AFTimeRange, AFBoundaryType, '', False)
        timestamps = []
        values = []
        for value in pivalues:
            timestamps.append(timestamp_to_index(value.Timestamp.UtcTime))
            values.append(value.Value)
        return pd.Series(index = timestamps, data = values).to_frame() 


    def get_recorded_values_multiple_AFTimerange(self, tag_list, AFTimeRange):
        ''''Return Dataframe of interpolated values for tags specified by list of tagnames or PIPoint, for AFTimerange'''
        if type(tag) == str:
            raise Exception ("Tag needs to be a inside a list")
        PIPointlist = AF.PI.PIPointList()
        for tag in tag_list:
            if type(tag) == AF.PI.PIPoint:
                pass
            elif type(tag) == str:
                try:
                    tag = self.search(tag)[0]
                except:
                    print('Tag "' + tag + '" was not found')
                    continue
            PIPointlist.Add(tag)
        AFBoundaryType = BoundaryType.INSIDE
        #Could have issues with quering multiple PI Data Archives simultanously, see documentation
        #https://docs.osisoft.com/bundle/af-sdk/page/html/M_OSIsoft_AF_PI_PIPointList_InterpolatedValues.htm
        result = PIPointlist.RecordedValues(AFTimeRange, AFBoundaryType, '', False, AF.PI.PIPagingConfiguration(AF.PI.PIPageType.TagCount, 1000))

        #process query results
        data = list(result.ResultQueue.GetConsumingEnumerable())
        data = [list(reeks)for reeks in data]
        
        dct = {}
        tags = [tag.Name for tag in result.PointList]
        for i, lst in enumerate(data):
            df = pd.DataFrame([lst]).T
            df.columns = ['Data']
            #https://docs.osisoft.com/bundle/af-sdk/page/html/T_OSIsoft_AF_Asset_AFValue.htm
            df.index = df['Data'].apply(lambda x: timestamp_to_index(x.Timestamp.UtcTime))
            df.index.name = 'Index'
            df = df.applymap(lambda x: x.Value)
            dct[tags[i]] = df
        return dct
                

    def get_recorded_values_multiple(self, tag_list, starttime, endtime):
        ''''Return Dataframe of interpolated values for tags specified by list of tagnames or PIPoint, between starttime and endtime'''
        PIPointlist = AF.PI.PIPointList()
        for tag in tag_list:
            if type(tag) == AF.PI.PIPoint:
                pass
            elif type(tag) == str:
                try:
                    tag = self.search(tag)[0]
                except:
                    print('Tag "' + tag + '" was not found')
                    continue
            PIPointlist.Add(tag)
        AFTimeRange = to_af_time_range(starttime, endtime)
        AFBoundaryType = BoundaryType.INSIDE
        #Could have issues with quering multiple PI Data Archives simultanously, see documentation
        #https://docs.osisoft.com/bundle/af-sdk/page/html/M_OSIsoft_AF_PI_PIPointList_InterpolatedValues.htm
        result = PIPointlist.RecordedValues(AFTimeRange, AFBoundaryType, '', False, AF.PI.PIPagingConfiguration(AF.PI.PIPageType.TagCount, 1000))

        #process query results
        data = list(result.ResultQueue.GetConsumingEnumerable())
        data = [list(reeks)for reeks in data]

        dct = {}
        tags = [tag.Name for tag in result.PointList]
        for i, lst in enumerate(data):
            df = pd.DataFrame([lst]).T
            df.columns = ['Data']
            #https://docs.osisoft.com/bundle/af-sdk/page/html/T_OSIsoft_AF_Asset_AFValue.htm
            df.index = df['Data'].apply(lambda x: timestamp_to_index(x.Timestamp.UtcTime))
            df.index.name = 'Index'
            df = df.applymap(lambda x: x.Value)
            dct[tags[i]] = df
        return dct
       
    def to_MVA(self, event_dataframe, tag_list, interval, level=None):
        if 'Name' in event_dataframe.columns: #flat dataframe
            if level:
                #translate template to correspondinglevel if applicable
                if type(level) == str:
                    level = event_dataframe.loc[event_dataframe['Template']==level, 'Level'].iloc[0]
                print('building MVA table from flat eventframe dataframe...')
                
                if str(type(event_dataframe['AFTimerange'].iloc[0])) != "<class 'OSIsoft.AF.Time.AFTimeRange'>":
                    raise Exception ('AFTimerange is of type '+ str(type(event_dataframe['AFTimerange'].iloc[0])) +'(do not use views)')
                    
                event_dataframe = event_dataframe[event_dataframe['Level'] == level][['Path', 'Starttime', 'Endtime', 'AFTimerange']].copy()
                
                #Batch and phase based on path
                event_dataframe[['Batch','Phase']] = event_dataframe['Path'].str.split('\\', expand=True, n=5).loc[:, 4:]
                event_dataframe.replace({'Batch':{'EventFrames\[':'', '\]':''}}, regex=True, inplace=True) #partial replacements
        
                df_MVA = event_dataframe[['Batch','Phase','Starttime','Endtime', 'AFTimerange']]
                df_MVA.reset_index(drop = True, inplace=True)
                
                #get tag data as df, convert to list of tuples (.to_records) and explode
                print('Fetching Tag data...')
                df_MVA['Time'] = df_MVA['AFTimerange'].apply(lambda x: list(self.get_interpolated_values_multiple_AFTimerange(tag_list, x, interval).to_records(index=True)))
        
                df_MVA = df_MVA.explode('Time') #explode list to rows
                df_MVA['Time'] = df_MVA['Time'].apply(lambda x: [el for el in x]) #numpy record to list
                df_MVA[['Time'] + [str(tag) for tag in tag_list]] = df_MVA['Time'].apply(pd.Series) #explode list to columns
                df_MVA['Time'] = df_MVA['Time'].apply(lambda x: add_timezone(x))
                
                #format
                df_MVA = df_MVA [['Batch', 'Phase', 'Time'] +[str(tag) for tag in tag_list]]
                df_MVA.reset_index(drop = True, inplace=True)
                df_MVA.sort_values(by=['Time'], ascending=True, inplace=True)
                for col in df_MVA.columns:
                    df_MVA.loc[df_MVA[col].apply(lambda x: str(type(x)) == "<class 'OSIsoft.AF.Asset.AFEnumerationValue'>"), col] = np.nan
                df_MVA.dropna(inplace=True) #in case of selecting nonsubsequent phases for a batch are filtered out (data is captured per batch in single call)
                return df_MVA
            
            else:
                raise Exception('It seems like you are using a flat dataframe, please specify a Level/Template')
            
        else: #condensed dataframe
            print('building MVA table from condensed eventframe dataframe...')

            #only Name columns and AFTimerange get selected
            cols = [col_name for col_name in event_dataframe.columns if col_name.startswith('Name')]
            col_starttime = [col_name for col_name in event_dataframe.columns if col_name.startswith('Starttime')][-1]
            col_endtime = [col_name for col_name in event_dataframe.columns if col_name.startswith('Endtime')][-1]
            
            if str(type(event_dataframe[col_starttime].iloc[0])) != "<class 'pandas._libs.tslibs.timestamps.Timestamp'>":
                raise Exception ('Starttime is of type '+ str(type(event_dataframe[col_starttime].iloc[0])) +'(do not use views)')
            if str(type(event_dataframe[col_endtime].iloc[0])) != "<class 'pandas._libs.tslibs.timestamps.Timestamp'>":
                raise Exception ('Endtime is of type '+ str(type(event_dataframe[col_endtime].iloc[0])) +'(do not use views)')
            
            cols.append(col_starttime)
            cols.append(col_endtime)
            df_mva = event_dataframe[cols].copy()
            df_mva.reset_index(drop = True, inplace=True)
            
            #get tag data as df, convert to list of tuples (.to_records) and explode
            print('Fetching Tag data...')
            
            df_MVA = pd.DataFrame()
            for i, proc in df_mva.groupby(cols[:-2]): #completely accurate would be per procedure & phase to have right starting time for each phase, this is more efficient though (per procedure) = Done?
                start = proc.iloc[0,-2]
                end = proc.iloc[-1,-1]
                values = list(self.get_interpolated_values_multiple(tag_list, start, end, interval).to_records(index=True))
                df_MVA = df_MVA.append(pd.DataFrame([[i, values]], columns=['Batch', 'Time']), ignore_index=True)

            df_MVA = df_MVA.explode('Time') #explode list to rows
            df_MVA['Time'] = df_MVA['Time'].apply(lambda x: [el for el in x]) #numpy record to list
            df_MVA[['Time'] + [str(tag) for tag in tag_list]] = df_MVA['Time'].apply(pd.Series) #explode list to columns
            df_MVA['Time'] = df_MVA['Time'].apply(lambda x: add_timezone(x))

            #add phase info back
            df_MVA['Batch'] = df_MVA['Batch'].apply(lambda x: tuple_split(list(x)))
            df_MVA[['Batch', 'Phase']] = pd.DataFrame(df_MVA['Batch'].tolist(), index=df_MVA.index)
            
            #format
            #df_MVA.drop([col_timerange] + cols[1:-1], 1, inplace=True)
            df_MVA = df_MVA [['Batch', 'Phase', 'Time'] +[str(tag) for tag in tag_list]]
            df_MVA.reset_index(drop = True, inplace=True)
            df_MVA.sort_values(by=['Time'], ascending=True, inplace=True)
            df_MVA.reset_index(drop = True, inplace=True)
            
            #remove time that is AFEnumerationValue
            df_MVA.loc[df_MVA['Time'].apply(lambda x: str(type(x)) == "<class 'OSIsoft.AF.Asset.AFEnumerationValue'>"), 'Time'] = np.nan
            df_MVA.dropna(inplace=True) #in case of previous (time out), or in case of selecting nonsubsequent phases for a batch are filtered out (data is captured per batch in single call)
            for col in df_MVA.columns:
                df_MVA.loc[df_MVA[col].apply(lambda x: str(type(x)) == "<class 'OSIsoft.AF.Asset.AFEnumerationValue'>"), col] = df_MVA.loc[df_MVA[col].apply(lambda x: str(type(x)) == "<class 'OSIsoft.AF.Asset.AFEnumerationValue'>"), col].astype(str)

            
            #for col in df_MVA.columns:
            #    df_MVA.loc[df_MVA[col].apply(lambda x: str(type(x)) == "<class 'OSIsoft.AF.Asset.AFEnumerationValue'>"), col] = 'XXXXXX'
            #df_MVA.dropna(inplace=True) #in case of previous (time out), or in case of selecting nonsubsequent phases for a batch are filtered out (data is captured per batch in single call)

            return df_MVA
    
    
    def to_overlay(self, event_dataframe, tag_list, interval, level=None):
        if 'Name' in event_dataframe.columns: #flat dataframe
            if level:
                #translate template to correspondinglevel if applicable
                if type(level) == str:
                    level = event_dataframe.loc[event_dataframe['Template']==level, 'Level'].iloc[0]
                print('building overlay table from flat eventframe dataframe...')
                
                if str(type(event_dataframe['Starttime'].iloc[0])) != "<class 'pandas._libs.tslibs.timestamps.Timestamp'>":
                    raise Exception ('Starttime is of type '+ str(type(event_dataframe['Starttime'].iloc[0])) +'(do not use views)')
                if str(type(event_dataframe['Endtime'].iloc[0])) != "<class 'pandas._libs.tslibs.timestamps.Timestamp'>":
                    raise Exception ('Endtime is of type '+ str(type(event_dataframe['Endtime'].iloc[0])) +'(do not use views)')
                    
                event_dataframe = event_dataframe[event_dataframe['Level'] == level][['Path', 'Starttime', 'Endtime']].copy()

                #Batch and phase based on path
                event_dataframe[['Batch','Phase']] = event_dataframe['Path'].str.split('\\', expand=True, n=5).loc[:, 4:]
                event_dataframe.replace({'Batch':{'EventFrames\[':'', '\]':''}}, regex=True, inplace=True) #partial replacements
                event_dataframe = event_dataframe[['Batch','Phase','Starttime','Endtime']]
                event_dataframe.reset_index(drop = True, inplace=True)
                
                #get tag data as df, convert to list of tuples (.to_records) and explode
                print('Fetching Tag data...')
                df_overlay = pd.DataFrame()
                for i, proc in event_dataframe.groupby('Batch'):
                    start = proc.iloc[0,-2]
                    end = proc.iloc[-1,-1]
                    values = list(self.get_interpolated_values_multiple(tag_list, start, end, interval).to_records(index=True))
                    df_overlay = df_overlay.append(pd.DataFrame([[i, values]], columns=['Batch', 'Time']), ignore_index=True)
                
                df_overlay = df_overlay.explode('Time') #explode list to rows
                df_overlay['Time'] = df_overlay['Time'].apply(lambda x: [el for el in x]) #numpy record to list
                df_overlay[['Time'] + [str(tag) for tag in tag_list]] = df_overlay['Time'].apply(pd.Series) #explode list to columns
                #add_timezone nog nut hier?
                df_overlay['Time'] = df_overlay['Time'].apply(lambda x: add_timezone(x))
                
                #add phase info back
                df_overlay['Phase'] = np.nan
                for i, row in event_dataframe.iterrows():
                    df_overlay['Phase'].loc[(df_overlay['Time'] >= row['Starttime']) & 
                                            (df_overlay['Time'] <= row['Endtime'])] = row['Phase']
                
                #format
                df_overlay = df_overlay[['Batch', 'Phase', 'Time'] +[str(tag) for tag in tag_list]]
                df_overlay.reset_index(drop = True, inplace=True)
                df_overlay.sort_values(by=['Time'], ascending=True, inplace=True)
                #for col in df_overlay.columns:
                #    df_overlay.loc[df_overlay[col].apply(lambda x: str(type(x)) == "<class 'OSIsoft.AF.Asset.AFEnumerationValue'>", col)] = np.nan
                return view(df_overlay)
            
            else:
                raise Exception('It seems like you are using a flat dataframe, please specify a Level/Template')
            
        else: #condensed dataframe
            print('building overlay table from condensed eventframe dataframe...')
            #only Name columns and AFTimerange get selected
            cols = [col_name for col_name in event_dataframe.columns if col_name.startswith('Name')]
            col_starttime = [col_name for col_name in event_dataframe.columns if col_name.startswith('Starttime')][-1]
            col_endtime = [col_name for col_name in event_dataframe.columns if col_name.startswith('Endtime')][-1]
            
            if str(type(event_dataframe[col_starttime].iloc[0])) != "<class 'pandas._libs.tslibs.timestamps.Timestamp'>":
                raise Exception ('Starttime is of type '+ str(type(event_dataframe[col_starttime].iloc[0])) +'(do not use views)')
            if str(type(event_dataframe[col_endtime].iloc[0])) != "<class 'pandas._libs.tslibs.timestamps.Timestamp'>":
                raise Exception ('Endtime is of type '+ str(type(event_dataframe[col_endtime].iloc[0])) +'(do not use views)')
            
            cols.append(col_starttime)
            cols.append(col_endtime)
            df_MVA = event_dataframe[cols].copy()
            df_MVA.reset_index(drop = True, inplace=True)
            
            #get tag data as df, convert to list of tuples (.to_records) and explode
            print('Fetching Tag data...')
            
            df_overlay = pd.DataFrame()
            for i, proc in df_MVA.groupby('Name [Procedure]'):
                start = proc.iloc[0,-2]
                end = proc.iloc[-1,-1]
                values = list(self.get_interpolated_values_multiple(tag_list, start, end, interval).to_records(index=True))
                df_overlay = df_overlay.append(pd.DataFrame([[i, values]], columns=['Batch', 'Time']), ignore_index=True)
            
            df_overlay = df_overlay.explode('Time') #explode list to rows
            df_overlay['Time'] = df_overlay['Time'].apply(lambda x: [el for el in x]) #numpy record to list
            #get tag values from point?
            #pd.DataFrame(df['b'].tolist(), index=df.index) instead of apply(pd.Series) = faster
            df_overlay[['Time'] + [str(tag) for tag in tag_list]] = df_overlay['Time'].apply(pd.Series) #explode list to columns
            df_overlay['Time'] = df_overlay['Time'].apply(lambda x: add_timezone(x))

            #add phase info back
            df_MVA['Phase'] = df_MVA[cols[1:-2]].apply(lambda x: '\\'.join(x.values.astype(str)), axis=1)
            df_overlay['Phase'] = np.nan
            for i, row in df_MVA.iterrows():
                df_overlay['Phase'].loc[(df_overlay['Time'] >= row[col_starttime]) & 
                                        (df_overlay['Time'] <= row[col_endtime])] = row['Phase']
            
            #format
            df_overlay = df_overlay[['Batch', 'Phase', 'Time'] +[str(tag) for tag in tag_list]]
            df_overlay.sort_values(by=['Time'], ascending=True, inplace=True)
            df_overlay.reset_index(drop = True, inplace=True)
            #for col in df_overlay.columns:
            #    df_overlay.loc[df_overlay[col].apply(lambda x: str(type(x)) == "<class 'OSIsoft.AF.Asset.AFEnumerationValue'>", col)] = np.nan
            return view(df_overlay)

            
    def get_tags(self, query):
        tags = self.search(str(query))
        
        df = pd.DataFrame()
        df['PIPoint'] = [tag for tag in tags]
        #load attributes before GET
        df['PIPoint'].apply(lambda x: x.LoadAttributes(['descriptor', 'engunits']))
        df['Name'] = df['PIPoint'].apply(lambda x: x.Name)
        df['Description'] = df['PIPoint'].apply(lambda x: x.GetAttribute('descriptor'))
        df['UOM'] = df['PIPoint'].apply(lambda x: x.GetAttribute('engunits'))
        #https://docs.osisoft.com/bundle/af-sdk/page/html/T_OSIsoft_AF_PI_PIPointType.htm
        df['PointType'] = df['PIPoint'].apply(lambda x: x.PointType)
        df['PointType_desc'] = df['PointType'].apply(lambda x: str(PIPointType(x)))
        return df
    
 #aux function
def tuple_split(x): #used in to_MVA
        x[1:] = ['\\'.join(x[1:])]
        return x

def generate_pipointlist(tag_list):
    '''Generate and populate object of PIPointList class from TagList object'''
    if not type(tag_list) == TagList:
        raise Exception ("Input is not a TagList object")

    PIPointlist = AF.PI.PIPointList()
    for tag in tag_list:
        PIPointlist.Add(tag.pipoint)
    return PIPointlist
    
def convert_to_TagList(tag_list, dataserver=None):
    ''' Convert list of strings OR list of Tag objects to Taglist '''
    if type(tag_list) == str:
        raise Exception ("Tag(s) need to be inside a list")
    elif type(tag_list) == TagList:
        return tag_list
    else:
        try:
            return TagList(tag_list)
        except:
            if dataserver:
                 return dataserver.search(tag_list)
            else:
                 raise AttributeError('Please specifiy a dataserver when using tags in string format')