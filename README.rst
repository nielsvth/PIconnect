#########
PIconnect
#########

A Python connector to the OSISoft PI AF SDK
========================================================

PIconnect provides a programmatic interface in Python to the OSISoft PI AF SDK. 
This branch expands upon the PIconnect package to include additional functionality for working with Assets and (hierarchical) EventFrames.
It also provides added functionality for executing bulk queries. 

The basic introduction to working with the PIconnect package is covered in the Tutorial below.

* Free software: MIT license

Tutorial
========================================================

1. Connection
*******************************************************

.. code-block:: python

    import PIconnect
    
    #set up timezone
    #Pick timezone from https://gist.github.com/heyalexej/8bf688fd67d7199be4a1682b3eec7568
    PIconnect.PIConfig.DEFAULT_TIMEZONE = 'Europe/Brussels'

    #List of available PI data servers
    #PI Servers are used for accessing Tag (pipoint) data
    dataservers = list(PIconnect.PIServer.servers.keys())
    print(dataservers)

    #List of available PI AF servers
    # AF servers are used for accessing Event and Asset objects
    afservers = list(PIconnect.PIAFDatabase.servers.keys())
    print(afservers)

    #List of available PI AF databases for first AF server in afservers list
    afdatabases = list(PIconnect.PIAFDatabase.servers[afservers[0]]['databases'].keys())
    print(afdatabases)

    #Initiate connection to PI data server & PI AF database of interest by defining their name
    with PIconnect.PIAFDatabase(server=afservers[0], database=afdatabases[0]) as afdatabase, PIconnect.PIServer(server=dataservers[0]) as server:

        #print name of specified server
        print(server.server_name)

        #print server and database name for specified AF database
        print(f'{afdatabase.server_name}\\{afdatabase.database_name}')

        #...<All other code blocks are inserted here>...

2. Asset
*******************************************************

Assets usually refer to pieces of equipment and are structured in hierarchies (Asset Framework, AF).
The following tutorial elaborates on the Asset class and some of its key attributes & methods. 

.. code-block:: python

    #Returns list of Assets that meets the query criteria
    #Here a query is executed for an Asset with name '091_R022'
    #For more info on how to construct queries, see further
    assetlist = afdatabase.find_assets(query='091_R022')
    
    #Use '*' as a joker sign
    assetlist = afdatabase.find_assets(query='*R022')
    
    #Select the Asset from the Asset list 
    asset = assetlist[0]
    
    #Some Asset class attributes
    print(asset.name)
    print(asset.parent.name)
    
    #Get EventList of Events on this Asset that meet the query criteria
    #Here a query is executed for Events with template name 'Phase' within the defined timeframe
    events = asset.get_events(start_time='*-50d', end_time='*')
    events = asset.get_events(start_time='*-50d', end_time='*', template_name='Phase')
    events = asset.get_events(start_time='01/03/2022', end_time='31/03/2022', template_name='Phase')

3. AssetHierarchy
*******************************************************

The AssetHierarchy objects provides a dataframe-like representation of the hierachical structure of the Asset Tree

.. code-block:: python
    
    #Return full Asset Framework up to specified hierachy depth
    afhierarchy = afdatabase.all_assets(depth=10)
    
    #Make afhierarchy visible in variable explorer (string & float representation)
    viewable = PIconnect.PI.view(afhierarchy)
    
    #For accessing AssetHierarchy methods, use accessor("ahy") -----
    
    #Condense the AssetHierarchy object to return a condensed, vertically layered representation of the Asset Tree
    afhierarchy_condensed = afhierarchy.ahy.condense()
    
    #Make condensed afhierarchy visible in variable explorer (string & float representation)
    viewable2 = PIconnect.PI.view(afhierarchy_condensed)

4. Event
*******************************************************

Events provide an easy way to capture process events and related system data.
An event frame encapsulates the time period of the event and links it to assets and attributes.

.. code-block:: python
    
    #Returns EventList with Events that meets the query criteria
    #Here a query is executed over the whole Event Hierarchy for an Event that contains the string 'UP_HR102164G401_R1'
    eventlist = afdatabase.find_events(query='*UP_HR102164G401_R1*', start_time='*-70d', end_time='*-10d')
    
    #Here a query is executed over the whole Event Hierarchy for an Event that has template name 'Phase'
    eventlist = afdatabase.find_events(template_name='Phase', start_time='01/03/2022', end_time='31/03/2022')
    
    #Select an Event from the EventList 
    event =  eventlist[0]
    
    #Some Event class attributes
    print(event.name)
    print(event.parent.name)
    print(event.starttime)
    print(event.duration)
    print(event.template_name)
    print(event.attributes)
    print(event.ref_elements)

    #some Event class methods
    #Return Dataframe of interpolated values for tags specified by list of tagnames (100_091_R014_TT04A) or Tags, for a defined interval within the event
    interpol_values = event.interpolated_values(tag_list=['100_091_R014_TT04A'], interval='1m', dataserver=server)
    #Optionally, specify a filter condition
    interpol_values = event.interpolated_values(tag_list=['100_091_R014_TT04A'], interval='1m', filter_expression="'100_091_R019_TT04A' > 20", dataserver=server)
    
    #Return Dataframe of recorded values for tags specified by list of tagnames (100_091_R014_TT04A) or Tags, within the event
    recorded_values = event.recorded_values(tag_list=['100_091_R014_TT04A'], dataserver=server)
    
    #Return specified summary measure(s) for tags specified by list of tagnames (100_091_R014_TT04A) or Tags within the event
    #Summary_types are defined as integers separated by '|'
    #Integer values for all summary measures are specified below:
        #- TOTAL = 1: A total over the time span
        #- AVERAGE = 2: Average value over the time span
        #- MINIMUM = 4: The minimum value in the time span
        #- MAXIMUM = 8: The maximum value in the time span
        #- RANGE = 16: The range of the values (max-min) in the time span
        #- STD_DEV = 32 : The sample standard deviation of the values over the time span
        #- POP_STD_DEV = 64: The population standard deviation of the values over the time span
        #- COUNT = 128: The sum of the event count (when the calculation is event weighted). The sum of the event time duration (when the calculation is time weighted.)
        #- PERCENT_GOOD = 8192: The percentage of the data with a good value over the time range. Based on time for time weighted calculations, based on event count for event weigthed calculations.
        #- TOTAL_WITH_UOM = 16384: The total over the time span, with the unit of measurement that's associated with the input (or no units if not defined for the input)
        #- ALL = 24831: A convenience to retrieve all summary types
        #- ALL_FOR_NON_NUMERIC = 8320: A convenience to retrieve all summary types for non-numeric data
    summary_values = event.summary(tag_list=['100_091_R014_TT04A'], summary_types=4|8, dataserver=server)
    
    #Make summary dataframe visible in variable explorer (string & float representation)
    viewable = PIconnect.PI.view(summary_values)
    
    #Return values voor specified attribute(s), if no arguments: returns all
    print(event.get_attribute_values())

5. EventList
*******************************************************

The EventList class provides a list-like object that contains Event objects. 

6. EventHierarchy
*******************************************************

The AssetHierarchy objects provides a dataframe-like representation of the hierachical structure of the Event Tree

.. code-block:: python

    #Returns EventList object that meets the query criteria
    #Here a query is executed over the whole Event Hierarchy for an Event that contains the string 'UP_HR102164G401_R1'
    eventlist = afdatabase.find_events(query='*UP_HR102164G401_R1*', start_time='*-70d', end_time='*-10d')
    
    #Return event hierarchy down to the depth specified, starting from the Event(s) specified. 
        #starting from EventList
    eventhierarchy = eventlist.get_event_hierarchy(depth=2)
        
        #Starting from Event
    eventhierarchy = eventlist[0].get_event_hierarchy()

    #For accessing EventHierarchy methods, use accessor("ehy") -----

    #Add attribute values to EventHierarchy for specified attributes, defined for the specified template
    #Here values are added for the attribute 'B_PH_INFO', defined for the Phase template
    eventhierarchy = eventhierarchy.ehy.add_attributes(attribute_names_list=['B_PH_INFO'], template_name='Phase')

    #Add referenced elements to EventHierarchy for specified event template/level
    #Here referenced elements are added that are defined for the the UnitProcedure template
    eventhierarchy = eventhierarchy.ehy.add_ref_elements(template_name='UnitProcedure')
    
    #Make EventHierarchy dataframe visible in variable explorer (string & float representation)
    viewable = PIconnect.PI.view(eventhierarchy)
    
    #Return dataframe of interpolated data for discrete events of EventHierarchy'''
    #Set 'col' argument to 'False' to specify a list of tags
    interpolated_values = eventhierarchy.ehy.interpol_discrete_extract(tag_list=['100_091_R019_TT04A', '100_091_R019_ST01'], interval='1h', dataserver=server, col=False)
    
    #Set 'col' argument to 'True' to have the ability to specify a column that contains tag per event
    interpolated_values = eventhierarchy.ehy.interpol_discrete_extract(tag_list=['column_name'], interval='1h', dataserver=server, col=True)
    
    #Return dataframe of summary data for discrete events of EventHierarchy'''
    summary_values = eventhierarchy.ehy.summary_extract(tag_list=['100_091_R019_TT04A', '100_091_R019_ST01'], summary_types=4|8|32, dataserver=server, col=False)
    
7. CondensedEventHierarchy
*******************************************************

The CondensedEventHierarchy object provides a dataframe-like representation of the condensed, vertically layered representation of the Event Tree.

.. code-block:: python
    
    #Returns EventList object that meets the query criteria
    eventlist = afdatabase.find_events(query='*UP_HR102164G401_R1*', start_time='*-70d', end_time='*-10d')
    
    #Return event hierarchy down to the depth specified, starting from the Event(s) specified. 
    eventhierarchy = eventlist.get_event_hierarchy(depth=2)

    #Add attribute values to EventHierarchy for specified attributes, defined for the specified template
    eventhierarchy = eventhierarchy.ehy.add_attributes(['B_PH_INFO'], template_name='Phase')
    
    #Add referenced elements to EventHierarchy for specified event template/level
    eventhierarchy = eventhierarchy.ehy.add_ref_elements(template_name='UnitProcedure')
    
    #Condense the EventHierarchy object to return a condensed, vertically layered representation of the Event Tree
    condensed = eventhierarchy.condense()
    
    #Use Pandas dataframe methods to filter out events of interest
    df_cond = condensed[(condensed['B_PH_INFO [Phase]'] >= 30010) & (condensed['B_PH_INFO [Phase]'] <= 30020)]
    
    #For accessing EventHierarchy methods, use accessor("ecd") -----
    
    #Return dataframe of interpolated values for discrete events on bottom level of condensed hierarchy
    disc_interpol_values = df_cond.ecd.interpol_discrete_extract(tag_list=['100_091_R014_TT04A', '100_091_R014_ST01'], interval='1m', dataserver=server)
    
    #Return dataframe of continous, interpolated values from the start of the first filtered event to the end of the last filtered event for each procedure on bottom level of condensed hierarchy
    cont_interpol_values = df_cond.ecd.interpol_continuous_extract(tag_list=['100_091_R014_TT04A', '100_091_R014_ST01'], interval='1m', dataserver=server)
    
    #Return nested dictionary (level 1: Procedures, Level 2: Tags) of recorded values from the start of the first filtered event to the end of the last filtered event for each procedure on bottom level of condensed hierarchy
    recorded_values = df_cond.ecd.recorded_extract(tag_list=['100_091_R014_TT04A', '100_091_R014_ST01'], dataserver=server)
    
    #Return dataframe of summary data for events on bottom level of condensed hierarchy
    summary_values = df_cond.ecd.summary_extract(tag_list=['100_091_R014_TT04A', '100_091_R014_ST01'], summary_types=2|4|8, dataserver=server)
   

8. Tag
*******************************************************

A Tag refers to a single data stream stored by PI Data Archive and is also known as a PIPoint.  

For example, a Tag might store the flow rate from a meter, a controller's mode of operation, the batch number of a product, text comments from an operator, or the results of a calculation.

.. code-block:: python
    
    #Returns comprhenesive overview of tags that meet the query criteria
    #Quite slow and meant for tag exploration, for efficiently querying tags the 'find_tags' method (cfr. infra) is preferred. 
    tag_overview = server.tag_overview('*091_R019*')
    
    #Make EventHierarchy dataframe visible in variable explorer (string & float representation)
    viewable = PIconnect.PI.view(tag_overview)
    
    #Returns TagList with tags that meet the query criteria
    #Here a query is executed to find tag '100_091_R019_TT04A'
    taglist = server.find_tags('*091_R019_TT04A') 
    
    #Select an Tag from the TagList
    tag =  taglist[0]
    
    #Some Tag class attributes
    print(tag.name)
    print(tag.server)
    print(tag.description)
    print(tag.uom)
    print(tag.pointtype_desc)
    print(tag.created)
    print(tag.raw_attributes)
    
    #Return the last recorded value for a Tag
    current_value = tag.current_value()
    print(f'The value of {tag.name} ({tag.description}) at {tag.last_update} is {current_value}{tag.uom}')
    
    #Return interpolated values at the specified interval for Tag, between starttime and endtime
    interpol_values = tag.interpolated_values(starttime='*-20d', endtime='*-10d', interval='1m')
    
    #Return recorded values for Tag, between starttime and endtime
    recorded_values = tag.recorded_values(starttime='*-5d', endtime='*-2d')
    #Optionally, specify a filter condition:'%tag%' refers back to Tag name
    recorded_values = tag.recorded_values(starttime='18/08/2021', endtime='19/08/2021', filter_expression="'%tag%' > 20")
    
    #Retrieves values over the specified time range suitable for plotting over the number of intervals (typically represents pixels)
    #Returns a Dataframe with values that will produce the most accurate plot over the time range while minimizing the amount of data returned
    #Each interval can produce up to 5 values if they are unique, the first value in the interval, the last value, the highest value, the lowest value and at most one exceptional point (bad status or digital state).
    plot_values = tag.plot_values(starttime='*-20d', endtime='*-10d', nr_of_intervals=10)
    
    #Return specified summary measure(s) for Tag within defined timeframe
    summary_values = tag.summary(starttime='*-20d', endtime='*-10d',  summary_types=2|4|8)
    
    #Return one or more summary values for each interval for a Tag, within a specified timeframe
    summaries_values = tag.summaries(starttime='*-20d', endtime='*-10d', interval='1d', summary_types=2|4|8)
    
    #Return one or more summary values for each interval for a Tag, within a specified timeframe, for values that meet the specified filter condition
    filtered_summaries_values = tag.filtered_summaries(starttime='*-20d', endtime='*-10d', interval='1d', summary_types=2|4|8, filter_expression="'100_091_R019_TT04A' > 20")
   

9. TagList
*******************************************************

The TagList class provides a list-like object that contains Tag objects.

It is recommened to use the Taglist methods when collecting data for multiple Tags at once, as opposed to making calls for each Tags separately, as the performance for bulk calls will be superior. 

.. code-block:: python

    #Returns TagList with tags that meet the query criteria
    taglist = server.find_tags('*091_R019_TT0*') 
    
    #Return the last recorded value for a Tag
    current_value = taglist.current_value()
    
    #Return interpolated values at the specified interval for Tag, between starttime and endtime
    interpol_values = taglist.interpolated_values(starttime='*-20d', endtime='*-10d', interval='1m')
    
    #Return recorded values for Tag, between starttime and endtime
    recorded_values = taglist.recorded_values(starttime='*-5d', endtime='*-2d')
    #Optionally, specify a filter condition
    recorded_values = taglist.recorded_values(starttime='18/08/2021', endtime='19/08/2021', filter_expression="'100_091_R019_TT01A' > 20")
    
    #Retrieves values over the specified time range suitable for plotting over the number of intervals (typically represents pixels)
    #Returns a Dataframe with values that will produce the most accurate plot over the time range while minimizing the amount of data returned
    #Each interval can produce up to 5 values if they are unique, the first value in the interval, the last value, the highest value, the lowest value and at most one exceptional point (bad status or digital state).
    plot_values = taglist.plot_values(starttime='*-20d', endtime='*-10d', nr_of_intervals=10)
    
    #Return specified summary measure(s) for Tag within defined timeframe
    summary_values = taglist.summary(starttime='*-20d', endtime='*-10d',  summary_types=2|4|8)
    
    #Return one or more summary values for each interval for a Tag, within a specified timeframe
    summaries_values = taglist.summaries(starttime='*-20d', endtime='*-10d', interval='1d', summary_types=2|4|8)
    
    #Return one or more summary values for each interval for a Tag, within a specified timeframe, for values that meet the specified filter condition
    filtered_summaries_values = taglist.filtered_summaries(starttime='*-20d', endtime='*-10d', interval='1d', summary_types=2|4|8, filter_expression="'100_091_R019_TT04A' > 20")
   

10. Attribute & Method Overview
*******************************************************

.. csv-table:: PIServer
   :header: "Atrribute/ Method", "Type", "Description"
   :widths: 30, 15, 50

   "**.servers**", "*Attribute*", "Return dictionary of type {servername: <OSIsoft.AF.PI.PIServer object>}"
   "**.default_server**", "*Attribute*", "Return <OSIsoft.AF.PI.PIServer object>"
   "**.server_name**", "*Attribute*", "Return name of connected server"
   "**.find_tags**
   (query, source=None)", "*Method*", "Return list of Tag objects as a result of the query"
   "**.tag_overview**
   (query)", "*Method*", "Return dataframe containing overview of Tag object, tag name, description and UOM for each tag that meets the restrictions specified in the query"
   
.. csv-table:: Tag
   :header: "Atrribute/ Method", "Type", "Description"
   :widths: 30, 15, 50

   "**.name**", "*Attribute*", "Return name of Tag (PIPoint)"
   "**.pipoint**", "*Attribute*", "Return <OSIsoft.AF.PI.PIPoint object>"
   "**.server**", "*Attribute*", "Return connected server"
   "**.raw_attributes**", "*Attribute*", "Return dictionary of the raw attributes"
   "**.last_update**", "*Attribute*", "Return datetime at which the last value was recorded"
   "**.uom**", "*Attribute*", "Return units of measument"
   "**.description**", "*Attribute*", "Return description"
   "**.created**", "*Attribute*", "Return the creation datetime"
   "**.pointtype**", "*Attribute*", "Return an integer value corresponding to the pointtype (https://docs.osisoft.com/bundle/af-sdk/page/html/T_OSIsoft_AF_PI_PIPointType.htm)"
   "**.pointtype_desc**", "*Attribute*", "Return the pointtype"
   "**.current_value**
   ()", "*Method*", "Return last recorded value"
   "**.interpolated_values**
   (starttime, endtime, interval, filter_expression='')", "*Method*", "Return Dataframe of interpolated values at specified interval for Tag, between starttime and endtime"
   "**.recorded_values**
   (starttime, endtime, filter_expression='', AFBoundaryType=BoundaryType.INTERPOLATED)", "*Method*", "Return Dataframe of recorded values for Tag, between starttime and endtime"
   "**.plot_values**
   (starttime, endtime, nr_of_intervals)", "*Method*", "Retrieves values over the specified time range suitable for plotting over the number of intervals (typically represents pixels). Returns a Dataframe with values that will produce the most accurate plot over the time range while minimizing the amount of data returned.Each interval can produce up to 5 values if they are unique, the first value in the interval, the last value, the highest value, the lowest value and at most one exceptional point (bad status or digital state)"
   "**.summary**
   (starttime, endtime, summary_types, calculation_basis=CalculationBasis.TIME_WEIGHTED, time_type=TimestampCalculation.AUTO)", "*Method*", "Return specified summary measure(s) for Tag within the specified timeframe 
        
        Summary_types are defined as integers separated by '|'
        fe: to extract min and max >> event.summary(['tag_x'], dataserver, 4|8)"
   "**.summaries**
   (starttime, endtime, interval, summary_types, calculation_basis=CalculationBasis.TIME_WEIGHTED, time_type=TimestampCalculation.AUTO)", "*Method*", "Return one or more summary values for each interval, within a specified timeframe"
   "**filtered_summaries**
   (starttime, endtime, interval,summary_types, filter_expression, calculation_basis=CalculationBasis.TIME_WEIGHTED, time_type=TimestampCalculation.AUTO, AFfilter_evaluation=ExpressionSampleType.EXPRESSION_RECORDED_VALUES, filter_interval=None)", "*Method*", "Return one or more summary values for each interval, within a specified timeframe, for values that meet the specified filter condition"


.. csv-table:: TagList
   :header: "Atrribute/ Method", "Type", "Description"
   :widths: 30, 15, 50

   "**.current_values**
   ()", "*Method*", "Return Dataframe of current values per tag"
   "**.plot_values**
   (starttime, endtime, nr_of_intervals)", "*Method*", "Retrieves values over the specified time range suitable for plotting over the number of intervals (typically represents pixels). Returns a Dictionary of DataFrames for Tags in Taglist with values that will produce the most accurate plot over the time range while minimizing the amount of data returned"
   "**.interpolated_values**
   (starttime, endtime, interval, filter_expression='')", "*Method*", "Return Dataframe of interpolated values for Tags in TagList, between starttime and endtime"
   "**.recorded_values**
   (starttime, endtime, filter_expression='', AFBoundaryType=BoundaryType.INTERPOLATED)", "*Method*", "Return dictionary of Dataframes of recorded values for Tags in TagList, between starttime and endtime"
   "**.summary**
   (starttime, endtime, summary_types, calculation_basis=CalculationBasis.TIME_WEIGHTED, time_type=TimestampCalculation.AUTO)", "*Method*", "Return specified summary measure(s) for Tags in Taglist
        
        Summary_types are defined as integers separated by '|'
        fe: to extract min and max >> event.summary(['tag_x'], dataserver, 4|8)"
   "**.summaries**
   (starttime, endtime, interval, summary_types, calculation_basis=CalculationBasis.TIME_WEIGHTED, time_type=TimestampCalculation.AUTO)", "*Method*", "Return one or more summary values for Tags in Taglist, for each interval within a time range"
   "**filtered_summaries**
   (self, starttime, endtime, interval,summary_types, filter_expression, calculation_basis=CalculationBasis.TIME_WEIGHTED, time_type=TimestampCalculation.AUTO, AFfilter_evaluation=ExpressionSampleType.EXPRESSION_RECORDED_VALUES, filter_interval=None)", "*Method*", "Return one or more summary values for Tags in Taglist, (Optional: for each interval) that meet the filter criteria"


.. csv-table:: PIAFDatabase
   :header: "Atrribute/ Method", "Type", "Description"
   :widths: 30, 15, 50

   "**.servers**", "*Attribute*", "Return dictionary of type {servername: <OSIsoft.AF.PI.PIServer object>, 'database':{databasename: <OSIsoft.AF.AFDatabase object>}}"
   "**.default_server**", "*Attribute*", "Return dictionary of type {servername: <OSIsoft.AF.PI.PIServer object>, 'database':{databasename: <OSIsoft.AF.AFDatabase object>}} for default server"
   "**.server_name**", "*Attribute*", "Return name of connected server"
   "**.database_name**", "*Attribute*", "Return name of connected database"
   "**.children**", "*Attribute*", "Return dictionary of the direct child elements of the database"
   "**.descendant**
   (path)", "*Method*", "Return a descendant of the database from an exact path"
   "**.find_events**
   (query=None, asset='*', start_time=None, end_time='*', template_name = None, start_index=0, max_count=1000000, search_mode=SearchMode.OVERLAPPED, search_full_hierarchy=True, sortField=SortField.STARTTIME, sortOrder=SortOrder.ASCENDING)", "*Method*", "Return a EventList of Events that meet query criteria"
   "**.find_assets**
   (query=None, top_asset=None, searchField=SearchField.NAME, search_full_hierarchy=True, sortField=SortField.STARTTIME, sortOrder=SortOrder.ASCENDING, max_count=10000000)", "*Method*", "Return list of Assets that meet query criteria"
   
   
.. csv-table:: Event
   :header: "Atrribute/ Method", "Type", "Description"
   :widths: 30, 15, 50

   "**.name**", "*Attribute*", "Return name of event"
   "**.path**", "*Attribute*", "Return path"
   "**.pisystem_name**", "*Attribute*", "Return PISystem name"
   "**.database_name**", "*Attribute*", "Return connected database name"
   "**.database**", "*Attribute*", "Return PIAFDatabase object"
   "**.af_eventframe**", "*Attribute*", "Return <OSIsoft.AF.EventFrame.AFEventFrame object>"
   "**.af_template**", "*Attribute*", "Return <OSIsoft.AF.Asset.AFElementTemplate object>"
   "**.template_name**", "*Attribute*", "Return template name"
   "**.starttime**", "*Attribute*", "Return starttime"
   "**.endtime**", "*Attribute*", "Return endtime"
   "**.af_timerange**", "*Attribute*", "Return <OSIsoft.AF.Time.AFTimeRange object>"
   "**.attributes**", "*Attribute*", "Return list of attribute names"
   "**.af_attributes**", "*Attribute*", "Return list of <OSIsoft.AF.Asset.AFAttribute objects>"
   "**.children**", "*Attribute*", "Return EventList of children"
   "**.parent**", "*Attribute*", "Return parent event"
   "**.description**", "*Attribute*", "Return description"
   "**.duration**", "*Attribute*", "Return duration as datetime.timedelta object"
   "**.top_event**", "*Attribute*", "Return top-level event name"
   "**.plot_values**
   (tag_list, nr_of_intervals, dataserver=None)", "*Method*", "Retrieves values over the specified time range suitable for plotting over the number of intervals (typically represents pixels). Returns a Dictionary of DataFrames for tags specified by list of tagnames or Tags within the event, with values that will produce the most accurate plot over the time range while minimizing the amount of data returned. Each interval can produce up to 5 values if they are unique, the first value in the interval, the last value, the highest value, the lowest value and at most one exceptional point (bad status or digital state)"
   "**.interpolated_values**
   (tag_list, interval, dataserver=None, filter_expression='')", "*Method*", "Return Dataframe of interpolated values for tags specified by list of tagnames or Tags, for a defined interval within the event"
   "**.recorded_values**
   (tag_list, dataserver=None, filter_expression='', AFBoundaryType=BoundaryType.INSIDE)", "*Method*", "Return Dataframe of recorded values for tags specified by list of tagnames or Tags, within the event"
   "**.summary**
   (tag_list, summary_types, dataserver=None, calculation_basis=CalculationBasis.TIME_WEIGHTED, time_type=TimestampCalculation.AUTO)", "*Method*", "Return specified summary measure(s) for event
        
        Summary_types are defined as integers separated by '|'
        fe: to extract min and max >> event.summary(['tag_x'], dataserver, 4|8)"
   "**.summaries**
   (tag_list, interval, summary_types, dataserver=None, calculation_basis=CalculationBasis.TIME_WEIGHTED, time_type=TimestampCalculation.AUTO)", "*Method*", "Return one or more summary values for Tags in Taglist, for each interval"
   "**.filtered_summaries**
   (tag_list, interval,summary_types, filter_expression, dataserver=None, calculation_basis=CalculationBasis.TIME_WEIGHTED, time_type=TimestampCalculation.AUTO, AFfilter_evaluation=ExpressionSampleType.EXPRESSION_RECORDED_VALUES, filter_interval=None)", "*Method*", "Return one or more summary values for Tags in Taglist, (Optional: for each interval) that meet filter the criteria"
   "**.get_attribute_values**
   (attribute_names_list=[])", "*Method*", "Return dict of attribute values for specified attributes"
   "**.get_event_hierarchy**
   (depth=10)", "*Method*", "Return EventHierarchy down to the specified depth"
   
   
.. csv-table:: EventList
   :header: "Atrribute/ Method", "Type", "Description"
   :widths: 30, 15, 50   
   
   "**.to_set**
   ()", "*Method*", "Return EventList as set"
   "**.get_event_hierarchy**
   (depth=10)", "*Method*", "Return EventHierarchy down to the specified depth"
   
.. csv-table:: EventHierarchy
   :header: "Atrribute/ Method", "Type", "Description"
   :widths: 30, 15, 50   
   
   "**.add_attributes**
   (attribute_names_list, template_name)", "*Method*", "Add attribute values to EventHierarchy for specified attributes, defined for the specified template"
   "**.add_ref_elements**
   (template_name)", "*Method*", "Add referenced element values to EventHierarchy, defined for the specified template"
   "**.condense**
   ()", "*Method*", "Condense the EventHierarchy object to return a vertically layered CondensedEventHierarchy object"
   "**.interpol_discrete_extract**
   (tag_list, interval, filter_expression='', dataserver=None, col=False)", "*Method*", "Return dataframe of interpolated data for discrete events of EventHierarchy, for the tag(s) specified"
   "**.summary_extract**
   (tag_list, summary_types, dataserver=None, calculation_basis=CalculationBasis.TIME_WEIGHTED, time_type=TimestampCalculation.AUTO, col=False)", "*Method*", "Return dataframe of summary measures for discrete events of EventHierarchy, for the tag(s) specified"
   
   
.. csv-table:: CondensedEventHierarchy
   :header: "Atrribute/ Method", "Type", "Description"
   :widths: 30, 15, 50  
   
   "**.interpol_discrete_extract**
   (tag_list, interval, filter_expression='', dataserver=None, col=False)", "*Method*", "Return dataframe of interpolated values for discrete events on bottom level of condensed hierarchy"
   "**.interpol_continuous_extract**
   (tag_list, interval, filter_expression='', dataserver=None)", "*Method*", "Return dataframe of continous, interpolated values from the start of the first filtered event to the end of the last filtered event, for each procedure, on bottom level of condensed hierarchy"
   "**.recorded_extract**
   (tag_list, filter_expression='', AFBoundaryType=BoundaryType.INTERPOLATED, dataserver=None)", "*Method*", "Return nested dictionary (level 1: Procedures, Level 2: Tags) of recorded data extracts from the start of the first filtered event to the end of the last filtered event for each procedure on bottom level of condensed hierarchy"
   "**.plot_continuous_extract**
   (tag_list, nr_of_intervals, dataserver=None)", "*Method*", "Return nested dictionary (level 1: Procedures, Level 2: Tags) of continuous plot values from the start of the first filtered event to the end of the last filtered event for each procedure on bottom level of condensed hierarchy. Each interval can produce up to 5 values if they are unique, the first value in the interval, the last value, the highest value, the lowest value and at most one exceptional point (bad status or digital state)"
   "**.summary_extract**
   (tag_list, summary_types, dataserver=None, calculation_basis=CalculationBasis.TIME_WEIGHTED, time_type=TimestampCalculation.AUTO, col=False)", "*Method*", "Return dataframe of summary values for events on bottom level of condensed hierarchy"


.. csv-table:: Asset
   :header: "Atrribute/ Method", "Type", "Description"
   :widths: 30, 15, 50  
   
   "**.name**", "*Attribute*", "Return name of Asset"
   "**.path**", "*Attribute*", "Return path"
   "**.pisystem_name**", "*Attribute*", "Return PISystem name"
   "**.database_name**", "*Attribute*", "Return connected database name"
   "**.database**", "*Attribute*", "Return PIAFDatabase object"
   "**.af_asset**", "*Attribute*", "Return <OSIsoft.AF.Asset.AFElement object>"
   "**.af_template**", "*Attribute*", "Return <OSIsoft.AF.Asset.AFElementTemplate object>"
   "**.template_name**", "*Attribute*", "Return template name"
   "**.attributes**", "*Attribute*", "Return list of attribute names"
   "**.af_attributes**", "*Attribute*", "Return list of <OSIsoft.AF.Asset.AFAttribute objects>"
   "**.children**", "*Attribute*", "Return list of children"
   "**.parent**", "*Attribute*", "Return parent asset"
   "**.description**", "*Attribute*", "Return description"
   "**.get_attribute_values**
   (attribute_names_list=[])", "*Method*", "Return dict of attribute values for specified attributes"
   "**.get_events**
   (query=None, start_time=None, end_time='*', template_name = None, start_index=0, max_count=1000000, search_mode=SearchMode.OVERLAPPED, search_full_hierarchy=True, sortField=SortField.STARTTIME, sortOrder=SortOrder.ASCENDING)", "*Method*", "Return EventList of Events on Asset within specified time period that meets the query criteria"
   

.. csv-table:: AssetHierarchy
   :header: "Atrribute/ Method", "Type", "Description"
   :widths: 30, 15, 50  
   
   "**.add_attributes**
   (attribute_names_list, level)", "*Method*", "Add attributtes to AssetHierarchy for specified attributes and level"
   "**.condense**
   ()", "*Method*", "Condense the AssetHierarchy object to return a condensed, vertically layered representation of the Asset Tree"


11. PIConstants
*******************************************************
PIConstants provides a defined set of arguments that can be passed to some of the class methods specified above to modify their behaviour. 
They are imported from the PIConsts module and used as illustrated in the example below. 

.. code-block:: python

    import PIconnect

    #Initiate connection to PI data server & PI AF database of interest by defining their name
    with PIconnect.PIAFDatabase(server=afservers[0], database=afdatabases[0]) as afdatabase, PIconnect.PIServer(server=dataservers[0]) as server:
        
        #Return Dataframe of recorded values for tags specified by list of tagnames (100_091_R014_TT04A) or Tags, within the event
        recorded_values = event.recorded_values(tag_list=['100_091_R014_TT04A'], dataserver=server, AFBoundaryType=BoundaryType.INSIDE)
        
        #Now let's change the AFBoundaryType argument to INTERPOLATED
        #Class BoundaryType has following options:
            #Return the recorded values on the inside of the requested time range as the first and last values.
            #INSIDE = 0
            #Return the recorded values on the outside of the requested time range as the first and last values.
            #OUTSIDE = 1
            #Create an interpolated value at the end points of the requested time range if a recorded value does not exist at that time.
            #INTERPOLATED = 2
            
        #import right class from PIConsts
        from PIConsts import BoundaryType
        
        #lets set BoundaryType to BoundaryType.INTERPOLATED
        recorded_values = event.recorded_values(tag_list=['100_091_R014_TT04A'], dataserver=server, AFBoundaryType=BoundaryType.INTERPOLATED)
    

Copyright notice
================
OSIsoft, the OSIsoft logo and logotype, Managed PI, OSIsoft Advanced Services,
OSIsoft Cloud Services, OSIsoft Connected Services, PI ACE, PI Advanced
Computing Engine, PI AF SDK, PI API, PI Asset Framework, PI Audit Viewer, PI
Builder, PI Cloud Connect, PI Connectors, PI Data Archive, PI DataLink, PI
DataLink Server, PI Developer's Club, PI Integrator for Business Analytics, PI
Interfaces, PI JDBC driver, PI Manual Logger, PI Notifications, PI ODBC, PI
OLEDB Enterprise, PI OLEDB Provider, PI OPC HDA Server, PI ProcessBook, PI
SDK, PI Server, PI Square, PI System, PI System Access, PI Vision, PI
Visualization Suite, PI Web API, PI WebParts, PI Web Services, RLINK and
RtReports are all trademarks of OSIsoft, LLC.


