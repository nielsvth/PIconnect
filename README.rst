#########
JanssenPI
#########

A Python connector to the OSISoft PI AF SDK
========================================================

JanssenPI provides a programmatic interface in Python to the OSISoft PI AF SDK. 
JanssenPI is a package which was built upon the PIconnect package to include additional functionality for working with Assets and hierarchical EventFrames.
It also provides added functionality for executing bulk queries. 

The basic introduction to working with the JanssenPI package is covered in the Tutorial below.

* Free software: MIT license

Tutorial
========================================================

1. Connection
*******************************************************

.. code-block:: python

    import JanssenPI
    
    #set up timezone
    #Pick timezone from https://gist.github.com/heyalexej/8bf688fd67d7199be4a1682b3eec7568
    JanssenPI.PIConfig.DEFAULT_TIMEZONE = 'Europe/Brussels'

    #List of available PI data servers
    dataservers = list(JanssenPI.PIServer.servers.keys())
    print(dataservers)

    #List of available PI AF servers
    afservers = list(JanssenPI.PIAFDatabase.servers.keys())
    print(afservers)

    #List of available PI AF databases for first AF server in afservers list
    afdatabases = list(JanssenPI.PIAFDatabase.servers[afservers[0]]['databases'].keys())
    print(afdatabases)

    #Initiate connection to PI data server & PI AF database of interest by defining their name
    with JanssenPI.PIAFDatabase(server=afservers[0], database=afdatabases[0]) as afdatabase, JanssenPI.PIServer(server=dataservers[0]) as server:

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

    #Returns list of Assets that meet the query criteria
    #Here a query is executed for an Asset with name '091_R022'
    #For more info on how to construct queries, see further
    assetlist = afdatabase.find_assets(query='091_R022')
    
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

2. AssetHierarchy
*******************************************************

.. code-block:: python
    
    #Return full Asset Framework up to specified hierachy depth
    afhierarchy = afdatabase.all_assets(depth=10)
    
    #Make afhierarchy visible in variable explorer (string & float representation)
    viewable = JanssenPI.PI.view(afhierarchy)
    
    #condense the AssetHierarchy object to return a vertically layered result 
    afhierarchy_condensed = afhierarchy.ahy.condense()
    
    #Make condensed afhierarchy visible in variable explorer (string & float representation)
    viewable2 = JanssenPI.PI.view(afhierarchy_condensed)

3. Events
*******************************************************

Events provide an easy way to capture process events and related system data.
An event frame encapsulates the time period of the event and links it to assets and attributes.

.. code-block:: python
    
    #Returns list of Events that meet the query criteria
    #Here a query is executed over the whole Event Hierarchy for an Event that contains the string 'UP_HR102164G401_R1'
    eventlist = afdatabase.find_events(query='*UP_HR102164G401_R1*', start_time='*-70d', end_time='*-10d')
    
    #Here a query is executed over the whole Event Hierarchy for an Event that has template name 'Phase'
    eventlist = afdatabase.find_events(template_name='Phase', start_time='01/03/2022', end_time='31/03/2022')
    
    #Select an Event from the Event list 
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
    #Return Dataframe of interpolated values for tags specified by list of tagnames or Tags, for a defined interval and within the specified time window
    interpol_values = event.interpolated_values(['100_091_R014_TT04A'], '1m', server)
    
    #Return Dataframe of recorded values for tags specified by list of tagnames or Tags, within the specified time window
    recorded_values = event.recorded_values(['100_091_R014_TT04A'], server)
    
    #Return specified summary measure(s) for event
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
    
    summary_values = event.summary(['100_091_R014_TT04A'], 4|8, server)
    
    #Make summary dataframe visible in variable explorer (string & float representation)
    viewable = JanssenPI.PI.view(summary_values)
    
    #Return values voor specified attribute(s), if no arguments: returns all
    print(event.get_attribute_values())

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


