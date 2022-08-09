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
    print(list(JanssenPI.PIServer.servers.keys()))

    #List of available PI AF servers 
    print(list(JanssenPI.PIAFDatabase.servers.keys()))

    #List of available PI AF databases for specified server
    print(list(JanssenPI.PIAFDatabase.servers['PIMS_EU_BEERSE_AF_PE']['databases'].keys()))

    #Initiate connection to PI data server & PI AF database of interest 
    with JanssenPI.PIAFDatabase(server='PIMS_EU_BEERSE_AF_PE', database='DeltaV-Events') as afdatabase, JanssenPI.PIServer(server='ITSBEBEPIHISCOL') as server:
        
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
    assetlist = afdatabase.find_assets(query='091_R022')
    
    #Select the Asset from the Asset list 
    asset = assetlist[0]
    
    #Some Asset class attributes
    print(asset.name)
    print(asset.parent.name)
    
    #Get EventList of Events on this Asset that meet the query criteria
    events = asset.get_events(start_time='*-50d', end_time='*')
    events = asset.get_events(start_time='*-50d', end_time='*', template_name='Phase')

2. AssetHierarchy
*******************************************************

.. code-block:: python
    
    #Return full Asset Framework up to specified hierachy depth
    afhierarchy = afdatabase.all_assets(depth=10)
    
    #Make afhierarchy visible in variable explorer (string & float representation)
    viewable = JanssenPI.PI.view(afhierarchy)


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


