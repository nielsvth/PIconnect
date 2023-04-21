#########
PIconnect
#########

A Python connector to the OSISoft PI AF SDK
========================================================

'nielsvth/PIconnect' provides a programmatic interface in Python to the OSISoft PI AF SDK. 
This branch expands upon the 'Hugovdberg/PIconnect' package to include additional functionality for working with Assets, Attributes and (hierarchical) EventFrames.
It also provides functionality for executing bulk queries, threading and calculations. 

The basic introduction to working with the PIconnect package is covered in the Tutorial below.

* Free software: MIT license

Installation
========================================================

It is good practice to create a new environment for each project.

General set-up instructions:

.. code-block:: console
    
    >> conda create --name <ENVIRONMENT NAME> --no-default-package
    >> conda activate <ENVIRONMENT NAME> 
    >> pip install git+https://github.com/nielsvth/PIconnect.git
    
**Attention:** 

* Pythonnet >= 3.0 is currently not compatible with the Spyder IDE
* Pythonnet >= 3.0 is not compatible with Python <= 3.6
* Pythonnet <= 2.5 is not compatible with Python >= 3.9

When using **Spyder IDE**, use following instructions for set-up:


.. code-block:: console
    
    >> conda create --name <ENVIRONMENT NAME> pythonnet==2.5.1 --no-default-package
    >> conda activate <ENVIRONMENT NAME> 
    >> pip install git+https://github.com/nielsvth/PIconnect.git

Tutorial
========================================================

1. Connection
*******************************************************

**AFDatabase**

A custom NuGreen' database was created to test the package functionality.
To import the NuGreen database, follow these steps:

1. Import the database from the provided "NuGreen.xml" via PI System Explorer>File>Import from File.
2. Verify that a root element "NuGreen" is created inside the NuGreen database.

**PIServer**

In case the default 'SINUSOID' and 'SINUSOIDU' tags do not exist on your PIserver: 

- `How do I generate sinusoid/sinusoidu data in Pi Server 2018? <https://pisquare.osisoft.com/s/question/0D51I00004UHjpQSAT/how-do-i-generate-sinusoidsinusoidu-data-in-pi-server-2018>`_


.. code-block:: python
    
    #import PIconnect
    
    # set up timezone. Pick timezone from
    # https://gist.github.com/heyalexej/8bf688fd67d7199be4a1682b3eec7568
    PIconnect.PIConfig.DEFAULT_TIMEZONE = "Europe/Brussels"

    # List of available PI data servers
    # PI Servers are used for accessing Tag (pipoint) data
    dataservers = list(PIconnect.PIServer.servers.keys())
    print(dataservers)

    # List of available PI AF servers, which are not empty
    # AF servers are used for accessing Event and Asset objects
    afservers = [
        servName
        for servName in PIconnect.PIAFDatabase.servers.keys()
        if len(PIconnect.PIAFDatabase.servers[servName]["databases"].keys()) > 0
    ]
    print(afservers)

    # List of available PI AF databases for first AF server in afservers list
    afdatabases = list(
        PIconnect.PIAFDatabase.servers[afservers[0]]["databases"].keys()
    )
    print(afdatabases)

    # Find AF server that contains custom "NuGreen" database
    # See instructions above on how to set up the "NuGreen" database for testing purposes
    afserver = [
        servName
        for servName in PIconnect.PIAFDatabase.servers
        if "NuGreen" in PIconnect.PIAFDatabase.servers[servName]["databases"].keys()
    ][0]

    # Initiate connection to default PI data server & "NuGreen" PI AF database
    with PIconnect.PIAFDatabase(
        server=afserver, database="NuGreen"
    ) as afdatabase, PIconnect.PIServer(server=dataservers[0]) as server:

        # print name of specified server
        print(server.name)

        # print server and database name for specified AF database
        print(f"{afdatabase.server_name}\\{afdatabase.database_name}")

        # ...<All other code blocks are inserted here>...


2. Asset
*******************************************************

Assets usually refer to pieces of equipment and are structured in hierarchies (Asset Framework, AF).
The following tutorial elaborates on the Asset class and some of its key attributes & methods. 

.. code-block:: python

    from datetime import datetime

    # Returns list of Assets that meets the query criteria
    # Here a query is executed for an Asset with name 'P-560'
    # For more info on how to construct queries, see further
    assetlist = afdatabase.find_assets(query="P-560")

    # Use '*' as a joker sign
    assetlist = afdatabase.find_assets(query="P*560")

    # Select the first Asset from the AssetList
    asset = assetlist[0]

    # Some Assetclass attributes
    print(asset.name)
    print(asset.parent.name)

    # Get EventList of Events on this Asset that meet the query criteria
    # Here a query is executed for Events with template name 'Operation_template' within
    # the defined timeframe
    events = asset.get_events(starttime="*-10000d", endtime="*")
    events = asset.get_events(
        starttime="1/1/2022 14:00", endtime="10/10/2022 18:00", template_name="Operation_template"
    )

    # Using datetime to avoid US vs. EU date confusion
    # Now specify both event name and event template within defined timeframe
    start_date = datetime(day=1, month=10, year=2022)
    end_date = datetime(day=10, month=10, year=2022)
    events = asset.get_events(
        query="Operation A", starttime=start_date, endtime=end_date, template_name="Operation_template"
    )

3. AssetList
*******************************************************

The AssetList class provides a list-like object that contains Asset objects. 


4. AssetHierarchy
*******************************************************

The AssetHierarchy objects provides a dataframe-like representation of the hierachical structure of the List Tree

.. code-block:: python
    
    # Returns list of Assets that meets the query criteria
    # Here a query is executed for an Asset location with name 'Equipment'
    assetlist = afdatabase.find_assets(query="Equipment")
    
    # get AssetHierarchy from assetlist from current location, up to 2 levels deep
    # Use assetlist[0].top_asset to find top level asset location
    assethierarchy = assetlist.get_asset_hierarchy(depth=2)

    # Make afhierarchy visible in variable explorer
    # (string & float representation)
    viewable = PIconnect.PI.view(assethierarchy)
    
    # For accessing AssetHierarchy methods, use accessor("ahy") -----
    # Condense the AssetHierarchy object to return a condensed, vertically layered
    # representation of the Asset Tree
    assethierarchy_condensed = assethierarchy.ahy.condense()
    
    # Make condensed afhierarchy visible in variable explorer
    # (string & float representation)
    viewable2 = PIconnect.PI.view(assethierarchy_condensed)


5. Event
*******************************************************

Events provide an easy way to capture process events and related system data.
An event frame encapsulates the time period of the event and links it to assets and attributes.

.. code-block:: python
    
    # Returns EventList with Events that meets the query criteria
    # Here a query is executed over the whole Event Hierarchy for Events that
    # contain the string 'Operation A'
    eventlist = afdatabase.find_events(
        query="Operation A", starttime="1/1/2022", endtime="*"
    )
    
    # Here a query is executed over the whole Event Hierarchy for Events that
    # have template name 'Phase'.
    # Using datetime to avoid US vs. EU date confusion
    start_date = datetime(day=1, month=3, year=2022)
    end_date = datetime(day=31, month=10, year=2022)
    eventlist = afdatabase.find_events(
        template_name="Operation_template", starttime=start_date, endtime=end_date
    )

    # Select an Event from the EventList 
    event =  eventlist[0]
    
    #Some Event class attributes
    print(event.name)
    print(event.parent.name)
    print(event.starttime)
    print(event.duration)
    print(event.template_name)
    print(event.attributes)
    print(event.ref_elements)

    # Some Event class methods
    # Return Dataframe of interpolated values for tags specified by list of
    # tagnames ["SINUSOID"] or Tags, for a defined interval within
    # the event
    interpol_values = event.interpolated_values(
        tag_list=["SINUSOID"], interval="1m", dataserver=server
    )
    
    # Optionally, specify a filter condition
    interpol_values = event.interpolated_values(
        tag_list=["SINUSOID"],
        interval="1m",
        filter_expression="'SINUSOID' > 40",
        dataserver=server,
    )
    
    # Return Dataframe of recorded values for tags specified by list of tagnames
    # (SINUSOID) or Tags, within the event
    recorded_values = event.recorded_values(
        tag_list=["SINUSOID"], dataserver=server
    )
 
    # Return specified summary measure(s) for tags specified by list of tagnames
    # (SINUSOID) or Tags within the event.

    # summary_types (SummaryType object): SummaryType objects separated by '|'. List given
    # below. E.g. "summary_types = SummaryType.Minimum | SummaryType.Maximum"
    # Do not forget to import the SummaryType object from PIconnect.PIConsts

    #     - Total: A total over the time span
    #     - Average: Average value over the time span
    #     - Minimum: The minimum value in the time span
    #     - Maximum: The maximum value in the time span
    #     - Range: The range of the values (max-min) in the time span
    #     - StdDev: The sample standard deviation of the values over the time span
    #     - PopulationStdDev: The population standard deviation of the values over the time span
    #     - Count: The sum of the event count (when the calculation is event weighted).
    #             The sum of the event time duration (when the calculation is time
    #             weighted.)
    #     - PercentGood: The percentage of the data with a good value over the time range.
    #             Based on time for time weighted calculations, based on event count for
    #             event weigthed calculations.
    #     - TotalWithUOM: The total over the time span, with the unit of measurement that's
    #             associated with the input (or no units if not defined for the input).
    #     - All: A convenience to retrieve all summary types
    #     - AllForNonNumeric: A convenience to retrieve all summary types for non-numeric data
    
    # import SummaryType object
    from PIconnect.PIConsts import SummaryType
    
    summary_values = event.summary(
        tag_list=["SINUSOID"], summary_types= SummaryType.Minimum | SummaryType.Maximum, dataserver=server
    )

    # Make summary dataframe visible in variable explorer
    # (string & float representation)
    viewable = PIconnect.PI.view(summary_values)
    
    # Return values voor specified attribute(s), if no arguments: returns all
    print(event.get_attribute_values())


6. EventList
*******************************************************

The EventList class provides a list-like object that contains Event objects. 


7. EventHierarchy
*******************************************************

The AssetHierarchy objects provides a dataframe-like representation of the hierachical structure of the Event Tree

.. code-block:: python

    # Returns EventList object that meets the query criteria
    # Here a query is executed over the whole Event Hierarchy for an Event that
    # contains the string 'Batch' within the specified time window 
    eventlist = afdatabase.find_events(
        query="*Batch*", starttime="1-9-2022", endtime="1-11-2022"
    )

    # Return event hierarchy down to the hierarchy depth specified, 
    # starting from the EventList
    eventhierarchy = eventlist.get_event_hierarchy(depth=2)

    # Starting from Event
    eventhierarchy = eventlist[0].get_event_hierarchy()

    # For accessing EventHierarchy methods, use accessor("ehy") -----

    # Add attribute values to EventHierarchy for specified attributes, defined for
    # the specified template. Here values are added for the attribute 'Manufacturer',
    # as defined for the 'Unit_template' template
    eventhierarchy = eventhierarchy.ehy.add_attributes(
        attribute_names_list=["Manufacturer"], template_name="Unit_template"
    )

    # Add referenced elements to EventHierarchy for specified event template
    # Here referenced elements are added that are defined for the the
    # 'Unit_template' template
    eventhierarchy = eventhierarchy.ehy.add_ref_elements(
        template_name="Unit_template"
    )

    # Make EventHierarchy dataframe visible in variable explorer
    # (string & float representation)
    viewable = PIconnect.PI.view(eventhierarchy)

    # Return dataframe of interpolated data for discrete events of EventHierarchy
    # Set 'col' argument to 'False' to specify a list of tags
    interpolated_values = eventhierarchy.ehy.interpol_discrete_extract(
        tag_list=["SINUSOID", "SINUSOIDU"],
        interval="1h",
        dataserver=server,
        col=False,
    )

    # Set 'col' argument to 'True' to have the ability to specify a column that
    # can contains tag per event
    eventhierarchy["tags"] = "SINUSOID" 
    eventhierarchy["tags"].iloc[0] = "SINUSOIDU"

    interpolated_values = eventhierarchy.ehy.interpol_discrete_extract(
        tag_list=["tags"], interval="1h", dataserver=server, col=True
    )
    
    # import SummaryType object
    from PIconnect.PIConsts import SummaryType
    
    # Return dataframe of summary data for discrete events of EventHierarchy
    summary_values = eventhierarchy.ehy.summary_extract(
        tag_list=["SINUSOID", "SINUSOIDU"],
        summary_types= SummaryType.Minimum | SummaryType.Maximum | SummaryType.StdDev,
        dataserver=server,
        col=False,
    )
    
    # Return dataframe of summary data for for calculated values over specified intervals per event
    # interval is set to 100h. As long as interval is bigger than the event duration, 
    # only one value is calculated for each summary over this event
    calc_summary_values = eventhierarchy.ehy.calc_summary_extract(
        interval="100h",
        summary_types= SummaryType.Minimum | SummaryType.Maximum,
        expression=r"('\\ITSBEBEPIHISCOL\SINUSOID')-('\\ITSBEBEPIHISCOL\SINUSOIDU')",
        col = False
    )
    
    
8. CondensedEventHierarchy
*******************************************************

The CondensedEventHierarchy object provides a dataframe-like representation of the condensed, vertically layered representation of the Event Tree.

.. code-block:: python
    
    # Returns EventList object that meets the query criteria
    eventlist = afdatabase.find_events(
        query="*Batch*", starttime="1-9-2022", endtime="1-11-2022"
    )

    # Return event hierarchy down to the depth specified, starting from the
    # Event(s) specified.
    eventhierarchy = eventlist.get_event_hierarchy(depth=2)

    # Add attribute values to EventHierarchy for specified attributes, defined for
    # the specified template. Here values are added for the attribute 'Manufacturer',
    # as defined for the 'Unit_template' template
    eventhierarchy = eventhierarchy.ehy.add_attributes(
        attribute_names_list=["Manufacturer"], template_name="Unit_template"
    )

    # Add referenced elements to EventHierarchy for specified event template
    # Here referenced elements are added that are defined for the the
    # 'Unit_template' template
    eventhierarchy = eventhierarchy.ehy.add_ref_elements(
        template_name="Unit_template"
    )

    # Condense the EventHierarchy object to return a condensed, vertically
    # layered representation of the Event Tree
    condensed = eventhierarchy.ehy.condense()

    # Use Pandas dataframe methods to filter out events of interest
    # In this case, only select events on equipment "P-560"
    df_cond = condensed[(condensed["Referenced_el [Unit_template](0)"] == "P-560")]

    # For accessing EventHierarchy methods, use accessor("ecd") -----
    # Return dataframe of interpolated values for discrete events on bottom level
    # of the condensed hierarchy
    disc_interpol_values = df_cond.ecd.interpol_discrete_extract(
        tag_list=["SINUSOID", "SINUSOIDU"],
        interval="1h",
        dataserver=server,
    )

    # Return dataframe of continous, interpolated values from the start of the
    # first filtered event to the end of the last filtered event for each
    # subsequent event on bottom level of the condensed hierarchy, by top-level event
    cont_interpol_values = df_cond.ecd.interpol_continuous_extract(
        tag_list=["SINUSOID", "SINUSOIDU"],
        interval="1h",
        dataserver=server,
    )

    # Return nested dictionary (level 1: Procedures, Level 2: Tags) of recorded
    # values from the start of the first filtered event to the end of the last 
    # filtered event for each subsequent event on the bottom level of the condensed hierarchy, by top-level event
    recorded_values = df_cond.ecd.recorded_extract(
        tag_list=["SINUSOID", "SINUSOIDU"], dataserver=server
    )
    
    # import SummaryType object
    from PIconnect.PIConsts import SummaryType
    
    # Return dataframe of summary data for events on bottom level of condensed
    # hierarchy
    summary_values = df_cond.ecd.summary_extract(
        tag_list=["SINUSOID", "SINUSOIDU"],
        summary_types= SummaryType.Average | SummaryType.Minimum | SummaryType.Maximum,
        dataserver=server,
    )
    
    # Return dataframe of summary data for for calculated values over specified intervals per bottom level event
    # interval is set to 100h. As long as interval is bigger than the event duration, 
    # only one value is calculated for each summary over this event
    calc_summary_values = condensed.ecd.calc_summary_extract(
        interval="100h",
        summary_types= SummaryType.Minimum | SummaryType.Maximum,
        expression=r"('\\ITSBEBEPIHISCOL\SINUSOID')-('\\ITSBEBEPIHISCOL\SINUSOIDU')",
        col = False
    )


9. Tag
*******************************************************

A Tag refers to a single data stream stored by PI Data Archive and is also known as a PIPoint.  

For example, a Tag might store the flow rate from a meter, a controller's mode of operation, the batch number of a product, text comments from an operator, or the results of a calculation.

.. code-block:: python
    
    # Returns comprehensive overview of tags that meet the query criteria
    # Quite slow and meant for tag exploration, for efficiently querying tags
    # the 'find_tags' method (cfr. infra) is preferred.
    tag_overview = server.tag_overview("SINUSOID*")

    # Make EventHierarchy dataframe visible in variable explorer
    # (string & float representation)
    viewable = PIconnect.PI.view(tag_overview)

    # Returns TagList with tags that meet the query criteria
    # Here a query is executed to find tag 'SINUSOID'
    taglist = server.find_tags("SINUSOID")

    # Select an Tag from the TagList
    tag = taglist[0]

    # Some Tag class attributes
    print(tag.name)
    print(tag.server)
    print(tag.description)
    print(tag.uom)
    print(tag.pointtype_desc)
    print(tag.created)
    print(tag.raw_attributes)

    # Return the last recorded value for a Tag
    current_value = tag.current_value()
    print(
        f"The value of {tag.name} ({tag.description}) at {tag.last_update}"
        + f" is {current_value[1]}{tag.uom}"
    )

    # Return interpolated values at the specified interval for Tag, between
    # starttime and endtime
    interpol_values = tag.interpolated_values(
        starttime="*-20d", endtime="*-10d", interval="1h"
    )

    # Return recorded values for Tag, between starttime and endtime
    recorded_values = tag.recorded_values(starttime="*-5d", endtime="*-2d")

    # Optionally, specify a filter condition
    # '%tag%' refers back to Tag name, and can be used for an individual tag
    #  When working with multiple tags, specificy full tag name
    recorded_values = tag.recorded_values(
        starttime="18/08/2022",
        endtime="19/08/2022",
        filter_expression="'%tag%' > 30",
    )

    # Retrieves values over the specified time range suitable for plotting over
    # the number of intervals (typically represents pixels). Returns a Dataframe
    # with values that will produce the most accurate plot over the time range
    # while minimizing the amount of data returned. Each interval can produce up
    # to 5 values if they are unique, the first value in the interval, the last
    # value, the highest value, the lowest value and at most one exceptional point
    # (bad status or digital state).
    plot_values = tag.plot_values(
        starttime="*-20d", endtime="*-10d", nr_of_intervals=10
    )
    
    # import SummaryType object
    from PIconnect.PIConsts import SummaryType
    
    # Return specified summary measure(s) for Tag within defined timeframe
    summary_values = tag.summary(
        starttime="*-20d", endtime="*-10d", summary_types= SummaryType.Average | SummaryType.Minimum | SummaryType.Maximum
    )

    # Return one or more summary values for each interval for a Tag, within a
    # specified timeframe
    summaries_values = tag.summaries(
        starttime="*-20d", endtime="*-10d", interval="1d", summary_types= SummaryType.Average | SummaryType.Minimum | SummaryType.Maximum
    )

    # Return one or more summary values for each interval for a Tag, within a
    # specified timeframe, for values that meet the specified filter condition
    filtered_summaries_values = tag.filtered_summaries(
        starttime="*-20d",
        endtime="*-10d",
        interval="1d",
        summary_types= SummaryType.Average | SummaryType.Minimum | SummaryType.Maximum,
        filter_expression="'\\ITSBEBEPIHISCOL\SINUSOID' > 30",
    )


10. TagList
*******************************************************

The TagList class provides a list-like object that contains Tag objects.

It is recommened to use the Taglist methods when collecting data for multiple Tags at once, as opposed to making calls for each Tags separately, as the performance for bulk calls will be superior. 

.. code-block:: python

    # Returns TagList with tags that meet the query criteria
    taglist = server.find_tags("*SINUSOID*")

    # Return the last recorded value for tags in TagList
    current_value = taglist.current_value()

    # Return interpolated values at the specified interval for tags in TagList, between
    # starttime and endtime
    interpol_values = taglist.interpolated_values(
        starttime="*-20d", endtime="*-10d", interval="1h"
    )

    # Return recorded values for tags in TagList, between starttime and endtime
    recorded_values = taglist.recorded_values(starttime="*-5d", endtime="*-2d")
    # Optionally, specify a filter condition
    recorded_values = taglist.recorded_values(
        starttime="18/08/2022",
        endtime="19/08/2022",
        filter_expression="'SINUSOID' > 30",
    )

    # Retrieves values over the specified time range suitable for plotting over
    # the number of intervals (typically represents pixels). Returns a Dataframe
    # with values that will produce the most accurate plot over the time range
    # while minimizing the amount of data returned. Each interval can produce up
    # to 5 values if they are unique, the first value in the interval, the last
    # value, the highest value, the lowest value and at most one exceptional point
    # (bad status or digital state).
    plot_values = taglist.plot_values(
        starttime="*-20d", endtime="*-10d", nr_of_intervals=10
    )
    
    # import SummaryType object
    from PIconnect.PIConsts import SummaryType

    # Return specified summary measure(s) for tags in TagList, within defined timeframe
    summary_values = taglist.summary(
        starttime="*-20d", endtime="*-10d", summary_types= SummaryType.Average | SummaryType.Minimum | SummaryType.Maximum
    )

    # Return one or more summary values for each interval for tags in TagList, within a
    # specified timeframe
    summaries_values = taglist.summaries(
        starttime="*-20d", endtime="*-10d", interval="1d", summary_types= SummaryType.Average | SummaryType.Minimum | SummaryType.Maximum
    )

    # Return one or more summary values for each interval for tags in TagList, within a
    # specified timeframe, for values that meet the specified filter condition
    filtered_summaries_values = taglist.filtered_summaries(
        starttime="*-20d",
        endtime="*-10d",
        interval="1d",
        summary_types= SummaryType.Average | SummaryType.Minimum | SummaryType.Maximum,
        filter_expression="'SINUSOID' > 30",
    )


11. Attribute
*******************************************************

The Attribute class provide an easy way to capture attribute data.
The Attribute represents a single value that is used to represent a specific piece of information that is part of an List or an Event.

.. code-block:: python
    
    # Returns list of Assets that meets the query criteria
    # Here a query is executed for an Asset with name 'P-560'
    assetlist = afdatabase.find_assets(query="P-560")

    # Select the first Asset from the AssetList
    asset = assetlist[0]

    # select first attribute for this asset
    attribute = asset.attributes[0]

    print(attribute.source_type)
    print(attribute.path)
    print(attribute.description)
    print(attribute.current_value())

    # select first asset attribute that has a Tag/PIpoint as a source
    attribute = [
        attribute 
        for attribute in asset.attributes
        if attribute.source_type == 'PI Point'][0]

    print(attribute.source_type)
    print(attribute.path)
    print(attribute.description)
    print(attribute.pipoint)
    print(attribute.current_value())


12. Calculation
*******************************************************

Core functionality for doing advanced calculations and filtering

.. code-block:: python

    # with '\\ITSBEBEPIHISCOL' as PIServer
    # Tags's full path need to be specified in the expression argument
    # expression argument needs to be passed as raw string (r"")
    
    # calculation on recorded values
    # for overview of expression syntax: 
    # https://docs.aveva.com/bundle/pi-server-af-analytics/page/1021946.html
    calc1 = PIconnect.calc.calc_recorded(
        "1-10-2022 14:00",
        "1-10-2022 22:00",
        r"IF ('\\ITSBEBEPIHISCOL\SINUSOID' > 70) THEN (Abs('\\ITSBEBEPIHISCOL\SINUSOID')) ELSE (0)",
    )

    # calculation on interpolated values
    calc2 = PIconnect.calc.calc_interpolated(
        "1-10-2022 14:00",
        "1-10-2022 14:00",
        "1h",
        r"TagTot('\\ITSBEBEPIHISCOL\SINUSOID', '01-Oct-2022 14:00:00', '03-Oct-2022 14:00:00')",
    )
    
    # import SummaryType object
    from PIconnect.PIConsts import SummaryType
    
    # calculation of summary measures of interval for calculated values
    # interval is set to 100h. As long as interval is bigger than the event duration, 
    # only one value is calculated for each summary over this event. 
    calc3 = PIconnect.calc.calc_summary(
        starttime = "1-10-2022 14:00",
        endtime = "1-10-2022 22:00",
        interval='100h',
        summary_types= SummaryType.Minimum | SummaryType.Maximum,
        expression=r"('\\ITSBEBEPIHISCOL\SINUSOID')-('\\ITSBEBEPIHISCOL\SINUSOIDU')",
    )


13. Threading
*******************************************************

Core functionality for doing I/O threading: requests will be split up in chunks of defined size and run in parallel, thereby increasing performance.

Threading is currently available for:

- PIconnect.PIAF.EventHierarchy.summary_extract
- PIconnect.PIAF.EventHierarchy.calc_summary_extract
- PIconnect.PIAF.EventHierarchy.interpol_discrete_extract

- PIconnect.PIAF.CondensedEventHierarchy.summary_extract
- PIconnect.PIAF.CondensedEventHierarchy.calc_summary_extract
- PIconnect.PIAF.CondensedEventHierarchy.interpol_discrete_extract

- All Taglist methods

.. code-block:: python
   
    #get a condensed hierarchy
    condensed = eventhierarchy.ehy.condense()
    
    # import SummaryType object
    from PIconnect.PIConsts import SummaryType
    
    #pass arguments as a dict
    x = dict(
        tag_list=["SINUSOID, SINUSOIDU"],
        summary_types= SummaryType.Minimum | SummaryType.Maximum,
        dataserver=server,
        col=False,
    )

    #initialize the threading function by providing 
    # source (Taglist, EventHierarchy or Condensed object), 
    # appropriate class method, 
    # args dict 
    # and chunk_size
    res = PIconnect.thread.threading(
        source = condensed,
        method = PIconnect.PIAF.CondensedEventHierarchy.summary_extract,
        args = x,
        chunk_size = 1000,
        )

    
14.  Attribute & Method Overview
*******************************************************

.. csv-table:: PIServer
   :header: "Atrribute/ Method", "Type", "Description"
   :widths: 30, 15, 50

   "**.servers**", "*Attribute*", "Return dictionary of type {servername: <OSIsoft.AF.PI.PIServer object>}"
   "**.default_server**", "*Attribute*", "Return <OSIsoft.AF.PI.PIServer object>"
   "**.name**", "*Attribute*", "Return name of connected server"
   "**.find_tags**
   (query, source=None)", "*Method*", "Return TagList object as a result of the query"
   "**.tag_overview**
   (query)", "*Method*", "Return dataframe containing overview of Tag object, tag name, description and UOM for each tag that meets the query criteria"
   
.. csv-table:: Tag
   :header: "Atrribute/ Method", "Type", "Description"
   :widths: 30, 15, 50

   "**.name**", "*Attribute*", "Return Tag name"
   "**.pipoint**", "*Attribute*", "Return <OSIsoft.AF.PI.PIPoint object>"
   "**.server**", "*Attribute*", "Return connected PIServer"
   "**.raw_attributes**", "*Attribute*", "Return dictionary of the raw attributes"
   "**.last_update**", "*Attribute*", "Return datetime at which the last value was recorded"
   "**.uom**", "*Attribute*", "Return units of measument"
   "**.description**", "*Attribute*", "Return description"
   "**.created**", "*Attribute*", "Return the creation datetime"
   "**.pointtype**", "*Attribute*", "Return an integer value corresponding to the pointtype (https://docs.osisoft.com/bundle/af-sdk/page/html/T_OSIsoft_AF_PI_PIPointType.htm)"
   "**.pointtype_desc**", "*Attribute*", "Return the pointtype"
   "**.current_value**
   ()", "*Method*", "Return tuple of timestamp and last recorded value"
   "**.interpolated_value**
   (timestamp)", "*Method*", "Return tuple of timestamp and interpolated value at specified time"
   "**.interpolated_values**
   (starttime, endtime, interval, filter_expression='')", "*Method*", "Return Dataframe of interpolated values at specified interval for Tag, between starttime and endtime"
   "**.recorded_values**
   (starttime, endtime, filter_expression='', AFBoundaryType=BoundaryType.Interpolated)", "*Method*", "Return Dataframe of recorded values for Tag, between starttime and endtime"
   "**.plot_values**
   (starttime, endtime, nr_of_intervals)", "*Method*", "Retrieves values over the specified time range suitable for plotting over the number of intervals (typically represents pixels). Returns a Dataframe with values that will produce the most accurate plot over the time range while minimizing the amount of data returned.Each interval can produce up to 5 values if they are unique, the first value in the interval, the last value, the highest value, the lowest value and at most one exceptional point (bad status or digital state)"
   "**.summary**
   (starttime, endtime, summary_types, calculation_basis=CalculationBasis.TimeWeighted, time_type=TimestampCalculation.Auto)", "*Method*", "Return specified summary measure(s) for Tag within the specified timeframe 
        
        Summary_types are defined as SummaryType objects separated by '|'
        fe: to extract min and max >> event.summary(['tag_x'], dataserver, SummaryType.Minimum|SummaryType.Maximum)
        Do not forget to import the SummaryType object from PIconnect.PIConsts"
   "**.summaries**
   (starttime, endtime, interval, summary_types, calculation_basis=CalculationBasis.TimeWeighted, time_type=TimestampCalculation.Auto)", "*Method*", "Return one or more summary values for each interval, within a specified timeframe"
   "**filtered_summaries**
   (starttime, endtime, interval,summary_types, filter_expression, calculation_basis=CalculationBasis.EventWeighted, time_type=TimestampCalculation.Auto, AFfilter_evaluation=ExpressionSampleType.ExpressionRecordedValues, filter_interval=None)", "*Method*", "Return one or more summary values for each interval, within a specified timeframe, for values that meet the specified filter condition"


.. csv-table:: TagList
   :header: "Atrribute/ Method", "Type", "Description"
   :widths: 30, 15, 50

   "**.current_value**
   ()", "*Method*", "Return Dataframe of current value per tag"
   "**.interpolated_value**
   (timestamp)", "*Method*", "Return Dataframe of interpolated value per tag at specified timestamp"
   "**.plot_values**
   (starttime, endtime, nr_of_intervals)", "*Method*", "Retrieves values over the specified time range suitable for plotting over the number of intervals (typically represents pixels). Returns a Dictionary of DataFrames for Tags in Taglist with values that will produce the most accurate plot over the time range while minimizing the amount of data returned"
   "**.interpolated_values**
   (starttime, endtime, interval, filter_expression='')", "*Method*", "Return Dataframe of interpolated values for Tags in TagList, between starttime and endtime"
   "**.recorded_values**
   (starttime, endtime, filter_expression='', AFBoundaryType=BoundaryType.Interpolated)", "*Method*", "Return dictionary of Dataframes of recorded values for Tags in TagList, between starttime and endtime"
   "**.summary**
   (starttime, endtime, summary_types, calculation_basis=CalculationBasis.TimeWeighted, time_type=TimestampCalculation.Auto)", "*Method*", "Return specified summary measure(s) for Tags in Taglist
        
        Summary_types are defined as SummaryType objects separated by '|'
        fe: to extract min and max >> event.summary(['tag_x'], dataserver, SummaryType.Minimum|SummaryType.Maximum)
        Do not forget to import the SummaryType object from PIconnect.PIConsts"
   "**.summaries**
   (starttime, endtime, interval, summary_types, calculation_basis=CalculationBasis.TimeWeighted, time_type=TimestampCalculation.Auto)", "*Method*", "Return one or more summary values for Tags in Taglist, for each interval within a time range"
   "**filtered_summaries**
   (self, starttime, endtime, interval,summary_types, filter_expression, calculation_basis=CalculationBasis.EventWeighted, time_type=TimestampCalculation.Auto, AFfilter_evaluation=ExpressionSampleType.ExpressionRecordedValues, filter_interval=None)", "*Method*", "Return one or more summary values for Tags in Taglist, (Optional: for each interval) that meet the filter criteria"


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
   (query=None, asset='*', start_time=None, end_time='*', template_name = None, start_index=0, max_count=1000000, search_mode=SearchMode.Inclusive, search_full_hierarchy=True, sortField=SortField.StartTime, sortOrder=SortOrder.Ascending)", "*Method*", "Return an EventList that meets query criteria"
   "**.find_assets**
   (query=None, top_asset=None, searchField=SearchField.Name, search_full_hierarchy=True, sortField=SortField.StartTime, sortOrder=SortOrder.Ascending, max_count=10000000)", "*Method*", "Return an AssetList that meets query criteria"
   
   
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
   "**.attributes**", "*Attribute*", "Return list of Attribute objects"
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
   (tag_list, dataserver=None, filter_expression='', AFBoundaryType=BoundaryType.Inside)", "*Method*", "Return Dataframe of recorded values for tags specified by list of tagnames or Tags, within the event"
   "**.summary**
   (tag_list, summary_types, dataserver=None, calculation_basis=CalculationBasis.TimeWeighted, time_type=TimestampCalculation.Auto)", "*Method*", "Return specified summary measure(s) for event
        
        Summary_types are defined as SummaryType objects separated by '|'
        fe: to extract min and max >> event.summary(['tag_x'], dataserver, SummaryType.Minimum|SummaryType.Maximum)
        Do not forget to import the SummaryType object from PIconnect.PIConsts"
   "**.summaries**
   (tag_list, interval, summary_types, dataserver=None, calculation_basis=CalculationBasis.TimeWeighted, time_type=TimestampCalculation.Auto)", "*Method*", "Return one or more summary values for Tags in Taglist, for each interval"
   "**.filtered_summaries**
   (tag_list, interval,summary_types, filter_expression, dataserver=None, calculation_basis=CalculationBasis.TimeWeighted, time_type=TimestampCalculation.Auto, AFfilter_evaluation=ExpressionSampleType.ExpressionRecordedValues, filter_interval=None)", "*Method*", "Return one or more summary values for Tags in Taglist, (Optional: for each interval) that meet filter the criteria"
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
   
   "**.ehy.add_attributes**
   (attribute_names_list, template_name)", "*Method*", "Add attribute values to EventHierarchy for specified attributes, for the specified template"
   "**.ehy.add_ref_elements**
   (template_name)", "*Method*", "Add referenced element values to EventHierarchy, defined for the specified template"
   "**.ehy.condense**
   ()", "*Method*", "Condense the EventHierarchy object to return a vertically layered CondensedEventHierarchy object"
   "**.ehy.interpol_discrete_extract**
   (tag_list, interval, filter_expression='', dataserver=None, col=False)", "*Method*", "Return dataframe of interpolated data for discrete events of EventHierarchy, for the tag(s) specified"
   "**.ehy.summary_extract**
   (tag_list, summary_types, dataserver=None, calculation_basis=CalculationBasis.TimeWeighted, time_type=TimestampCalculation.Auto, col=False)", "*Method*", "Return dataframe of summary measures for discrete events of EventHierarchy, for the tag(s) specified"
   "**.ehy.calc_summary_extract** 
   (interval, summary_types, expression, calculation_basis=CalculationBasis.TimeWeighted, time_type=TimestampCalculation.Auto, AFfilter_evaluation=ExpressionSampleType.ExpressionRecordedValues, filter_interval=None, col=False)", "*method*", "Returns dataframe of summary measures of calculations specified in expression, for the interval for each event in the Hierarchy. Expression argument need to be entered as raw strings: r'expression'."
   
   
.. csv-table:: CondensedEventHierarchy
   :header: "Atrribute/ Method", "Type", "Description"
   :widths: 30, 15, 50  
   
   "**.ecd.interpol_discrete_extract**
   (tag_list, interval, filter_expression='', dataserver=None, col=False)", "*Method*", "Return dataframe of interpolated values for discrete events on bottom level of condensed hierarchy"
   "**.ecd.interpol_continuous_extract**
   (tag_list, interval, filter_expression='', dataserver=None)", "*Method*", "Return dataframe of continous, interpolated values from the start of the first filtered event to the end of the last filtered event, for each procedure, on bottom level of condensed hierarchy"
   "**.ecd.recorded_extract**
   (tag_list, filter_expression='', AFBoundaryType=BoundaryType.Interpolated, dataserver=None)", "*Method*", "Return nested dictionary (level 1: Procedures, Level 2: Tags) of recorded data extracts from the start of the first filtered event to the end of the last filtered event for each procedure on bottom level of condensed hierarchy"
   "**.ecd.plot_continuous_extract**
   (tag_list, nr_of_intervals, dataserver=None)", "*Method*", "Return nested dictionary (level 1: Procedures, Level 2: Tags) of continuous plot values from the start of the first filtered event to the end of the last filtered event for each procedure on bottom level of condensed hierarchy. Each interval can produce up to 5 values if they are unique, the first value in the interval, the last value, the highest value, the lowest value and at most one exceptional point (bad status or digital state)"
   "**.ecd.summary_extract**
   (tag_list, summary_types, dataserver=None, calculation_basis=CalculationBasis.TimeWeighted, time_type=TimestampCalculation.Auto, col=False)", "*Method*", "Return dataframe of summary values for events on bottom level of condensed hierarchy"
    "**.ecd.calc_summary_extract** 
   (interval, summary_types, expression, calculation_basis=CalculationBasis.TimeWeighted, time_type=TimestampCalculation.Auto, AFfilter_evaluation=ExpressionSampleType.ExpressionRecordedValues, filter_interval, col=False)", "*method*", "Returns dataframe of summary measures of calculations specified in expression, for the interval for each event at bottom level of the CondensedHierarchy. Expression argument need to be entered as raw strings: r'expression'."


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
   (query=None, starttime, endtime, template_name = None, start_index=0, max_count=1000000, search_mode=SearchMode.OVERLAPPED, search_full_hierarchy=True, sortField=SortField.STARTTIME, sortOrder=SortOrder.ASCENDING)", "*Method*", "Return EventList of events on this Asset within specified time period that meets the query criteria"
   

.. csv-table:: AssetList
   :header: "Atrribute/ Method", "Type", "Description"
   :widths: 30, 15, 50   
   
   "**.get_asset_hierarchy**
   (depth=10)", "*Method*", "Return AssetHierarchy down to the specified depth"


.. csv-table:: AssetHierarchy
   :header: "Atrribute/ Method", "Type", "Description"
   :widths: 30, 15, 50  
   
   "**.ahy.add_attributes**
   (attribute_names_list, level)", "*Method*", "Add attributtes to AssetHierarchy for specified attributes and level"
   "**.ahy.condense**
   ()", "*Method*", "Condense the AssetHierarchy object to return a condensed, vertically layered representation of the Asset Tree"


.. csv-table:: Attribute
   :header: "Atrribute/ Method", "Type", "Description"
   :widths: 30, 15, 50  
    
   "**.name**", "*Attribute*", "Return name of Attribute"
   "**.path**", "*Attribute*", "Return path"
   "**.pisystem_name**", "*Attribute*", "Return PISystem name"
   "**.database_name**", "*Attribute*", "Return connected database name"
   "**.database**", "*Attribute*", "Return PIAFDatabase object"
   "**.af_attribute**", "*Attribute*", "Return <OSIsoft.AF.Asset.AFAttribute object>"
   "**.af_template**", "*Attribute*", "Return <OSIsoft.AF.Asset.AFAttributeTemplate object>"
   "**.template_name**", "*Attribute*", "Return template name"
   "**.parent**", "*Attribute*", "Return parent asset"
   "**.description**", "*Attribute*", "Return description"
   "**.uom**", "*Attribute*", "Return displayed Unit of Measurement (uom) for Attribute"
   "**.type**", "*Attribute*", "Return datatype of Attribute"
   "**.source_type**", "*Attribute*", "Return name of Attribute's data reference"
   "**.pipoint**", "*Attribute*", "Return Tag object, if exists"
   "**.current_value**
   ()", "*Method*", "Return current value for Attribute"


.. csv-table:: Calculation
   :header: "Atrribute/ Method", "Type", "Description"
   :widths: 30, 15, 50 

   "**PIconnect.calc.calc_recorded**
   (starttime, endtime, expression=r"")", "*Method*", "Returns dataframe that contains the result of evaluating the passed expression at each point in time over the passed time range where a recorded value exists for a member of the expression. Expression argument need to be entered as raw strings: r'expression'"
    "**PIconnect.calc.calc_interpolated**
   (starttime, endtime, interval, expression=r"")", "*Method*", "Returns dataframe that contains the result of evaluating the passed expression over the passed time range at a defined interval. Expression argument need to be entered as raw strings: r'expression'."
   "**calc.calc_summary**
   (starttime, endtime, interval, summary_types, expression, calculation_basis=CalculationBasis.TimeWeighted, time_type=TimestampCalculation.Auto, AFfilter_evaluation=ExpressionSampleType.ExpressionRecordedValues, filter_interval)", "*method*", "Returns dataframe of summary measures of calculations specified in expression, for the specified duration and interval. Expression argument need to be entered as raw strings: r'expression'."


15.    PIConstants
*******************************************************
PIConstants provides a defined set of arguments that can be passed to some of the class methods specified above to modify their behaviour. 
They are imported from the PIConsts module and used as illustrated in the example below. 

.. code-block:: python

    # import right class from PIConsts
    from PIconnect.PIConsts import BoundaryType

    # Initiate connection to PI data server & PI AF database of interest 
    
    #Select event
    eventlist = afdatabase.find_events(
        query="Operation A", starttime="1/1/2022", endtime="*"
        )
    event =  eventlist[0]

    # Return Dataframe of recorded values for tags specified by list of
    # tagnames (100_091_R014_TT04A) or Tags, within the event
    recorded_values = event.recorded_values(
        tag_list=["SINUSOID"],
        dataserver=server,
        AFBoundaryType=BoundaryType.Inside,
    )

    # Now let's change the AFBoundaryType argument to Interpolated
    # Class BoundaryType has following options:
    # Specifies to return the recorded values on the inside 
    #of the requested time range as the first and last values.
    - Inside
    # Specifies to return the recorded values on the outside 
    #of the requested time range as the first and last values.
    - Outside
    # Specifies to create an interpolated value 
    # at the end points of the requested time range
    # if a recorded value does not exist at that time.
    - Interpolated

    # lets set BoundaryType to BoundaryType.Interpolated
    recorded_values = event.recorded_values(
        tag_list=["SINUSOID"],
        dataserver=server,
        AFBoundaryType=BoundaryType.Interpolated,
    )


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


