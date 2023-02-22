import PIconnect
import datetime


# https://realpython.com/intro-to-python-threading/

# Set up timezone info
PIconnect.PIConfig.DEFAULT_TIMEZONE = "Europe/Brussels"

with PIconnect.PIAFDatabase(
    server="PIMS_EU_BEERSE_AF_PE", database="DeltaV-Events"
) as afdatabase, PIconnect.PIServer(server="ITSBEBEPIHISCOL") as server:

    eventlist = afdatabase.find_events(
        query="*HR102164G4-*",
        starttime="*-100d",
        endtime="*-10d",
        search_full_hierarchy=False,
    )
    eventhierarchy = eventlist.get_event_hierarchy(depth=3)

    # get your condensed hierarchy
    condensed = eventhierarchy.ehy.condense()

    # multiple tags at te same time can be defined now, but needs to be as csv string
    condensed["Tag"] = "SINUSOID, SINUSOIDU"

    # pass arguments as a dict
    x = dict(
        tag_list=["Tag"],
        summary_types=2 | 4,
        dataserver=server,
        col=True,
    )

    # initialize the threading function by providing source, appropriate class method, args dict and chunk_size
    res = PIconnect.thread.threading(
        condensed,
        PIconnect.PIAF.CondensedEventHierarchy.summary_extract,
        x,
        100,
    )

    # get your condensed hierarchy
    condensed = eventhierarchy.ehy.condense()

    # specify multiple tags via column, by using single CSV string
    condensed["Tag"] = "SINUSOID, SINUSOIDU"

    # set tag queries as desired for events by using logic/mapping/..
    condensed["Tag"] = "SINUSOID"
    condensed["Tag"].iloc[-4:-2] = "SINUSOID, 100_091_R024_ST01"
    condensed["Tag"].iloc[-2:] = "SINUSOIDU"

    # pass Tag column name and set col argument to True
    result = condensed.ecd.summary_extract(
        tag_list=["Tag"], summary_types=2 | 4, dataserver=server, col=True
    )
