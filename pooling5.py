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
    condensed = eventhierarchy.ehy.condense()

    # multiple tags at te same time can be defined now, but needs to be as csv string
    condensed["Tag"] = "SINUSOID, SINUSOIDU"

    x = dict(
        tag_list=["Tag"],
        summary_types=2 | 4,
        dataserver=server,
        col=True,
    )

    a = datetime.datetime.now()

    res = PIconnect.thread.threading(
        condensed,
        PIconnect.PIAF.CondensedEventHierarchy.summary_extract,
        x,
        100,
    )

    b = datetime.datetime.now()
    print(b - a)
