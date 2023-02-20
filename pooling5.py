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
        starttime="*-50d",
        endtime="*-10d",
        search_full_hierarchy=False,
    )
    eventhierarchy = eventlist.get_event_hierarchy(depth=3)
    # condense
    condensed = eventhierarchy.ehy.condense()

    x = dict(
        tag_list=["SINUSOID"],
        summary_types=2 | 4,
        dataserver=server,
        col=False,
    )

    a = datetime.datetime.now()
    PIconnect.thread.threading(
        condensed,
        PIconnect.PIAF.CondensedEventHierarchy.summary_extract,
        x,
        100,
    )
    b = datetime.datetime.now()
    print(b - a)
