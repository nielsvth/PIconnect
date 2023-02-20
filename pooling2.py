import PIconnect
import datetime

# Set up timezone info
PIconnect.PIConfig.DEFAULT_TIMEZONE = "Europe/Brussels"

with PIconnect.PIAFDatabase(
    server="PIMS_EU_BEERSE_AF_PE", database="DeltaV-Events"
) as afdatabase, PIconnect.PIServer(server="ITSBEBEPIHISCOL") as server:

    eventlist = afdatabase.find_events(
        query="*HR102164G4-*",
        starttime="*-500d",
        endtime="*-10d",
        search_full_hierarchy=False,
    )
    eventhierarchy = eventlist.get_event_hierarchy(depth=3)
    # condense
    condensed = eventhierarchy.ehy.condense()
    taglist = server.find_tags("SINUSOID")

    a = datetime.datetime.now()

    taglist = taglist
    condensed["Tag"] = "SINUSOID, SINUSOIDU"

    queue = condensed.ecd.summary_extract(
        tag_list=["Tag"],
        summary_types=2 | 4 | 8,
        dataserver=server,
        col=True,
    )

    b = datetime.datetime.now()

    print(b - a)
