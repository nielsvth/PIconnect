import PIconnect

# Set up timezone info
# PIconnect.PIConfig.DEFAULT_TIMEZONE = "Europe/Brussels"

with PIconnect.PIAFDatabase(
    server="PIMS_EU_BEERSE_AF_PE", database="Geel_CRB_Batch_Events"
) as afdatabase, PIconnect.PIServer() as server:

    eventlist = afdatabase.find_events(
        query="*RT001484G4E25*",
        starttime="*-2000d",
        endtime="*-10d",
        search_full_hierarchy=False,
    )
    print(eventlist)
    eventhierarchy = eventlist.get_event_hierarchy(depth=3)

# [] give error?

viewable = PIconnect.PI.view(eventhierarchy)
