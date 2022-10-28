import PIconnect

# Set up timezone info
PIconnect.PIConfig.DEFAULT_TIMEZONE = "Europe/Brussels"

with PIconnect.PIAFDatabase(
    server="PIMS_EU_BEERSE_AF_PE", database="DeltaV-Events"
) as afdatabase, PIconnect.PIServer() as server:

    eventlist = afdatabase.find_events(
        query="*HR102164G4-*", starttime="*-70d", endtime="*-10d"
    )
    print(eventlist)
    eventhierarchy = eventlist.get_event_hierarchy(depth=2)
