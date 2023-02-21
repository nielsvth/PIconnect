import PIconnect

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

    # pick one event
    event = eventlist[1]
    eventhierarchy = event.get_event_hierarchy(depth=1)
    eventhierarchy = eventhierarchy.ehy.add_ref_elements(
        template_name="UnitProcedure"
    )

    print(event.name)
    print(event.starttime)
    print(event.endtime)

    result = PIconnect.calc.calc_summary(
        starttime=event.starttime,
        endtime=event.endtime,
        interval="100h",
        summary_types=4 | 8,
        expression=r"('\\ITSBEBEPIHISCOL\100_091_R015_TT08')-('\\ITSBEBEPIHISCOL\100_091_R015_TT09')",
    )
