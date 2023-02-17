import PIconnect
import datetime
from PIconnect.AFSDK import AF

# Set up timezone info
PIconnect.PIConfig.DEFAULT_TIMEZONE = "Europe/Brussels"

paging_config = AF.PI.PIPagingConfiguration(AF.PI.PIPageType.EventCount, 10)

with PIconnect.PIAFDatabase(
    server="PIMS_EU_BEERSE_AF_PE", database="DeltaV-Events"
) as afdatabase, PIconnect.PIServer(server="ITSBEBEPIHISCOL") as server:

    a = datetime.datetime.now()
    # get recent events for RT003474 (selection kept limited to one reactor for now)
    eventlist = afdatabase.find_events(
        query="*HR102164G4-*A19JB2451*",
        starttime="*-2000d",
        endtime="*",
        search_full_hierarchy=False,
    )

    eventhierarchy = eventlist.get_event_hierarchy(depth=3)

    # get attributes
    eventhierarchy = eventhierarchy.ehy.add_attributes(
        attribute_names_list=["B_PH_INFO"], template_name="Phase"
    )

    # get ref elements
    eventhierarchy = eventhierarchy.ehy.add_ref_elements(
        template_name="UnitProcedure"
    )

    # condense
    condensed = eventhierarchy.ehy.condense()

    for tag in ["100_091_R021_ST01"]:
        # Populate tag column
        condensed["Tag"] = tag
        # extract summaries
        summary_values = condensed.ecd.summary_extract(
            tag_list=["Tag"],
            summary_types=2 | 4 | 8,
            dataserver=server,
            col=True,
            paging_config=paging_config,
        )

    b = datetime.datetime.now()

    print(b - a)
