import PIconnect
import datetime

# Set up timezone info
PIconnect.PIConfig.DEFAULT_TIMEZONE = "Europe/Brussels"

with PIconnect.PIAFDatabase(
    server="PIMS_EU_BEERSE_AF_PE", database="DeltaV-Events"
) as afdatabase, PIconnect.PIServer(server="ITSBEBEPIHISCOL") as server:

    a = datetime.datetime.now()
    # get recent events for RT003474 (selection kept limited to one reactor for now)
    eventlist = afdatabase.find_events(
        query="*RT002675G3-*",
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

    # remove nan values
    condensed.dropna(subset=["Referenced_el [UnitProcedure](0)"], inplace=True)

    ##### get reactors on UP level (?)
    # Restrict to reactors
    condensed = condensed[
        condensed["Referenced_el [UnitProcedure](0)"].str.contains(
            r"090_R\d{3}", regex=True
        )
    ]

    print(condensed["Referenced_el [UnitProcedure](0)"].unique())

    for tag in ["ST01"]:
        # Populate tag column
        condensed["Tag"] = (
            "100090"
            + condensed["Referenced_el [UnitProcedure](0)"].str.extract(
                pat=r"(R\d{3})"
            )
            + tag
        )

        # extract summaries
        summary_values = condensed.ecd.summary_extract(
            tag_list=["Tag"],
            summary_types=2 | 4 | 8,
            dataserver=server,
            col=True,
        )

    b = datetime.datetime.now()

    print(b - a)
