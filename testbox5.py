import PIconnect

# Initiate connection to PI data server & PI AF database
with PIconnect.PIAFDatabase(
    server="PIMS_EU_BEERSE_AF_PE", database="DeltaV-Events"
) as afdatabase, PIconnect.PIServer(server="ITSBEBEPIHISCOL") as server:

    # Select Procedures
    eventlist = afdatabase.find_events(
        query="*HR102164G4-*",
        starttime="*-100d",
        endtime="*-10d",
        search_full_hierarchy=False,
    )

    # get eventhierarchy
    eventhierarchy = eventlist.get_event_hierarchy(depth=3)

    # add stepnrs
    eventhierarchy = eventhierarchy.ehy.add_attributes(
        attribute_names_list=["B_PH_INFO"], template_name="Phase"
    )

    # add linked equipment
    eventhierarchy = eventhierarchy.ehy.add_ref_elements(
        template_name="UnitProcedure"
    )

    # condense
    condensed = eventhierarchy.ehy.condense()

    # just reactors
    condensed = condensed[
        condensed["Referenced_el [UnitProcedure](0)"].str.contains(
            "R\d{3}", regex=True
        )
    ]

    # populate expression column
    # condensed['Expression'] = condensed['Referenced_el [UnitProcedure](0)'].apply(lambda x: fr"('\\ITSBEBEPIHISCOL\100_{x}_TT08')-('\\ITSBEBEPIHISCOL\100_{x}_TT09')")

    condensed["Expression"] = condensed[
        "Referenced_el [UnitProcedure](0)"
    ].apply(lambda x: rf"'\\ITSBEBEPIHISCOL\100_{x}_ST01'*1440")

    # https://docs.aveva.com/bundle/pi-server-af-analytics/page/1020877.html
    # https://www.hallam-ics.com/blog/creating-totalizers-in-pi-server-historian#:~:text=The%20PI%20Totalizer%20subsystem%20is,stored%20to%20the%20data%20archive.

    # condensed["Expression"] = condensed[
    #    "Referenced_el [UnitProcedure](0)"
    # ].apply(lambda x: rf"IF ('\\ITSBEBEPIHISCOL\100_{x}_ST01' < 30) THEN ('\\ITSBEBEPIHISCOL\100_{x}_ST01'*30*1440) ELSE IF ('\\ITSBEBEPIHISCOL\100_{x}_ST01' >= 30) AND ('\\ITSBEBEPIHISCOL\100_{x}_ST01' <= 90) THEN ('\\ITSBEBEPIHISCOL\100_{x}_ST01'*45*1440) ELSE IF ('\\ITSBEBEPIHISCOL\100_{x}_ST01' > 90) THEN ('\\ITSBEBEPIHISCOL\100_{x}_ST01'*75*1440) ELSE 0")

    # calculation of summary measures of interval for calculated values
    calc_summary_values = condensed.ecd.calc_summary_extract(
        interval="event",
        summary_types=1,
        expression="Expression",
        col=True,
    )

    # calc_summary_values['Value'] returns (average_value/minute for 1 day * #days)
    # So divide by #days to get value average per minute over event duration
    calc_summary_values["Duration(day)"] = calc_summary_values["Event"].apply(
        lambda x: x.duration.total_seconds() / 86400
    )
    calc_summary_values["Avg_Value/min(day)"] = (
        calc_summary_values["Value"] / calc_summary_values["Duration(day)"]
    )

    # order by Totalized Value
    calc_summary_values = calc_summary_values.sort_values(
        by="Value", ascending=False
    ).reset_index(drop=True)

    # TEST using TagTot
    starttime = "1-10-2022 14:00"
    endtime = "1-10-2022 14:00"
    interval = "1h"
    expression = r"TagTot('\\ITSBEBEPIHISCOL\100_091_R002_TT04A', '30-Dec-2022 08:29:37', '01-Jan-2023 17:28:34')"

    calc = PIconnect.calc.calc_interpolated(
        starttime,
        endtime,
        interval,
        expression,
    )
