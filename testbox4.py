import PIconnect
import pandas as pd
import numpy as np
from datetime import datetime
from PIconnect.AFSDK import AF
from PIconnect.PI import (
    convert_to_TagList,
)
from PIconnect.time import timestamp_to_index, add_timezone

from PIconnect.PIConsts import TimestampCalculation, CalculationBasis

# Set up timezone info
PIconnect.PIConfig.DEFAULT_TIMEZONE = "Europe/Brussels"

with PIconnect.PIAFDatabase(
    server="ITSBEBEWSP06182 DEV", database="NuGreen"
) as afdatabase, PIconnect.PIServer() as server:

    starttime = datetime(day=1, month=10, year=2022)
    endtime = datetime(day=4, month=10, year=2022)

    eventlist = afdatabase.find_events(
        query="*", starttime=starttime, endtime=endtime
    )
    eventhierarchy = eventlist.get_event_hierarchy(depth=2)

    # add attributes
    eventhierarchy = eventhierarchy.ehy.add_attributes(
        attribute_names_list=["Equipment", "Manufacturer"],
        template_name="Unit_template",
    )

    # add referenced elements
    eventhierarchy = eventhierarchy.ehy.add_ref_elements(
        template_name="Operation_template"
    )

    # create condensed dataframe
    condensed = eventhierarchy.ehy.condense()

    condensed["Tag"] = "SINUSOID, SINUSOIDU"

    # do summary construction
    tag_list = ["Tag"]
    summary_types = 2 | 4 | 8
    filter_expression = ""
    dataserver = server
    time_type = TimestampCalculation.AUTO
    calculation_basis = CalculationBasis.TIME_WEIGHTED
    col = True
    paging_config = AF.PI.PIPagingConfiguration(
        AF.PI.PIPageType.EventCount, 1000
    )

    print("building summary table from condensed hierachy...")
    df = condensed.copy()

    # select events on bottom level of condensed hierarchy
    col_event = [
        col_name for col_name in df.columns if col_name.startswith("Event")
    ][-1]

    # performance checks
    maxi = max(df[col_event].apply(lambda x: x.duration))
    if maxi > pd.Timedelta("60 days"):
        print(
            f"Large Event(s) with duration up to {maxi} detected, "
            + "Note that this might take some time..."
        )
    if len(df) > 50:
        print(
            f"Summaries will be calculated for {len(df)} Events, Note"
            + " that this might take some time..."
        )

    # based on list of tags
    if not col:
        df = df[[col_event]].copy()
        df.columns = ["Event"]

        # add procedure names
        df["Procedure"] = df["Event"].apply(lambda x: x.top_event)
        df = df[["Procedure", "Event"]]
        df.reset_index(drop=True, inplace=True)

        taglist = convert_to_TagList(tag_list, dataserver)
        # extract summary data for discrete events
        df["Time"] = df["Event"].apply(
            lambda x: list(
                x.summary(
                    taglist,
                    summary_types,
                    calculation_basis,
                    time_type,
                    paging_config=paging_config,
                ).to_records(index=False)
            )
        )

    # based on column with tags
    if col:
        if len(tag_list) > 1:
            raise AttributeError(
                f"You can only specify a single tag column at a time"
            )
        if tag_list[0] in df.columns:
            df = df[[col_event, tag_list[0]]].copy()
            df.columns = ["Event", "Tags"]
        else:
            raise AttributeError(
                f"The column option was set to True, but {tag_list} is "
                + "not a valid column"
            )

        # add procedure names
        df["Procedure"] = df["Event"].apply(lambda x: x.top_event)
        df = df[["Procedure", "Event", "Tags"]]
        df["Tags"] = df["Tags"].apply(lambda x: x.replace(" ", "").split(","))
        df.reset_index(drop=True, inplace=True)

        event = df.columns.get_loc("Event")
        tags = df.columns.get_loc("Tags")

        a = datetime.now()

        # extract summary data for discrete events
        df["Time"] = df.apply(
            lambda row: list(
                row[event]
                .summary(
                    row[tags],
                    summary_types,
                    dataserver,
                    calculation_basis,
                    time_type,
                    paging_config=paging_config,
                )
                .to_records(index=False)
            ),
            axis=1,
        )

    df = df.explode("Time")  # explode list to rows
    df["Time"] = df["Time"].apply(
        lambda x: [el for el in x]
    )  # numpy record to list
    df[["Tag", "Summary", "Value", "Time"]] = df["Time"].apply(
        pd.Series
    )  # explode list to columns
    df.reset_index(drop=True, inplace=True)

    b = datetime.now()

    print(b - a)
