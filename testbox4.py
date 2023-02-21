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
    server="PIMS_EU_BEERSE_AF_PE", database="DeltaV-Events"
) as afdatabase, PIconnect.PIServer(server="ITSBEBEPIHISCOL") as server:

    eventlist = afdatabase.find_events(
        query="*HR102164G4-*",
        starttime="*-100d",
        endtime="*-10d",
        search_full_hierarchy=False,
    )
    eventhierarchy = eventlist.get_event_hierarchy(depth=3)
    # condense
    condensed = eventhierarchy.ehy.condense()

    # drop NAN
    condensed = condensed[
        condensed[
            condensed.columns[
                condensed.columns.str.contains(r"Event\s\[.*]", regex=True)
            ]
        ]
        .notnull()
        .all(1)
    ]

    eventhierarchy["Tag"] = "SINUSOID"
    eventhierarchy["Tag"].iloc[-4:-2] = "SINUSOID, 100_091_R024_ST01"
    eventhierarchy["Tag"].iloc[-2:] = "SINUSOIDU"

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

    print("Building summary table from EventHierachy...")
    df = eventhierarchy.copy()

    # performance checks
    maxi = max(df["Event"].apply(lambda x: x.duration))
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

    if not col:
        taglist = convert_to_TagList(tag_list, dataserver)
        # extract summary data for discrete events
        df["Time"] = df["Event"].apply(
            lambda x: list(
                x.summary(
                    taglist,
                    summary_types,
                    dataserver,
                    calculation_basis,
                    time_type,
                    paging_config=paging_config,
                ).to_records(index=False)
            )
        )

    if col:
        if len(tag_list) > 1:
            raise AttributeError(
                f"You can only specify a single tag column at a time"
            )
        if tag_list[0] in df.columns:
            event = df.columns.get_loc("Event")

            df.reset_index(drop=True, inplace=True)
            # just single request for each unique target
            for tg in df[tag_list[0]].unique():
                tl = convert_to_TagList(
                    tg.replace(" ", "").split(","), dataserver
                )
                # https://stackoverflow.com/questions/39717809/insert-list-into-cells-which-meet-column-conditions
                df.loc[df[tag_list[0]] == tg, "Tags"] = pd.Series(
                    [tl] * df.shape[0]
                )

            # extract summary data for discrete events
            df["Time"] = df.apply(
                lambda row: list(
                    row[event]
                    .summary(
                        row["Tags"],
                        summary_types,
                        calculation_basis,
                        time_type,
                        paging_config=paging_config,
                    )
                    .to_records(index=False)
                ),
                axis=1,
            )
        else:
            raise AttributeError(
                f"The column option was set to True, but {tag_list[0]} "
                + "is not a valid column"
            )

    df = df.explode("Time")  # explode list to rows
    df["Time"] = df["Time"].apply(
        lambda x: [el for el in x] if not pd.isnull(x) else np.nan
    )  # numpy record to list
    df[["Tag", "Summary", "Value", "Time"]] = df["Time"].apply(
        pd.Series
    )  # explode list to columns
    df.reset_index(drop=True, inplace=True)
