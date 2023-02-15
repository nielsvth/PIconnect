import PIconnect
import pandas as pd
import numpy as np
from datetime import datetime
from PIconnect.AFSDK import AF
from PIconnect.PI import (
    convert_to_TagList,
)
from PIconnect.time import timestamp_to_index, add_timezone

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

    tag_list = ["SINUSOID"]
    interval = "1h"
    filter_expression = ""
    dataserver = server
    paging_config = AF.PI.PIPagingConfiguration(
        AF.PI.PIPageType.EventCount, 1000
    )

    taglist = convert_to_TagList(tag_list, dataserver)

    # select events on bottem level of condensed hierarchy
    col_start = [
        col_name
        for col_name in condensed.columns
        if col_name.startswith("Starttime")
    ][-1]
    # sort chronologically by starttime
    condensed.sort_values(by=[col_start], ascending=True, inplace=True)

    print("building continuous extract table from condensed hierachy...")
    # select events on bottem level of condensed hierarchy
    col_event = [
        col_name
        for col_name in condensed.columns
        if col_name.startswith("Event")
    ][-1]

    df_base = condensed[[col_event]].copy()
    df_base.columns = ["Event"]
    # add procedure names
    df_base["Procedure"] = df_base["Event"].apply(lambda x: x.top_event)
    df_base = df_base[["Procedure", "Event"]]
    df_base.reset_index(drop=True, inplace=True)

    # extract interpolated data for continuous events, per procedure
    df_cont = pd.DataFrame()
    for proc, df_proc in df_base.groupby("Procedure"):
        starttime = df_proc["Event"].iloc[0].starttime
        endtime = df_proc["Event"].iloc[-1].endtime
        values = list(
            taglist.interpolated_values(
                starttime,
                endtime,
                interval,
                filter_expression,
                paging_config=paging_config,
            ).to_records(index=True)
        )
        df_cont = pd.concat(
            [
                df_cont,
                pd.DataFrame([[proc, values]], columns=["Procedure", "Time"]),
            ],
            ignore_index=True,
        )

    df_cont = df_cont.explode("Time")  # explode list to rows
    df_cont["Time"] = df_cont["Time"].apply(
        lambda x: [el for el in x]
    )  # numpy record to list
    # pd.DataFrame(df['b'].tolist(), index=df.index) instead of
    # apply(pd.Series) could be faster
    df_cont[["Time"] + [tag.name for tag in taglist]] = df_cont["Time"].apply(
        pd.Series
    )  # explode list to columns

    df_cont["Time"] = df_cont["Time"].apply(lambda x: add_timezone(x))

    # add Event info back
    df_cont["Event"] = np.nan
    for event in df_base["Event"]:
        df_cont["Event"].loc[
            (df_cont["Time"] >= event.starttime)
            & (df_cont["Time"] <= event.endtime)
        ] = event

    # format
    df_cont = df_cont[
        ["Procedure", "Event", "Time"] + [tag.name for tag in taglist]
    ]
    df_cont.sort_values(by=["Time"], ascending=True, inplace=True)
    df_cont.reset_index(drop=True, inplace=True)
