import PIconnect
import pandas as pd
import numpy as np
from datetime import datetime
from pytz import timezone
from PIconnect.AFSDK import AF
from PIconnect.PIAF import Event

# Initiate connection to PI data server & PI AF database of interest by
# defining their name
with PIconnect.PIAFDatabase(
    server="ITSBEBEWSP06182 DEV", database="NuGreen"
) as afdatabase, PIconnect.PIServer(
    server=list(PIconnect.PIServer.servers.keys())[1]
) as server:

    start = timezone("Europe/Brussels").localize(
        datetime(day=1, month=10, year=2022)
    )
    end = timezone("Europe/Brussels").localize(
        datetime(day=4, month=10, year=2022)
    )

    eventlist = afdatabase.find_events(query="*", starttime=start, endtime=end)
    depth = "2"

    afcontainer = AF.AFNamedCollectionList[
        AF.EventFrame.AFEventFrame
    ]()  # empty container
    for event in eventlist:
        try:
            afcontainer.Add(event.af_eventframe)
        except:
            raise ("Failed to process event {}".format(event))

    df_events = pd.DataFrame(
        columns=[
            "Event",
            "Path",
            "Name",
            "Level",
            "Template",
            "Starttime",
            "Endtime",
        ]
    )

    if len(afcontainer) > 0:
        df_procedures = pd.DataFrame(
            [(y, y.GetPath()) for y in afcontainer],
            columns=["Event", "Path"],
        )

        print(
            "Fetching hierarchy data for {} Event(s)...".format(
                len(afcontainer)
            )
        )
        # https://docs.osisoft.com/bundle/af-sdk/page/html/M_OSIsoft_AF_EventFrame_AFEventFrame_LoadEventFramesToDepth.htm
        event_depth = AF.EventFrame.AFEventFrame.LoadEventFramesToDepth(
            afcontainer, False, depth, 1000000
        )

        if len(event_depth) > 0:
            df_events = pd.DataFrame(
                [(y, y.GetPath()) for y in event_depth],
                columns=["Event", "Path"],
            )

        # concatenate procedures and child event frames
        df_events = pd.concat([df_procedures, df_events], ignore_index=True)

        df_events["Event"] = df_events["Event"].apply(lambda x: Event(x))
        df_events["Name"] = df_events["Event"].apply(
            lambda x: x.name if x else np.nan
        )
        df_events["Template"] = df_events["Event"].apply(
            lambda x: x.template_name if x.af_template else np.nan
        )
        df_events["Level"] = (
            df_events["Path"].str.count(r"\\").apply(lambda x: x - 4)
        )
        df_events["Starttime"] = df_events["Event"].apply(
            lambda x: x.starttime if x else np.nan
        )
        df_events["Endtime"] = df_events["Event"].apply(
            lambda x: x.endtime if x else np.nan
        )

        df_events.drop_duplicates(inplace=True)

        df_events.drop_duplicates()

        df_events.drop_duplicates("Path")
