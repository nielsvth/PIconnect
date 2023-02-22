import PIconnect
import pandas as pd
import numpy as np
from datetime import datetime
from pytz import timezone
from PIconnect.AFSDK import AF
from PIconnect.PI import (
    convert_to_TagList,
)
from PIconnect.time import timestamp_to_index, add_timezone

from PIconnect.PIConsts import (
    SummaryType,
    CalculationBasis,
    TimestampCalculation,
    ExpressionSampleType,
)

# Set up timezone info
PIconnect.PIConfig.DEFAULT_TIMEZONE = "Europe/Brussels"

with PIconnect.PIAFDatabase(
    server="PIMS_EU_BEERSE_AF_PE", database="DeltaV-Events"
) as afdatabase, PIconnect.PIServer(server="ITSBEBEPIHISCOL") as server:

    eventlist = afdatabase.find_events(
        query="*HR102164G4-*",
        starttime="*-50d",
        endtime="*-20d",
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

    eventhierarchy["Duration"] = eventhierarchy["Event"].apply(
        lambda x: x.duration
    )
    eventhierarchy = eventhierarchy[
        eventhierarchy["Duration"] < pd.Timedelta("60d")
    ]
    eventhierarchy.drop(columns=["Duration"], inplace=True)

    # select events on bottom level of condensed hierarchy
    col_event = [
        col_name
        for col_name in condensed.columns
        if col_name.startswith("Event")
    ][-1]
    condensed["Duration"] = condensed[col_event].apply(lambda x: x.duration)
    condensed = condensed[condensed["Duration"] < pd.Timedelta("60d")]
    condensed.drop(columns=["Duration"], inplace=True)

    eventhierarchy[
        "Expres"
    ] = r"('\\ITSBEBEPIHISCOL\100_091_R015_TT08')-('\\ITSBEBEPIHISCOL\100_091_R015_TT09')"

    # do summary construction
    interval = "100h"
    summary_types = 4 | 8
    expression = "Expres"
    calculation_basis = CalculationBasis.TIME_WEIGHTED
    time_type = TimestampCalculation.AUTO
    AFfilter_evaluation = ExpressionSampleType.EXPRESSION_RECORDED_VALUES
    filter_interval = None

    res = eventhierarchy.ehy.calc_summary_extract(
        interval=interval,
        summary_types=summary_types,
        expression=expression,
        col=True,
    )

    x = dict(
        interval="100h",
        summary_types=4 | 8,
        expression="Expres",
        calculation_basis=CalculationBasis.TIME_WEIGHTED,
        time_type=TimestampCalculation.AUTO,
        AFfilter_evaluation=ExpressionSampleType.EXPRESSION_RECORDED_VALUES,
        filter_interval=None,
        col=True,
    )

    res = PIconnect.thread.threading(
        eventhierarchy,
        PIconnect.PIAF.EventHierarchy.calc_summary_extract,
        x,
        100,
    )
