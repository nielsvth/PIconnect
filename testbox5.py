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

    eventhierarchy["Tag"] = "SINUSOID, SINUSOIDU"

    # create condensed dataframe
    # condensed = eventhierarchy.ehy.condense()
    # condensed['Tag'] = "SINUSOID, SINUSOIDU"

    # do summary construction
    tag_list = ["Tag"]
    interval = "1h"
    filter_expression = ""
    dataserver = server
    col = True
    paging_config = AF.PI.PIPagingConfiguration(
        AF.PI.PIPageType.EventCount, 1000
    )

    # summary extract - specify tag from list
    summary_values = eventhierarchy.ehy.summary_extract(
        tag_list=["Tag"],
        summary_types=4 | 8 | 32,
        dataserver=server,
        col=True,
    )
