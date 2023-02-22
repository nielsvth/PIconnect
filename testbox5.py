import PIconnect
import pandas as pd
import numpy as np
from datetime import datetime
from pytz import timezone

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

    condensed = eventhierarchy.ehy.condense()

    # calculation of summary measures of interval for calculated values
    calc_summary_values = condensed.ecd.calc_summary_extract(
        interval="100h",
        summary_types=4 | 8,
        expression=r"('\\ITSBEBEPIHISCOL\SINUSOID')-('\\ITSBEBEPIHISCOL\SINUSOIDU')",
        col=False,
    )
    assert len(calc_summary_values) == (len(condensed) * 2)
