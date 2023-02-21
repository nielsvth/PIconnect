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

# pick one event
event = eventlist[1]
eventhierarchy = event.get_event_hierarchy(depth=1)
eventhierarchy = eventhierarchy.ehy.add_ref_elements(
    template_name="UnitProcedure"
)

print(event.name)
print(event.starttime)
print(event.endtime)

# calculation on interpolated values
# returns substracted values of TT08 and TT09 for R015
calc = PIconnect.calc.calc_interpolated(
    event.starttime,
    event.endtime,
    "1h",
    r"('\\ITSBEBEPIHISCOL\100_091_R015_TT08')-('\\ITSBEBEPIHISCOL\100_091_R015_TT09')",
)
