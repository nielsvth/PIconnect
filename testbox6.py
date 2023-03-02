import PIconnect
import pandas as pd
import numpy as np
from datetime import datetime
from pytz import timezone
from PIconnect.AFSDK import AF
from PIconnect.PIAF import Event

from threading import Thread
import PIconnect
import pandas as pd
from typing import Union
import types

# Initiate connection to PI data server & PI AF database
with PIconnect.PIAFDatabase(
    server="PIMS_EU_BEERSE_AF_PE", database="DeltaV-Events"
) as afdatabase, PIconnect.PIServer(server="ITSBEBEPIHISCOL") as server:

    taglist = server.find_tags(["*091*R021*TT*"])

    # pass arguments as a dict
    x = dict(starttime="*-20d", endtime="*-10d", interval="1h")

    # initialize the threading function by providing source, appropriate class method, args dict and chunk_size

    res = PIconnect.thread.threading(
        source=taglist,
        method=PIconnect.PI.TagList.interpolated_values,
        args=x,
        chunk_size=10,
    )

    tag = server.find_tags(["*091*R021*TT04A*"])[0]

    z = tag.interpolated_values(
        starttime="*-20d", endtime="*-10d", interval="1h"
    )
