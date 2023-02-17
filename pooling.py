import PIconnect
import datetime

# https://www.machinelearningplus.com/python/parallel-processing-python/
import multiprocessing as mp

# https://stackoverflow.com/questions/8804830/python-multiprocessing-picklingerror-cant-pickle-type-function
# works because ThreadPool shares memory with the main thread, rather than creating a new process
# this means that pickling is not required.
from multiprocessing.pool import ThreadPool as Pool
import dill

# specify number of historical processes
# print("Number of processors: ", mp.cpu_count())

# Set up timezone info
PIconnect.PIConfig.DEFAULT_TIMEZONE = "Europe/Brussels"

with PIconnect.PIAFDatabase(
    server="PIMS_EU_BEERSE_AF_PE", database="DeltaV-Events"
) as afdatabase, PIconnect.PIServer(server="ITSBEBEPIHISCOL") as server:

    eventlist = afdatabase.find_events(
        query="*HR102164G4-*",
        starttime="*-500d",
        endtime="*-10d",
        search_full_hierarchy=False,
    )
    eventhierarchy = eventlist.get_event_hierarchy(depth=3)
    # condense
    condensed = eventhierarchy.ehy.condense()

    row = condensed["Event [3]"].dropna()

    taglist = server.find_tags("SINUSOID")

    a = datetime.datetime.now()

    queue = []
    taglist = taglist
    interval = "1h"
    for event in row:
        x = event.interpolated_values(
            taglist,
            interval=interval,
        ).to_records(index=True)
        queue.append(x)

    print(queue)

    b = datetime.datetime.now()

    print(b - a)
