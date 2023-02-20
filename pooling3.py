from threading import Thread
import PIconnect
import datetime

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

    lst = condensed["Event [3]"].dropna()

    from itertools import islice

    def chunk(arr_range, arr_size):
        arr_range = iter(arr_range)
        return iter(lambda: tuple(islice(arr_range, arr_size)), ())

    lst_chunk = list(chunk(lst, 1000))

    taglist = server.find_tags("SINUSOID")
    # try apply
    def extract(row, queue, taglist=taglist):
        for event in row:
            x = event.summary(
                tag_list=taglist,
                summary_types=2 | 4 | 8,
                dataserver=server,
            ).to_records(index=False)
            queue.append(x)
        return queue

    # https://www.linkedin.com/pulse/speed-up-processing-millions-records-database-python-aditya-yogi/
    a = datetime.datetime.now()

    thread_list = []
    queue = []
    for i in range(len(lst_chunk)):
        t = Thread(target=extract, args=(lst_chunk[i], queue))
        thread_list.append(t)

    for thread in thread_list:
        thread.start()

    for thread in thread_list:
        thread.join()

    print(queue)

    b = datetime.datetime.now()

    print(b - a)
