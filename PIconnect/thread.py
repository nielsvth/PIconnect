from threading import Thread
import PIconnect
import pandas as pd
from typing import Union
import types

# https://realpython.com/intro-to-python-threading/


def chunk(
    obj: Union[pd.DataFrame, list, PIconnect.PI.TagList], chunk_size: int
):
    """ "Split list/ dataframe object in chunks

    Args:
        df (pd.DataFrame, list, PIconnect.PI.TagList): input for chuncking
        chunk_size (int): size of each chunk

    Returns:
        List: list of chunked elements
    """
    lst_chunk = []
    for i in range(1, (len(obj) // chunk_size) + 2):
        if i == 0:
            x = obj[0 : (i * chunk_size)]
        else:
            x = obj[(i - 1) * chunk_size : (i * chunk_size)]
        lst_chunk.append(x)
    return lst_chunk


def source_extract(
    chunk: Union[pd.DataFrame, list, PIconnect.PI.TagList],
    method,
    typ: str,
    args: dict,
    queue: list,
):
    """Single thread function

    Args:
        chunk (pd.DataFrame, list, PIconnect.PI.TagList): chunked object
        method (function): PIConnect method
        typ (str): specifies class type - 'ecd', 'ehy' or 'tag'
        args (dict): dictionary with method arguments
        queue (list): threading queue

    Returns:
        List: threading queue
    """
    if typ == "ecd":
        result = types.MethodType(
            method, PIconnect.PIAF.CondensedEventHierarchy(chunk)
        )(**args)
    elif typ == "ehy":
        result = types.MethodType(
            method, PIconnect.PIAF.EventHierarchy(chunk)
        )(**args)
    queue.append(result)
    return queue


def threading(
    source: Union[pd.DataFrame, list, PIconnect.PI.TagList],
    method,
    args: dict,
    chunk_size: int = 1000,
):
    """Threading function for increased performance by splitting source data in multiple chunks
    and executing queries for chunks in parallal.

    Args:
        source (pd.DataFrame, list, PIconnect.PI.TagList): input for threading, available for
        EventHierarchy, CondensedEventHierarchy and TagList classes
        method (function): PIConnect method,
        args (dict): dictionary with method arguments
        chunk_size(int): size of each chunk, default is 1000

    Returns pd.DataFrame
    """

    if "CondensedEventHierarchy" in str(method.__qualname__):
        # split df in smaller chunks for I/O bound threading
        lst_chunk = chunk(source, chunk_size)
        typ = "ecd"
        if not (
            method == PIconnect.PIAF.CondensedEventHierarchy.summary_extract
        ) and not (
            method
            == PIconnect.PIAF.CondensedEventHierarchy.interpol_discrete_extract
        ) and not (
            method == PIconnect.PIAF.CondensedEventHierarchy.calc_summary_extract
        ):
            raise AttributeError(
                "Threading only works for summary_extract, calc_summary_extract and interpol_discrete_extract methods"
            )

    elif "EventHierarchy" in str(method.__qualname__):
        # split df in smaller chunks for I/O bound threading
        lst_chunk = chunk(source, chunk_size)
        typ = "ehy"
        if (
            not (method == PIconnect.PIAF.EventHierarchy.summary_extract)
            and not (
                method
                == PIconnect.PIAF.EventHierarchy.interpol_discrete_extract
            )
            and not (
                method == PIconnect.PIAF.EventHierarchy.calc_summary_extract
            )
        ):
            raise AttributeError(
                "Threading only works for summary_extract, calc_summary_extract and interpol_discrete_extract methods"
            )

    elif "TagList" in str(method.__qualname__):
        # split tag list in smaller chunks for I/O bound threading
        typ = "tag"

    else:
        raise AttributeError(
            f"The {method} method currently has no threading functionality available"
        )

    thread_list = []
    queue = []

    if (typ == "ecd") or (typ == "ehy") or (typ == "tag"):
        for i in range(len(lst_chunk)):
            t = Thread(
                target=source_extract,
                args=(lst_chunk[i], method, typ, args, queue),
            )
            thread_list.append(t)

    # start threads
    for thread in thread_list:
        thread.start()
    # close threads
    for thread in thread_list:
        thread.join()

    return pd.concat(queue).reset_index(drop=True)
