""" Calculations
    Core functionality for doing calculations using the AF.Data.AFCalculation class
"""
from typing import Union
import datetime

from PIconnect.AFSDK import AF
from PIconnect.time import (
    timestamp_to_index,
    to_af_time_range,
)

import pandas as pd


def calc_recorded(
    starttime: Union[str, datetime.datetime],
    endtime: Union[str, datetime.datetime],
    expression: str = "",
) -> pd.DataFrame:
    """Returns dataframe that contains the result of evaluating the passed expression at each point in time
    over the passed time range where a recorded value exists for a member of the expression.

    Expression arguments need to be entered as raw strings: r'expression'"""
    afrange = to_af_time_range(starttime, endtime)
    result = AF.Data.AFCalculation.CalculateAtRecordedValues(
        0, expression, afrange
    )

    if result:
        # process query results
        data = [list(result)]
        df = pd.DataFrame(data).T
        df.columns = ["calculation"]
        # https://docs.osisoft.com/bundle/af-sdk/page/html/T_OSIsoft_AF_Asset_AFValue.htm # noqa
        df.index = df[df.columns[0]].apply(
            lambda x: timestamp_to_index(x.Timestamp.UtcTime)
        )
        df.index.name = "Index"
        df = df.applymap(lambda x: x.Value)
    else:  # if no result, return empty dataframe
        df = pd.DataFrame()

    return df


def calc_interpolated(
    starttime: Union[str, datetime.datetime],
    endtime: Union[str, datetime.datetime],
    interval: str,
    expression: str = "",
) -> pd.DataFrame:
    """Returns dataframe that contains the result of evaluating the passed expression
    over the passed time range at a defined interval.

    Expression arguments need to be entered as raw strings: r'expression'"""
    afrange = to_af_time_range(starttime, endtime)
    afinterval = AF.Time.AFTimeSpan.Parse(interval)
    result = AF.Data.AFCalculation.CalculateAtIntervals(
        0, expression, afrange, afinterval
    )

    if result:
        # process query results
        data = [list(result)]
        df = pd.DataFrame(data).T
        df.columns = ["calculation"]
        # https://docs.osisoft.com/bundle/af-sdk/page/html/T_OSIsoft_AF_Asset_AFValue.htm # noqa
        df.index = df[df.columns[0]].apply(
            lambda x: timestamp_to_index(x.Timestamp.UtcTime)
        )
        df.index.name = "Index"
        df = df.applymap(lambda x: x.Value)
    else:  # if no result, return empty dataframe
        df = pd.DataFrame()

    return df
