""" Calculations
    Core functionality for doing calculations using the AF.Data.AFCalculation class
"""
from typing import Union
import datetime
from pytz import timezone
from PIconnect.config import PIConfig

from PIconnect.AFSDK import AF
from PIconnect.time import (
    timestamp_to_index,
    to_af_time_range,
)
from PIconnect.PIConsts import (
    SummaryType,
    CalculationBasis,
    TimestampCalculation,
    ExpressionSampleType,
)

import pandas as pd


def calc_recorded(
    starttime: Union[str, datetime.datetime],
    endtime: Union[str, datetime.datetime],
    expression: str = "",
) -> pd.DataFrame:
    """Returns dataframe that contains the result of evaluating the passed expression at each point in time
    over the passed time range where a recorded value exists for a member of the expression.

    Expression argument need to be entered as raw strings: r'expression'"""
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


def calc_summary(
    starttime: Union[str, datetime.datetime],
    endtime: Union[str, datetime.datetime],
    interval: str,
    summary_types: SummaryType,
    expression: str = "",
    calculation_basis: CalculationBasis = CalculationBasis.TimeWeighted ,
    time_type: TimestampCalculation = TimestampCalculation.Auto,
    AFfilter_evaluation: ExpressionSampleType = ExpressionSampleType.ExpressionRecordedValues,
    filter_interval: str = None,
) -> pd.DataFrame:
    """Return dataframe of summary measures of calculations specified in expression,
    for the specified duration and interval.

    Args:
        starttime (Union[str, datetime.datetime]): start time
        endtime (Union[str, datetime.datetime]): end time
        interval (str): The bounding time for the evaluation period.
        summary_types (SummaryType object): SummaryType objects separated by '|'. List given
                below. E.g. "summary_types = SummaryType.Minimum | SummaryType.Maximum"
                Do not forget to import the SummaryType object from PIconnect.PIConsts

                - Total: A total over the time span
                - Average: Average value over the time span
                - Minimum: The minimum value in the time span
                - Maximum: The maximum value in the time span
                - Range: The range of the values (max-min) in the time span
                - StdDev: The sample standard deviation of the values over the time span
                - PopulationStdDev: The population standard deviation of the values over the time span
                - Count: The sum of the event count (when the calculation is event weighted).
                        The sum of the event time duration (when the calculation is time
                        weighted.)
                - PercentGood: The percentage of the data with a good value over the time range.
                        Based on time for time weighted calculations, based on event count for
                        event weigthed calculations.
                - TotalWithUOM: The total over the time span, with the unit of measurement that's
                        associated with the input (or no units if not defined for the input).
                - All: A convenience to retrieve all summary types
                - AllForNonNumeric: A convenience to retrieve all summary types for non-numeric data

        expression (raw string): A string containing the expression to be evaluated.
            The syntax for the expression generally follows the
            Performance Equation syntax as described in
            the PI Data Archive documentation.
        calculation_basis (CalculationBasis, optional): Basis by which to
            calculate the summary statistic.
            Defaults to CalculationBasis.TimeWeighted .
        time_type (TimestampCalculation, optional): How the timestamp is
            calculated. Defaults to TimestampCalculation.Auto.
        AFfilter_evaluation (ExpressionSampleType, optional): Expression
            Type. Defaults to
            ExpressionSampleType.ExpressionRecordedValues.

    Returns:
        pd.DataFrame: dataframe of summary measures"""

    AFTimeRange = to_af_time_range(starttime, endtime)
    if interval == 'event':
        AFInterval = AF.Time.AFTimeSpan(AFTimeRange.Span)
    else:
        AFInterval = AF.Time.AFTimeSpan.Parse(interval)
    AFfilter_interval = AF.Time.AFTimeSpan.Parse(filter_interval)

    try:
        result = AF.Data.AFCalculation.CalculateSummaries(
            0,
            expression,
            AFTimeRange,
            AFInterval,
            summary_types,
            calculation_basis,
            AFfilter_evaluation,
            AFfilter_interval,
            time_type,
        )
    except AF.PI.PIException as e:
        if str(e).startswith("[-11091]"):
            if type(endtime) == float:
                endtime = timezone(PIConfig.DEFAULT_TIMEZONE).localize(
                    datetime.datetime.now()
                )
            raise AttributeError(
                f"Duration of '{starttime - endtime}' exceeds the maximum allowed collection limit, please exclude event or reduce query duration"
            )
        else:
            raise AttributeError(e)

    df_final = pd.DataFrame()
    for x in result:  # per summary
        summary = x.ToString().replace("[", "").split(",")[0]
        values = [
            (timestamp_to_index(value.Timestamp.UtcTime), value.Value)
            for value in x.Value
        ]
        df = pd.DataFrame(values, columns=["Timestamp", "Value"])
        df["Summary"] = summary
        df_final = pd.concat([df_final, df], ignore_index=True)

    return df_final[
        [
            "Summary",
            "Value",
            "Timestamp",
        ]
    ]
