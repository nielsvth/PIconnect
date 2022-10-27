""" PI
    Core containers for connections to PI databases
"""
# pragma pylint: disable=unused-import, redefined-builtin
from __future__ import (
    absolute_import,
    division,
    print_function,
    unicode_literals,
)
from ast import Expression

from builtins import (
    ascii,
    bytes,
    chr,
    dict,
    filter,
    hex,
    input,
    int,
    list,
    map,
    next,
    object,
    oct,
    open,
    pow,
    range,
    round,
    str,
    super,
    zip,
)
from dataclasses import dataclass
import datetime
from typing import Dict, List, Union

try:
    from __builtin__ import str as BuiltinStr
except ImportError:
    BuiltinStr = str
# pragma pylint: enable=unused-import, redefined-builtin
from warnings import warn
from PIconnect._utils import classproperty
from PIconnect.AFSDK import AF
from PIconnect.PIConsts import (
    AuthenticationMode,
    BoundaryType,
    SummaryType,
    CalculationBasis,
    TimestampCalculation,
    PIPointType,
    ExpressionSampleType,
)
from PIconnect.time import timestamp_to_index, to_af_time_range, add_timezone, to_af_time

from collections import UserList

import pandas as pd
import numpy as np

pd.options.mode.chained_assignment = None  # default='warn'
_NOTHING = object()


class PIServer(object):  # pylint: disable=useless-object-inheritance
    """PIServer is a connection to an OSIsoft PI Server

    Args:
        server (str, optional): Name of the server to connect to.
            defaults to None
        username (str, optional): can be used only with password as well
        password (str, optional): -//-
        todo: domain, auth
        timeout (int, optional): the maximum seconds an operation can take

    .. note::
        If the specified `server` is unknown a warning is thrown and the
            connection is redirected to the default server, as if no server
            was passed. The list of known servers is available in the
            `PIServer.servers` dictionary.
    """

    version = "0.2.2"

    #: Dictionary of known servers, as reported by the SDK
    _servers = _NOTHING
    #: Default server, as reported by the SDK
    _default_server = _NOTHING

    def __init__(
        self,
        server=None,
        username=None,
        password=None,
        domain=None,
        authentication_mode=AuthenticationMode.PI_USER_AUTHENTICATION,
        timeout=None,
    ):
        if server and server not in self.servers:
            message = 'Server "{server}" not found, using the default server.'
            warn(message=message.format(server=server), category=UserWarning)
        if bool(username) != bool(password):
            raise ValueError(
                "When passing credentials both the username and password "
                + "must be specified."
            )
        if domain and not username:
            raise ValueError(
                "A domain can only specified together with a username and "
                + "password."
            )
        if username:
            from System.Net import NetworkCredential
            from System.Security import SecureString

            secure_pass = SecureString()
            for c in password:
                secure_pass.AppendChar(c)
            cred = [username, secure_pass] + ([domain] if domain else [])
            self._credentials = (
                NetworkCredential(*cred),
                int(authentication_mode),
            )
        else:
            self._credentials = None

        self.connection = self.servers.get(server, self.default_server)

        if timeout:
            from System import TimeSpan

            # System.TimeSpan(hours, minutes, seconds)
            self.connection.ConnectionInfo.OperationTimeOut = TimeSpan(
                0, 0, timeout
            )

    @classproperty
    def servers(self):
        if self._servers is _NOTHING:
            i, failures = 0, 0
            self._servers = {}
            from System import Exception as dotNetException  # type: ignore

            for i, server in enumerate(AF.PI.PIServers(), start=1):
                try:
                    self._servers[server.Name] = server
                except Exception:
                    failures += 1
                except dotNetException:
                    failures += 1
            if failures:
                warn(
                    "Could not load {} PI Server(s) out of {}".format(
                        failures, i
                    ),
                    ResourceWarning,
                )
        return self._servers

    @classproperty
    def default_server(self):
        if self._default_server is _NOTHING:
            self._default_server = None
            try:
                self._default_server = AF.PI.PIServers().DefaultPIServer
            except Exception:
                warn("Could not load the default PI Server", ResourceWarning)
        return self._default_server

    def __enter__(self):
        if self._credentials:
            self.connection.Connect(*self._credentials)
        else:
            # Don't force to retry connecting if previous attempt failed
            force_connection = False
            self.connection.Connect(force_connection)
        return self

    def __exit__(self, *args):
        self.connection.Disconnect()

    def __repr__(self):
        return "%s(\\\\%s)" % (self.__class__.__name__, self.name)

    @property
    def name(self):
        """name

        Name of the connected server
        """
        return self.connection.Name

    # methods
    def find_tags(self, query: Union[str, List[str]], source: str = None):
        """find_tags

        Search PIPoints on the PIServer

        Args:
            query (Union[str,List[str]]): String or list of strings with
                queries
            source (str, optional): Point source to limit the results.
                Defaults to None.

        Returns:
            list: A list of Tag objects as a result of the query

        .. todo::

            Reject searches while not connected
        """
        if isinstance(query, list):
            return TagList(
                [y for x in query for y in self.find_tags(x, source)]
            )
        elif not isinstance(query, str):
            raise TypeError(
                "Argument query must be either a string or a list of strings,"
                + "got type "
                + str(type(query))
            )
        return TagList(
            [
                Tag(pi_point)
                for pi_point in AF.PI.PIPoint.FindPIPoints(
                    self.connection, BuiltinStr(query), source, None
                )
            ]
        )

    def tag_overview(self, query: str) -> pd.DataFrame:
        """Returns dataframe containing overview for each tag that meets the
        restrictions specified in the query

        Args:
            query (str): string to query

        Returns:
            pd.DataFrame: dataframe with
                Tag object
                Tag name
                Description
                UOM
        """
        tags = self.find_tags(str(query))

        df = pd.DataFrame()
        df["Tag"] = [tag for tag in tags]
        # load attributes before GET
        # https://docs.osisoft.com/bundle/af-sdk/page/html/T_OSIsoft_AF_PI_PIPointType.htm
        attrsToGet = [
            "Name",
            "Description",
            "UOM",
            "PointType",
            "PointType_desc",
        ]
        for attr in attrsToGet:
            df[attr] = df["Tag"].apply(lambda x: getattr(x, attr.lower()))

        return df


class Tag:
    """Container for Tag object"""

    def __init__(self, tag):
        self.validate(tag)
        self.tag = tag
        self.__attributes_loaded = False

    def __repr__(self):
        return "Tag:" + self.name

    def __str__(self):
        return "Tag:" + self.name

    @staticmethod
    def validate(tag):
        if not isinstance(tag, AF.PI.PIPoint):
            raise AttributeError(
                "This type of input is not a Tag object, use the 'find_tags' function of the PIServer class to find tag objects"
            )

    def __load_attributes(self):
        """Load the raw attributes of the PI Point from the server"""
        if not self.__attributes_loaded:
            self.tag.LoadAttributes([])
            self.__attributes_loaded = True

    # Properties
    @property
    def name(self):
        """Return name of Tag"""
        return self.tag.Name

    @property
    def pipoint(self):
        """Return"""
        return self.tag

    @property
    def server(self):
        """Return connected server"""
        return PIServer(self.tag.Server.Name)

    @property
    def raw_attributes(self):
        """Return dictionary of the raw attributes"""
        self.__load_attributes()
        return {att.Key: att.Value for att in self.tag.GetAttributes([])}

    @property
    def last_update(self):
        """Return datetime at which the last value was recorded"""
        return timestamp_to_index(self.tag.CurrentValue().Timestamp.UtcTime)

    @property
    def uom(self):
        """Return units of measument"""
        return self.raw_attributes["engunits"]

    @property
    def description(self):
        """Return description"""
        return self.raw_attributes["descriptor"]

    @property
    def created(self):
        """Return the creation datetime"""
        return timestamp_to_index(self.raw_attributes["creationdate"])

    @property
    def pointtype(self):
        """Return an integer value corresponding to the pointtype"""
        # ref: https://docs.osisoft.com/bundle/af-sdk/page/html/T_OSIsoft_AF_PI_PIPointType.htm # noqa
        return self.tag.PointType

    @property
    def pointtype_desc(self):
        """Return the pointtype"""
        return str(PIPointType(self.pointtype))

    # Methods
    def current_value(self) -> int:
        """Return last recorded value"""
        return self.tag.CurrentValue().Value

    def interpolated_value(
        self,
        time: Union[str, datetime.datetime]
    ) -> int:
        """Return interpolated value as specified time"""
        aftime = to_af_time(time)
        return self.tag.InterpolatedValue(aftime)

    def interpolated_values(
        self,
        starttime: Union[str, datetime.datetime],
        endtime: Union[str, datetime.datetime],
        interval: str,
        filter_expression: str = "",
    ) -> pd.DataFrame:
        """Retrieve interpolated data across a time range and specified
        interval using optional expression.

        Args:
            starttime (Union[str, datetime.datetime]): start time
            endtime (Union[str, datetime.datetime]): end time
            interval (str): interval to interpolate to
            filter_expression (str, optional): Filter expression.
                Defaults to "".

        Returns:
            pd.DataFrame: resulting dataframe
        """
        AFInterval = AF.Time.AFTimeSpan.Parse(interval)
        AFTimeRange = to_af_time_range(starttime, endtime)
        filter_expression = filter_expression.replace("%tag%", self.name)

        # Could have issues with quering multiple PI Data Archives
        # simultanously, see documentation:
        # https://docs.osisoft.com/bundle/af-sdk/page/html/M_OSIsoft_AF_PI_PIPointList_InterpolatedValues.htm # noqa
        result = self.tag.InterpolatedValues(
            AFTimeRange, AFInterval, filter_expression, False
        )

        if result:
            # process query results
            data = [list(result)]
            df = pd.DataFrame(data).T
            df.columns = [self.name]
            # https://docs.osisoft.com/bundle/af-sdk/page/html/T_OSIsoft_AF_Asset_AFValue.htm
            df.index = df[df.columns[0]].apply(
                lambda x: timestamp_to_index(x.Timestamp.UtcTime)
            )
            df.index.name = "Index"
            df = df.applymap(lambda x: x.Value)
            return df
        else:  # if no result, return empty dataframe
            return pd.DataFrame()

    def recorded_values(
        self,
        starttime: Union[str, datetime.datetime],
        endtime: Union[str, datetime.datetime],
        filter_expression: str = "",
        AFBoundaryType=BoundaryType.INTERPOLATED,
    ) -> pd.DataFrame:
        """Retrieve recorded data across a time range and specified
        using optional expression.

        Args:
            starttime (Union[str, datetime.datetime]): start time
            endtime (Union[str, datetime.datetime]): end time
            filter_expression (str, optional): Filter expression.
                Defaults to "".
            AFBoundaryType (_type_, optional): Defined Boundary type.
                Defaults to BoundaryType.INTERPOLATED.

        Returns:
            pd.DataFrame: resultant dataframe
        """
        AFTimeRange = to_af_time_range(starttime, endtime)
        filter_expression = filter_expression.replace("%tag%", self.name)

        # Could have issues with quering multiple PI Data Archives
        # simultanously, see documentation maximum number of events that can
        # be returned with a single call. As of PI 3.4.380, the default is
        # set at 1.5M. Additonal info at:
        # https://docs.osisoft.com/bundle/af-sdk/page/html/M_OSIsoft_AF_PI_PIPointList_RecordedValues.htm # noqa
        result = self.tag.RecordedValues(
            AFTimeRange, AFBoundaryType, filter_expression, False
        )

        if result:
            # process query results
            data = [list(result)]
            df = pd.DataFrame(data).T
            df.columns = [self.name]
            # https://docs.osisoft.com/bundle/af-sdk/page/html/T_OSIsoft_AF_Asset_AFValue.htm # noqa
            df.index = df[df.columns[0]].apply(
                lambda x: timestamp_to_index(x.Timestamp.UtcTime)
            )
            df.index.name = "Index"
            df = df.applymap(lambda x: x.Value)
        else:  # if no result, return empty dataframe
            df = pd.DataFrame()

        return df

    def plot_values(
        self,
        starttime: Union[str, datetime.datetime],
        endtime: Union[str, datetime.datetime],
        nr_of_intervals: int,
    ) -> pd.DataFrame:
        """Retrieves values over the specified time range suitable for
        plotting over the number of intervals (typically represents pixels).
        Each interval can produce up to 5 values if they are unique, the first
        value in the interval, the last value, the highest value, the lowest
        value and at most one exceptional point (bad status or digital state).

        Args:
            starttime (Union[str, datetime.datetime]): start time
            endtime (Union[str, datetime.datetime]): end time
            nr_of_intervals (int): Number of intervals

        Returns:
            pd.DataFrame: Dataframe with values that will produce the most
            accurate plot over the time range while minimizing the amount
            of data returned
        """
        AFTimeRange = to_af_time_range(starttime, endtime)

        result = self.tag.PlotValues(AFTimeRange, nr_of_intervals)

        if result:
            # process query results
            data = [list(result)]
            df = pd.DataFrame(data).T
            df.columns = [self.name]
            # https://docs.osisoft.com/bundle/af-sdk/page/html/T_OSIsoft_AF_Asset_AFValue.htm # noqa
            df.index = df[df.columns[0]].apply(
                lambda x: timestamp_to_index(x.Timestamp.UtcTime)
            )
            df.index.name = "Index"
            df = df.applymap(lambda x: x.Value)
        else:
            df = pd.DataFrame()
        return df

    def _parseSummaryResult(self, result: SummaryType) -> pd.DataFrame:
        """Parse a Summary result and return a dataframe.

        Args:
            res (SummaryType): Summary to parse

        Returns:
            pd.DataFrame: resulting dataframe
        """
        # summary
        df_final = pd.DataFrame()
        for x in result:  # per summary
            summary = SummaryType(x.Key).name
            value = x.Value
            timestamp = timestamp_to_index(x.Value.Timestamp.UtcTime)
            df = pd.DataFrame(
                [[summary, value, timestamp]],
                columns=["Summary", "Value", "Timestamp"],
            )
            df_final = pd.concat([df_final, df], ignore_index=True)

        return df_final

    def _parseSummariesResult(self, result: SummaryType) -> pd.DataFrame:
        """Parse a Summaries result and return a dataframe.

        Args:
            res (SummaryType): Summary to parse

        Returns:
            pd.DataFrame: resulting dataframe
        """
        # summaries
        df_final = pd.DataFrame()
        for x in result:  # per summary
            summary = SummaryType(x.Key).name
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

    # CalculationBasis.EVENT_WEIGHTED avoids issues(?) with interpolation:
    # ref. #Issue 1
    def summary(
        self,
        starttime: Union[str, datetime.datetime],
        endtime: Union[str, datetime.datetime],
        summary_types: int,
        calculation_basis: CalculationBasis = CalculationBasis.TIME_WEIGHTED,
        time_type: TimestampCalculation = TimestampCalculation.AUTO,
    ) -> pd.DataFrame:
        """Return specified summary measure(s) for Tag within the specified
        timeframe

        Args:
            starttime (Union[str, datetime.datetime]): start time
            endtime (Union[str, datetime.datetime]): end time
            summary_types (int): integers separated by '|'. List given
                below. E.g. "summary_types = 1|8" gives TOTAL and MAXIMUM

                - TOTAL = 1: A total over the time span
                - AVERAGE = 2: Average value over the time span
                - MINIMUM = 4: The minimum value in the time span
                - MAXIMUM = 8: The maximum value in the time span
                - RANGE = 16: The range of the values (max-min) in the time
                    span
                - STD_DEV = 32 : The sample standard deviation of the values
                    over the time span
                - POP_STD_DEV = 64: The population standard deviation of the
                    values over the time span
                - COUNT = 128: The sum of the event count (when the
                    calculation is event weighted). The sum of the event time
                        duration (when the calculation is time weighted.)
                - PERCENT_GOOD = 8192: The percentage of the data with a good
                    value over the time range. Based on time for time weighted
                        calculations, based on event count for event weigthed
                        calculations.
                - TOTAL_WITH_UOM = 16384: The total over the time span, with
                    the unit of measurement that's associated with the input
                    (or no units if not defined for the input)
                - ALL = 24831: A convenience to retrieve all summary types
                - ALL_FOR_NON_NUMERIC = 8320: A convenience to retrieve all
                    summary types for non-numeric data

            calculation_basis (CalculationBasis, optional): Basis by which to
                calculate the summary statistic.
                Defaults to CalculationBasis.TIME_WEIGHTED.
            time_type (TimestampCalculation, optional): How the timestamp is
                calculated. Defaults to TimestampCalculation.AUTO.

        Returns:
            pd.DataFrame: Dataframe with requested summary statistics
        """
        AFTimeRange = to_af_time_range(starttime, endtime)

        result = self.tag.Summary(
            AFTimeRange, summary_types, calculation_basis, time_type
        )

        df_final = self._parseSummaryResult(result)

        return df_final

    def summaries(
        self,
        starttime: Union[str, datetime.datetime],
        endtime: Union[str, datetime.datetime],
        interval: str,
        summary_types: int,
        calculation_basis: CalculationBasis = CalculationBasis.TIME_WEIGHTED,
        time_type: TimestampCalculation = TimestampCalculation.AUTO,
    ) -> pd.DataFrame:
        """Return specified summary measure(s) for each interval within the
        specified timeframe

        Args:
            starttime (Union[str, datetime.datetime]): start time
            endtime (Union[str, datetime.datetime]): end time
            interval (str): interval to interpolate to
            summary_types (int): integers separated by '|'. List given
                below. E.g. "summary_types = 1|8" gives TOTAL and MAXIMUM

                - TOTAL = 1: A total over the time span
                - AVERAGE = 2: Average value over the time span
                - MINIMUM = 4: The minimum value in the time span
                - MAXIMUM = 8: The maximum value in the time span
                - RANGE = 16: The range of the values (max-min) in the time
                    span
                - STD_DEV = 32 : The sample standard deviation of the values
                    over the time span
                - POP_STD_DEV = 64: The population standard deviation of the
                    values over the time span
                - COUNT = 128: The sum of the event count (when the
                    calculation is event weighted). The sum of the event time
                        duration (when the calculation is time weighted.)
                - PERCENT_GOOD = 8192: The percentage of the data with a good
                    value over the time range. Based on time for time weighted
                        calculations, based on event count for event weigthed
                        calculations.
                - TOTAL_WITH_UOM = 16384: The total over the time span, with
                    the unit of measurement that's associated with the input
                    (or no units if not defined for the input)
                - ALL = 24831: A convenience to retrieve all summary types
                - ALL_FOR_NON_NUMERIC = 8320: A convenience to retrieve all
                    summary types for non-numeric data

            calculation_basis (CalculationBasis, optional): Basis by which to
                calculate the summary statistic.
                Defaults to CalculationBasis.TIME_WEIGHTED.
            time_type (TimestampCalculation, optional): How the timestamp is
                calculated. Defaults to TimestampCalculation.AUTO.

        Returns:
            pd.DataFrame: Dataframe with requested summary statistics
        """
        AFTimeRange = to_af_time_range(starttime, endtime)
        AFInterval = AF.Time.AFTimeSpan.Parse(interval)

        result = self.tag.Summaries(
            AFTimeRange,
            AFInterval,
            summary_types,
            calculation_basis,
            time_type,
        )

        df_final = self._parseSummariesResult(result)

        return df_final

    def filtered_summaries(
        self,
        starttime: Union[str, datetime.datetime],
        endtime: Union[str, datetime.datetime],
        interval: str,
        summary_types: int,
        filter_expression: str,
        calculation_basis: CalculationBasis = CalculationBasis.TIME_WEIGHTED,
        time_type: TimestampCalculation = TimestampCalculation.AUTO,
        AFfilter_evaluation: ExpressionSampleType = ExpressionSampleType.EXPRESSION_RECORDED_VALUES,  # noqa
        filter_interval: str = None,
    ) -> pd.DataFrame:
        """Return one or more summary values for each interval, within a
        specified timeframe, for values that meet the specified filter
        condition

        Args:
            starttime (Union[str, datetime.datetime]): start time
            endtime (Union[str, datetime.datetime]): end time
            interval (str): interval to interpolate to
            summary_types (int): integers separated by '|'. List given
                below. E.g. "summary_types = 1|8" gives TOTAL and MAXIMUM

                - TOTAL = 1: A total over the time span
                - AVERAGE = 2: Average value over the time span
                - MINIMUM = 4: The minimum value in the time span
                - MAXIMUM = 8: The maximum value in the time span
                - RANGE = 16: The range of the values (max-min) in the time
                    span
                - STD_DEV = 32 : The sample standard deviation of the values
                    over the time span
                - POP_STD_DEV = 64: The population standard deviation of the
                    values over the time span
                - COUNT = 128: The sum of the event count (when the
                    calculation is event weighted). The sum of the event time
                        duration (when the calculation is time weighted.)
                - PERCENT_GOOD = 8192: The percentage of the data with a good
                    value over the time range. Based on time for time weighted
                        calculations, based on event count for event weigthed
                        calculations.
                - TOTAL_WITH_UOM = 16384: The total over the time span, with
                    the unit of measurement that's associated with the input
                    (or no units if not defined for the input)
                - ALL = 24831: A convenience to retrieve all summary types
                - ALL_FOR_NON_NUMERIC = 8320: A convenience to retrieve all
                    summary types for non-numeric data

            filter_expression (str):  Filter expression.
            calculation_basis (CalculationBasis, optional): Basis by which to
                calculate the summary statistic.
                Defaults to CalculationBasis.TIME_WEIGHTED.
            time_type (TimestampCalculation, optional): How the timestamp is
                calculated. Defaults to TimestampCalculation.AUTO.
            AFfilter_evaluation (ExpressionSampleType, optional): Expression
                Type. Defaults to
                ExpressionSampleType.EXPRESSION_RECORDED_VALUES.
            filter_interval (str, optional): _description_. Defaults to None.

        Returns:
            pd.DataFrame: Dataframe with requested summary statistics
        """
        AFTimeRange = to_af_time_range(starttime, endtime)
        AFInterval = AF.Time.AFTimeSpan.Parse(interval)
        filter_expression = filter_expression.replace("%tag%", self.name)
        AFfilter_interval = AF.Time.AFTimeSpan.Parse(filter_interval)

        result = self.tag.FilteredSummaries(
            AFTimeRange,
            AFInterval,
            filter_expression,
            summary_types,
            calculation_basis,
            AFfilter_evaluation,
            AFfilter_interval,
            time_type,
        )

        df_final = self._parseSummariesResult(result)

        return df_final


class TagList(UserList):
    """Container for TagList object"""

    def __init__(self, data):
        self.validate(data)
        self.data = data

    def __repr__(self):
        return str([tag for tag in self.data])

    def __str__(self):
        return str([tag for tag in self.data])

    @staticmethod
    def validate(data):
        """Validate input meets requirements for TagList"""
        for tag in data:
            if not isinstance(tag, Tag):
                raise AttributeError(
                    f"Can not convert tag of type "
                    + f"{type(tag)} to TagList object"
                )

    def current_values(self) -> pd.DataFrame:
        """Getter method for current values of all tags in list

        Returns:
            pd.DataFrame: values of all tags with names as the column
        """
        PIPointlist = generate_pipointlist(self)
        result = PIPointlist.CurrentValue()
        if result:
            values = [x.Value for x in result]
            tags = [x.PIPoint.Name for x in result]
            out = pd.DataFrame([values], columns=tags)
        else:
            out = pd.DataFrame()
        return out

    # TODO: convert this to simply calling Tag.plot_values() if possible
    def plot_values(
        self,
        starttime: Union[str, datetime.datetime],
        endtime: Union[str, datetime.datetime],
        nr_of_intervals: int,
    ) -> Dict[str, pd.DataFrame]:
        """Retrieves values over the specified time range suitable for plotting
        over the number of intervals (typically represents pixels).Returns a
        Dictionary of DataFrames for Tags in Taglist with values that will
        produce the most accurate plot over the time range while minimizing
        the amount of data returned

        Args:
            starttime (Union[str, datetime.datetime]): start time
            endtime (Union[str, datetime.datetime]): end time
            nr_of_intervals (int): Number of intervals

        Returns:
            Dict[str, pd.DataFrame]: TagName: Dataframe of Data
        """
        AFTimeRange = to_af_time_range(starttime, endtime)
        PIPointlist = generate_pipointlist(self)

        result = PIPointlist.PlotValues(
            AFTimeRange,
            nr_of_intervals,
            AF.PI.PIPagingConfiguration(AF.PI.PIPageType.TagCount, 1000),
        )

        if result:
            # process query results
            data = list(result.ResultQueue.GetConsumingEnumerable())
            data = [list(series) for series in data]

            dct = {}
            tags = [tag.Name for tag in result.PointList]
            for i, lst in enumerate(data):
                df = pd.DataFrame([lst]).T
                df.columns = ["Data"]
                # https://docs.osisoft.com/bundle/af-sdk/page/html/T_OSIsoft_AF_Asset_AFValue.htm
                df.index = df["Data"].apply(
                    lambda x: timestamp_to_index(x.Timestamp.UtcTime)
                )
                df.index.name = "Index"
                df = df.applymap(lambda x: x.Value)
                dct[tags[i]] = df
            return dct
        else:
            return dict()

    # TODO: pass to underlying Tag function
    def interpolated_values(
        self,
        starttime: Union[str, datetime.datetime],
        endtime: Union[str, datetime.datetime],
        interval: str,
        filter_expression: str = "",
    ) -> pd.DataFrame:
        """Retrieve interpolated values for each Tag in TagList

        Args:
            starttime (Union[str, datetime.datetime]): start time
            endtime (Union[str, datetime.datetime]): end time
            interval (str): interval to interpolate to
            filter_expression (str, optional): Filter expression.
                Defaults to "".

        Returns:
            pd.DataFrame: interpolated values for Tags in TagList, between
                starttime and endtime
        """
        PIPointlist = generate_pipointlist(self)
        AFInterval = AF.Time.AFTimeSpan.Parse(interval)
        AFTimeRange = to_af_time_range(starttime, endtime)

        # Could have issues with quering multiple PI Data Archives
        # simultanously, see documentation
        # https://docs.osisoft.com/bundle/af-sdk/page/html/M_OSIsoft_AF_PI_PIPointList_InterpolatedValues.htm # noqa
        result = PIPointlist.InterpolatedValues(
            AFTimeRange,
            AFInterval,
            filter_expression,
            False,
            AF.PI.PIPagingConfiguration(AF.PI.PIPageType.TagCount, 1000),
        )

        if result:
            # process query results
            data = list(result.ResultQueue.GetConsumingEnumerable())
            data = [list(series) for series in data]
            df = pd.DataFrame(data).T
            df.columns = [tag.Name for tag in result.PointList]
            # https://docs.osisoft.com/bundle/af-sdk/page/html/T_OSIsoft_AF_Asset_AFValue.htm # noqa
            df.index = df[df.columns[0]].apply(
                lambda x: timestamp_to_index(x.Timestamp.UtcTime)
            )
            df.index.name = "Index"
            df = df.applymap(lambda x: x.Value)
            return df
        else:  # if no result, return empty dataframe
            return pd.DataFrame()

    # TODO: pass to underlying tag function
    # TODO: should default BoundaryType be INSIDE ?
    def recorded_values(
        self,
        starttime: Union[str, datetime.datetime],
        endtime: Union[str, datetime.datetime],
        filter_expression: str = "",
        AFBoundaryType: BoundaryType = BoundaryType.INTERPOLATED,
    ) -> Dict[str, pd.DataFrame]:
        """Retrieve recorded values for each Tag in TagList

        Args:
            starttime (Union[str, datetime.datetime]): start time
            endtime (Union[str, datetime.datetime]): end time
            filter_expression (str, optional): Filter expression.
                Defaults to "".
            AFBoundaryType (BoundaryType, optional): Defined BoundaryType.
                Defaults to BoundaryType.INTERPOLATED.

        Returns:
            Dict[str, pd.DataFrame]: tag Name: dataframe of data
        """
        PIPointlist = generate_pipointlist(self)
        AFTimeRange = to_af_time_range(starttime, endtime)

        # Could have issues with quering multiple PI Data Archives
        # simultanously, see documentation
        # maximum number of events that can be returned with a single call.
        # As of PI 3.4.380, the default is set at 1.5M
        # https://docs.osisoft.com/bundle/af-sdk/page/html/M_OSIsoft_AF_PI_PIPointList_RecordedValues.htm # noqa
        result = PIPointlist.RecordedValues(
            AFTimeRange,
            AFBoundaryType,
            filter_expression,
            False,
            AF.PI.PIPagingConfiguration(AF.PI.PIPageType.TagCount, 1000),
        )

        if result:
            # process query results
            data = list(result.ResultQueue.GetConsumingEnumerable())
            data = [list(series) for series in data]

            dct = {}
            tags = [tag.Name for tag in result.PointList]
            for i, lst in enumerate(data):
                df = pd.DataFrame([lst]).T
                df.columns = ["Data"]
                # https://docs.osisoft.com/bundle/af-sdk/page/html/T_OSIsoft_AF_Asset_AFValue.htm # noqa
                df.index = df["Data"].apply(
                    lambda x: timestamp_to_index(x.Timestamp.UtcTime)
                )
                df.index.name = "Index"
                df = df.applymap(lambda x: x.Value)
                dct[tags[i]] = df
            return dct
        else:  # if no result, return empty dictionary
            return dict()

    # TODO: pass to underlying Tag function
    def summary(
        self,
        starttime: Union[str, datetime.datetime],
        endtime: Union[str, datetime.datetime],
        summary_types: int,
        calculation_basis: CalculationBasis = CalculationBasis.TIME_WEIGHTED,
        time_type: TimestampCalculation = TimestampCalculation.AUTO,
    ) -> pd.DataFrame:
        """Return specified summary measure(s) for Tags in Taglist within
        the specified timeframe.

        Args:
            starttime (Union[str, datetime.datetime]): start time
            endtime (Union[str, datetime.datetime]): end time
            summary_types (int): integers separated by '|'. List given
                below. E.g. "summary_types = 1|8" gives TOTAL and MAXIMUM

                - TOTAL = 1: A total over the time span
                - AVERAGE = 2: Average value over the time span
                - MINIMUM = 4: The minimum value in the time span
                - MAXIMUM = 8: The maximum value in the time span
                - RANGE = 16: The range of the values (max-min) in the time
                    span
                - STD_DEV = 32 : The sample standard deviation of the values
                    over the time span
                - POP_STD_DEV = 64: The population standard deviation of the
                    values over the time span
                - COUNT = 128: The sum of the event count (when the
                    calculation is event weighted). The sum of the event time
                        duration (when the calculation is time weighted.)
                - PERCENT_GOOD = 8192: The percentage of the data with a good
                    value over the time range. Based on time for time weighted
                        calculations, based on event count for event weigthed
                        calculations.
                - TOTAL_WITH_UOM = 16384: The total over the time span, with
                    the unit of measurement that's associated with the input
                    (or no units if not defined for the input)
                - ALL = 24831: A convenience to retrieve all summary types
                - ALL_FOR_NON_NUMERIC = 8320: A convenience to retrieve all
                    summary types for non-numeric data

            calculation_basis (CalculationBasis, optional): Basis by which to
                calculate the summary statistic.
                Defaults to CalculationBasis.TIME_WEIGHTED.
            time_type (TimestampCalculation, optional): How the timestamp is
                calculated. Defaults to TimestampCalculation.AUTO.

        Returns:
            pd.DataFrame: Dataframe with requested summary statistics
        """
        PIPointlist = generate_pipointlist(self)
        AFTimeRange = to_af_time_range(starttime, endtime)

        result = PIPointlist.Summary(
            AFTimeRange,
            summary_types,
            calculation_basis,
            time_type,
            AF.PI.PIPagingConfiguration(AF.PI.PIPageType.TagCount, 1000),
        )
        # to avoid queue emptying
        data = list(result)
        if data:
            df_final = pd.DataFrame()
            for x in data:  # per tag
                point = [y.PIPoint.Name for y in x.Values][0]
                summaries = [SummaryType(y).name for y in x.Keys]
                values = [
                    [y.Value, timestamp_to_index(y.Timestamp.UtcTime)]
                    for y in x.Values
                ]
                df = pd.DataFrame(values, columns=["Value", "Timestamp"])
                df["Tag"] = point
                df["Summary"] = summaries
                df_final = pd.concat([df_final, df], ignore_index=True)

            return df_final[["Tag", "Summary", "Value", "Timestamp"]]
        else:
            return pd.DataFrame()

    # TODO: pass to underlying Tag function
    def summaries(
        self,
        starttime: Union[str, datetime.datetime],
        endtime: Union[str, datetime.datetime],
        interval: str,
        summary_types: int,
        calculation_basis: CalculationBasis = CalculationBasis.TIME_WEIGHTED,
        time_type: TimestampCalculation = TimestampCalculation.AUTO,
    ) -> pd.DataFrame:
        """For each Tag in TagList, return specified summary measure(s) for
        each interval within the specified timeframe

        Args:
            starttime (Union[str, datetime.datetime]): start time
            endtime (Union[str, datetime.datetime]): end time
            interval (str): interval to interpolate to
            summary_types (int): integers separated by '|'. List given
                below. E.g. "summary_types = 1|8" gives TOTAL and MAXIMUM

                - TOTAL = 1: A total over the time span
                - AVERAGE = 2: Average value over the time span
                - MINIMUM = 4: The minimum value in the time span
                - MAXIMUM = 8: The maximum value in the time span
                - RANGE = 16: The range of the values (max-min) in the time
                    span
                - STD_DEV = 32 : The sample standard deviation of the values
                    over the time span
                - POP_STD_DEV = 64: The population standard deviation of the
                    values over the time span
                - COUNT = 128: The sum of the event count (when the
                    calculation is event weighted). The sum of the event time
                        duration (when the calculation is time weighted.)
                - PERCENT_GOOD = 8192: The percentage of the data with a good
                    value over the time range. Based on time for time weighted
                        calculations, based on event count for event weigthed
                        calculations.
                - TOTAL_WITH_UOM = 16384: The total over the time span, with
                    the unit of measurement that's associated with the input
                    (or no units if not defined for the input)
                - ALL = 24831: A convenience to retrieve all summary types
                - ALL_FOR_NON_NUMERIC = 8320: A convenience to retrieve all
                    summary types for non-numeric data

            calculation_basis (CalculationBasis, optional): Basis by which to
                calculate the summary statistic.
                Defaults to CalculationBasis.TIME_WEIGHTED.
            time_type (TimestampCalculation, optional): How the timestamp is
                calculated. Defaults to TimestampCalculation.AUTO.

        Returns:
            pd.DataFrame: Dataframe with requested summary statistics
        """
        PIPointlist = generate_pipointlist(self)
        AFTimeRange = to_af_time_range(starttime, endtime)
        AFInterval = AF.Time.AFTimeSpan.Parse(interval)

        result = PIPointlist.Summaries(
            AFTimeRange,
            AFInterval,
            summary_types,
            calculation_basis,
            time_type,
            AF.PI.PIPagingConfiguration(AF.PI.PIPageType.TagCount, 1000),
        )
        data = list(result)
        df_final = pd.DataFrame()
        if data:
            for x in data:  # per tag
                point = [y.PIPoint.Name for y in x.Values][0]
                summaries = [y for y in x.Keys]
                df = pd.DataFrame(
                    [[point, summary] for summary in summaries],
                    columns=["Tag", "Summary"],
                )
                df["Timestamp"] = df["Summary"].apply(
                    lambda key: [
                        (
                            timestamp_to_index(value.Timestamp.UtcTime),
                            value.Value,
                        )
                        for value in x[key]
                    ]
                )
                df["Summary"] = df["Summary"].apply(
                    lambda x: SummaryType(x).name
                )
                df = df.explode("Timestamp")
                df[["Timestamp", "Value"]] = df["Timestamp"].apply(
                    pd.Series
                )  # explode list to columns
                df_final.append(df, ignore_index=True)

            df_final = df_final[["Tag", "Summary", "Value", "Timestamp"]]

        return df_final

    # TODO: pass to underlying Tag function
    def filtered_summaries(
        self,
        starttime: Union[str, datetime.datetime],
        endtime: Union[str, datetime.datetime],
        interval: str,
        summary_types: int,
        filter_expression: str,
        calculation_basis: CalculationBasis = CalculationBasis.TIME_WEIGHTED,
        time_type: TimestampCalculation = TimestampCalculation.AUTO,
        AFfilter_evaluation: ExpressionSampleType = ExpressionSampleType.EXPRESSION_RECORDED_VALUES,  # noqa
        filter_interval: str = None,
    ) -> pd.DataFrame:
        """For each Tag in TagList, return one or more summary values for each
        interval, within a specified timeframe, for values that meet the
        specified filter condition

        Args:
            starttime (Union[str, datetime.datetime]): start time
            endtime (Union[str, datetime.datetime]): end time
            interval (str): interval to interpolate to
            summary_types (int): integers separated by '|'. List given
                below. E.g. "summary_types = 1|8" gives TOTAL and MAXIMUM

                - TOTAL = 1: A total over the time span
                - AVERAGE = 2: Average value over the time span
                - MINIMUM = 4: The minimum value in the time span
                - MAXIMUM = 8: The maximum value in the time span
                - RANGE = 16: The range of the values (max-min) in the time
                    span
                - STD_DEV = 32 : The sample standard deviation of the values
                    over the time span
                - POP_STD_DEV = 64: The population standard deviation of the
                    values over the time span
                - COUNT = 128: The sum of the event count (when the
                    calculation is event weighted). The sum of the event time
                        duration (when the calculation is time weighted.)
                - PERCENT_GOOD = 8192: The percentage of the data with a good
                    value over the time range. Based on time for time weighted
                        calculations, based on event count for event weigthed
                        calculations.
                - TOTAL_WITH_UOM = 16384: The total over the time span, with
                    the unit of measurement that's associated with the input
                    (or no units if not defined for the input)
                - ALL = 24831: A convenience to retrieve all summary types
                - ALL_FOR_NON_NUMERIC = 8320: A convenience to retrieve all
                    summary types for non-numeric data

            filter_expression (str):  Filter expression.
            calculation_basis (CalculationBasis, optional): Basis by which to
                calculate the summary statistic.
                Defaults to CalculationBasis.TIME_WEIGHTED.
            time_type (TimestampCalculation, optional): How the timestamp is
                calculated. Defaults to TimestampCalculation.AUTO.
            AFfilter_evaluation (ExpressionSampleType, optional): Expression
                Type. Defaults to
                ExpressionSampleType.EXPRESSION_RECORDED_VALUES.
            filter_interval (str, optional): _description_. Defaults to None.

        Returns:
            pd.DataFrame: Dataframe with requested summary statistics
        """
        PIPointlist = generate_pipointlist(self)
        AFTimeRange = to_af_time_range(starttime, endtime)
        AFInterval = AF.Time.AFTimeSpan.Parse(interval)
        AFfilter_interval = AF.Time.AFTimeSpan.Parse(filter_interval)
        result = PIPointlist.FilteredSummaries(
            AFTimeRange,
            AFInterval,
            filter_expression,
            summary_types,
            calculation_basis,
            AFfilter_evaluation,
            AFfilter_interval,
            time_type,
            AF.PI.PIPagingConfiguration(AF.PI.PIPageType.TagCount, 1000),
        )

        data = list(result)
        df_final = pd.DataFrame()
        if data:
            for x in data:  # per tag
                point = [y.PIPoint.Name for y in x.Values][0]
                summaries = [y for y in x.Keys]
                df = pd.DataFrame(
                    [[point, summary] for summary in summaries],
                    columns=["Tag", "Summary"],
                )
                df["Timestamp"] = df["Summary"].apply(
                    lambda key: [
                        (
                            timestamp_to_index(value.Timestamp.UtcTime),
                            value.Value,
                        )
                        for value in x[key]
                    ]
                )
                df["Summary"] = df["Summary"].apply(
                    lambda x: SummaryType(x).name
                )
                df = df.explode("Timestamp")
                df[["Timestamp", "Value"]] = df["Timestamp"].apply(
                    pd.Series
                )  # explode list to columns
                df_final = df_final.append(df, ignore_index=True)

            df_final = df_final[["Tag", "Summary", "Value", "Timestamp"]]

        return df_final


# aux functions

# TODO: This should be converted to a staticmethod for the TagList class.
# TODO: Define the output type of this.
def generate_pipointlist(tag_list: TagList) -> AF.PI.PIPointList:
    """Generate and populate object of PIPointList class from TagList object

    Args:
        tag_list (TagList): TagList

    Raises:
        Exception: if improper type passed

    Returns:
        AF.PI.PIPointList: PiPointList from the AFSDK
    """
    if not isinstance(tag_list, TagList):
        raise Exception("Input is not a TagList object")

    PIPointlist = AF.PI.PIPointList()
    for tag in tag_list:
        PIPointlist.Add(tag.pipoint)
    return PIPointlist


# TODO: This should be converted to a staticmethod for the TagList class.
# TODO: Confirm the type for dataserver.


def convert_to_TagList(
    tag_list: List[Union[str, Tag]], dataserver: PIServer = None
) -> TagList:
    """Convert list of strings OR list of Tag objects to Taglist

    Args:
        tag_list (List[Union[str, Tag]]): list of strings/Tags
        dataserver (PIServer, optional): dataserver; necessary if the list is
            strings. Defaults to None.

    Raises:
        Exception: if tag_list is a string
        AttributeError: if tag_list is filled with strings and no dataserver
            is provided

    Returns:
        TagList: resultant TagList
    """
    if isinstance(tag_list, str):
        raise Exception("Tag(s) need to be inside a list")
    elif isinstance(tag_list, TagList):
        return tag_list
    else:
        try:
            return TagList(tag_list)
        except:
            return dataserver.find_tags(tag_list)

# Can't the user can simply use iPyKernel's display func, e.g. display(df)
def view(dataframe: pd.DataFrame) -> pd.DataFrame:
    """Return a string/float version of dataframe that can be viewed in the
    variable console

    Args:
        dataframe (pd.DataFrame): input dataframe

    Returns:
        pd.DataFrame: modified dataframe
    """
    dataframe = dataframe.copy()  # needs to return a copy
    for colname in dataframe.loc[
        :, ~dataframe.columns.isin(["Starttime", "Endtime"])
    ]:
        dataframe[colname] = dataframe[colname].astype(str)
        try:
            dataframe[colname] = dataframe[colname].astype(float)
        except:
            pass
    return dataframe
