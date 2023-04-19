from datetime import datetime, timedelta
from typing import Union
import numpy as np
import pytz

from PIconnect.AFSDK import AF
from PIconnect.config import PIConfig
from PIconnect.AFSDK import System


def to_af_time_range(
    start_time: Union[str, datetime],
    end_time: Union[str, datetime, float],
) -> AF.Time.AFTimeRange:
    """Convert a combination of start and end time to a time range.

    Both `start_time` and `end_time` can be either a :any:`datetime`
    object or a string `datetime` objects are first converted to a string,
    before being passed to :afsdk:`AF.Time.AFTimeRange
    <M_OSIsoft_AF_Time_AFTimeRange__ctor_1.htm>`. It is also possible to
    specify either end as a `datetime` object, and then specify the other
    boundary as a relative string.

    Args:
        start_time (Union[str, datetime]): start time
        end_time (Union[str, datetime, float]): end time if str or
            datetime object. Else if float will use current time.

    Returns:
        AF.Time.AFTimeRange:  Time range covered by the start and end time.
    """
    if isinstance(start_time, datetime):
        start_time = start_time.isoformat()
    if isinstance(end_time, datetime):
        end_time = end_time.isoformat()
    if isinstance(end_time, float):
        local_tz = pytz.timezone(PIConfig.DEFAULT_TIMEZONE)
        end_time = (
            datetime.utcnow()
            .replace(tzinfo=pytz.utc)
            .astimezone(local_tz)
            .isoformat()
        )

    return AF.Time.AFTimeRange(start_time, end_time)


def to_af_time(time: Union[str, datetime]) -> AF.Time.AFTime:
    """Convert a time to a AFTime value.

    Args:
        time (Union[str,datetime]): Time to convert to AFTime.

    Returns:
        :afsdk:`AF.Time.AFTime <M_OSIsoft_AF_Time_AFTime__ctor_7.htm>`: Time
            range covered by the start and end time.
    """
    if isinstance(time, datetime):
        time = time.isoformat()

    # ---NaT floats

    return AF.Time.AFTime(time)


def timestamp_to_index(timestamp: System.DateTime):
    """Convert System.DateTime to datetime in local timezone.

    Args:
        timestamp (`System.DateTime`): Timestamp in .NET format to convert to
            `datetime`.

    Returns:
        `datetime`: Datetime with the timezone info from
        :data:`PIConfig.DEFAULT_TIMEZONE
        <PIconnect.config.PIConfigContainer.DEFAULT_TIMEZONE>`.
    """
    try:  # issue of converting infite endtimes, now defaulted to timezone
        # unaware infinite timezone
        if datetime(
            timestamp.Year,
            timestamp.Month,
            timestamp.Day,
            timestamp.Hour,
            timestamp.Minute,
            timestamp.Second,
            timestamp.Millisecond * 1000,
        ) == datetime(9999, 12, 31, 23, 59, 59):
            return np.nan

        else:
            local_tz = pytz.timezone(PIConfig.DEFAULT_TIMEZONE)
            return (
                datetime(
                    timestamp.Year,
                    timestamp.Month,
                    timestamp.Day,
                    timestamp.Hour,
                    timestamp.Minute,
                    timestamp.Second,
                    timestamp.Millisecond * 1000,
                )
                .replace(tzinfo=pytz.utc)
                .astimezone(local_tz)
            )
    except:
        return np.nan


def add_timezone(timestamp):
    local_tz = pytz.timezone(PIConfig.DEFAULT_TIMEZONE)
    return timestamp.replace(tzinfo=pytz.utc).astimezone(local_tz)
