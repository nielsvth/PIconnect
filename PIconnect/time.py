from datetime import datetime, timedelta
import numpy as np
import pytz

from JanssenPI.AFSDK import AF
from JanssenPI.config import PIConfig


def to_af_time_range(start_time, end_time):
    """Convert a combination of start and end time to a time range.

    Both `start_time` and `end_time` can be either a :any:`datetime.datetime` object or a string.
    `datetime` objects are first converted to a string, before being passed to
    :afsdk:`AF.Time.AFTimeRange <M_OSIsoft_AF_Time_AFTimeRange__ctor_1.htm>`. It is also
    possible to specify either end as a `datetime` object, and then specify the other
    boundary as a relative string.

    Args:
        start_time (str | datetime): Start time of the time range.
        end_time (str | datetime): End time of the time range.

    Returns:
        :afsdk:`AF.Time.AFTimeRange <M_OSIsoft_AF_Time_AFTimeRange__ctor_1.htm>`: Time range covered by the start and end time.
    """
    if isinstance(start_time, datetime):
        start_time = start_time.isoformat()
    if isinstance(end_time, datetime):
        end_time = end_time.isoformat()
    if isinstance(end_time, float):
        local_tz = pytz.timezone(PIConfig.DEFAULT_TIMEZONE)
        end_time = datetime.utcnow().replace(tzinfo=pytz.utc).astimezone(local_tz).isoformat()

    return AF.Time.AFTimeRange(start_time, end_time)


def to_af_time(time):
    """Convert a time to a AFTime value.

    Args:
        time (str | datetime): Time to convert to AFTime.

    Returns:
        :afsdk:`AF.Time.AFTime <M_OSIsoft_AF_Time_AFTime__ctor_7.htm>`: Time range covered by the start and end time.
    """
    if isinstance(time, datetime):
        time = time.isoformat()

    #---NaT floats


    return AF.Time.AFTime(time)


def timestamp_to_index(timestamp):
    """Convert AFTime object to datetime in local timezone.

    Args:
        timestamp (`System.DateTime`): Timestamp in .NET format to convert to `datetime`.

    Returns:
        `datetime`: Datetime with the timezone info from :data:`PIConfig.DEFAULT_TIMEZONE <JanssenPI.config.PIConfigContainer.DEFAULT_TIMEZONE>`.
    """ 
    try: #issue of converting infite endtimes, now defaulted to timezone unaware infinite timezone
        if datetime(timestamp.Year,
                timestamp.Month,
                timestamp.Day,
                timestamp.Hour,
                timestamp.Minute,
                timestamp.Second,
                timestamp.Millisecond * 1000)== datetime(9999, 12, 31, 23, 59, 59):
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
                    timestamp.Millisecond * 1000)
                .replace(tzinfo=pytz.utc)
                .astimezone(local_tz))
    except:
        return np.nan

def add_timezone(timestamp):
    local_tz = pytz.timezone(PIConfig.DEFAULT_TIMEZONE)
    return timestamp.replace(tzinfo=pytz.utc).astimezone(local_tz)