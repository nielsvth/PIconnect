from PIconnect.AFSDK import AF

import clr
# depending on version of pythonnet, change class inheritance
if int(clr.__version__.split('.')[0]) >= 3:
    x = object
    y = object
else:
    import enum
    x = enum.IntEnum
    y = enum.IntFlag


class UpdateMode(x):
    """Indicates how to treat duplicate values in the archive, when supported
    by the Data Reference.

    Detailed information is available at
    :afsdk:`AF.Data.AFUpdateOption <T_OSIsoft_AF_Data_AFUpdateOption.htm>`
    https://docs.aveva.com/en-US/bundle/af-sdk/page/html/T_OSIsoft_AF_Data_AFUpdateOption.htm
    """

    #: Add the value to the archive.
    #: If any values exist at the same time, will overwrite one of them and
    # set its Substituted flag.
    Replace = AF.Data.AFUpdateOption.Replace
    #: Add the value to the archive. Any existing values at the same time are
    # not overwritten.
    Insert = AF.Data.AFUpdateOption.Insert
    #: Add the value to the archive only if no value exists at the same time.
    #: If a value already exists for that time, the passed value is ignored.
    NoReplace = AF.Data.AFUpdateOption.NoReplace
    #: Replace an existing value in the archive at the specified time.
    #: If no existing value is found, the passed value is ignored.
    ReplaceOnly = AF.Data.AFUpdateOption.ReplaceOnly
    #: Add the value to the archive without compression.
    #: If this value is written to the snapshot, the previous snapshot value
    # will be written to the archive,
    #: without regard to compression settings.
    #: Note that if a subsequent snapshot value is written without the
    # InsertNoCompression option,
    #: the value added with the InsertNoCompression option is still subject to
    # compression.
    InsertNoCompression = AF.Data.AFUpdateOption.InsertNoCompression
    #: Remove the value from the archive if a value exists at the passed time.
    Remove = AF.Data.AFUpdateOption.Remove


class BufferMode(x):
    """Indicates buffering option in updating values, when supported by the
    Data Reference.

    Detailed information is available at
    :afsdk:`AF.Data.AFBufferOption <T_OSIsoft_AF_Data_AFBufferOption.htm>`
    https://docs.aveva.com/en-US/bundle/af-sdk/page/html/T_OSIsoft_AF_Data_AFBufferOption.htm
    """

    #: Updating data reference values without buffer.
    DoNotBuffer = AF.Data.AFBufferOption.DoNotBuffer
    #: Try updating data reference values with buffer.
    #: If fails (e.g. data reference AFDataMethods does not support Buffering,
    # or its Buffering system is not available),
    #: then try updating directly without buffer.
    BufferIfPossible = AF.Data.AFBufferOption.BufferIfPossible
    # Updating data reference values with buffer.
    Buffer = AF.Data.AFBufferOption.Buffer


class AuthenticationMode(x):
    """AuthenticationMode indicates how a user authenticates to a PI Server

    Detailed information is available at
    :afsdk:
    `AF.PI.PIAuthenticationMode <T_OSIsoft_AF_PI_PIAuthenticationMode.htm>`.
    https://docs.aveva.com/en-US/bundle/af-sdk/page/html/T_OSIsoft_AF_PI_PIAuthenticationMode.htm
    """

    #: Use Windows authentication when making a connection
    WindowsAuthentication = AF.PI.PIAuthenticationMode.WindowsAuthentication
    #: Use the PI User authentication mode when making a connection
    PIUserAuthentication = AF.PI.PIAuthenticationMode.PIUserAuthentication


class CalculationBasis(x):
    """CalculationBasis indicates how values should be weighted over a time
    range

    Detailed information is available at
    :afsdk:
    `AF.Data.AFCalculationBasis <T_OSIsoft_AF_Data_AFCalculationBasis.htm>`.
    https://docs.aveva.com/bundle/af-sdk/page/html/T_OSIsoft_AF_Data_AFCalculationBasis.htm
    """

    #: Each event is weighted according to the time over which it applies.
    TimeWeighted = AF.Data.AFCalculationBasis.TimeWeighted
    #: Each event is weighted equally.
    EventWeighted = AF.Data.AFCalculationBasis.EventWeighted
    #: Each event is time weighted, but interpolation is always done as if it
    # is continous data.
    TimeWeightedContinuous = AF.Data.AFCalculationBasis.TimeWeightedContinuous
    #: Each event is time weighted, but interpolation is always done as if it
    # is discrete, stepped, data.
    TimeWeightedDiscrete = AF.Data.AFCalculationBasis.TimeWeightedDiscrete
    #: Each event is weighted equally, except data at the end of the interval
    # is excluded.
    EventWeightedExcludeMostRecentEvent = (
        AF.Data.AFCalculationBasis.EventWeightedExcludeMostRecentEvent
    )
    #: Each event is weighted equally, except data at the beginning of the
    # interval is excluded.
    EventWeightedExcludeEarliestEvent = (
        AF.Data.AFCalculationBasis.EventWeightedExcludeEarliestEvent
    )
    #: Each event is weighted equally, data at both boundaries of the interval
    # are explicitly included.
    EventWeightedIncludeBothEnds = (
        AF.Data.AFCalculationBasis.EventWeightedIncludeBothEnds
    )


class ExpressionSampleType(x):
    """ExpressionSampleType indicates how expressions are evaluated over a
    time range.

    Detailed information is available at
    :afsdk:`AF.Data.AFSampleType <T_OSIsoft_AF_Data_AFSampleType.htm>`.
    https://docs.aveva.com/bundle/af-sdk/page/html/T_OSIsoft_AF_Data_AFSampleType.htm
    """

    #: The expression is evaluated at each archive event.
    ExpressionRecordedValues = AF.Data.AFSampleType.ExpressionRecordedValues
    #: The expression is evaluated at a sampling interval, passed as a
    # separate argument.
    Interval = AF.Data.AFSampleType.Interval


class RetrievalMode(x):
    """RetrievalMode indicates which recorded value should be returned

    Detailed information is available at
    :afsdk:`AF.Data.AFRetrievalMode <T_OSIsoft_AF_Data_AFRetrievalMode.htm>`.
    https://docs.aveva.com/bundle/af-sdk/page/html/T_OSIsoft_AF_Data_AFRetrievalMode.htm
    """

    #: Autmatic detection
    Auto = AF.Data.AFRetrievalMode.Auto
    #: At the exact time if available, else the first before the requested
    # time
    AtOrBefore = AF.Data.AFRetrievalMode.AtOrBefore
    #: The first before the requested time
    Before = AF.Data.AFRetrievalMode.Before
    #: At the exact time if available, else the first after the requested time
    AtOrAfter = AF.Data.AFRetrievalMode.AtOrAfter
    #: The first after the requested time
    After = AF.Data.AFRetrievalMode.After
    #: At the exact time if available, else return an error
    Exact = AF.Data.AFRetrievalMode.Exact


class SummaryType(y):
    """SummaryType indicates which types of summary should be calculated.

    Based on :class:`enum.IntEnum` in Python 3.5 or earlier. `SummaryType`'s
    can be or'ed together. Python 3.6 or higher returns a new `IntFlag`, while
    in previous versions it will be casted down to `int`.

    # Returns minimum and maximum
    >> SummaryType.Minimum | SummaryType.Maximum

    Detailed information is available at
    :afsdk:`AF.Data.AFSummaryTypes <T_OSIsoft_AF_Data_AFSummaryTypes.htm>`.
    https://docs.aveva.com/bundle/af-sdk/page/html/T_OSIsoft_AF_Data_AFSummaryTypes.htm
    """

    #: A total over the time span
    Total = AF.Data.AFSummaryTypes.Total
    #: Average value over the time span
    Average = AF.Data.AFSummaryTypes.Average
    #: The minimum value in the time span
    Minimum = AF.Data.AFSummaryTypes.Minimum
    #: The maximum value in the time span
    Maximum = AF.Data.AFSummaryTypes.Maximum
    #: The range of the values (max-min) in the time span
    Range = AF.Data.AFSummaryTypes.Range
    #: The sample standard deviation of the values over the time span
    StdDev = AF.Data.AFSummaryTypes.StdDev
    #: The population standard deviation of the values over the time span
    PopulationStdDev = AF.Data.AFSummaryTypes.PopulationStdDev
    #: The sum of the event count (when the calculation is event weighted).
    # The sum of the event time duration (when the calculation is time
    # weighted.)
    Count = AF.Data.AFSummaryTypes.Count
    #: The percentage of the data with a good value over the time range.
    # Based on time for time weighted calculations, based on event count for
    # event weigthed calculations.
    PercentGood = AF.Data.AFSummaryTypes.PercentGood
    #: The total over the time span, with the unit of measurement that's
    # associated with the input (or no units if not defined for the input).
    TotalWithUOM = AF.Data.AFSummaryTypes.TotalWithUOM
    #: A convenience to retrieve all summary types
    All = AF.Data.AFSummaryTypes.All
    #: A convenience to retrieve all summary types for non-numeric data
    AllForNonNumeric = AF.Data.AFSummaryTypes.AllForNonNumeric


class TimestampCalculation(x):
    """
    TimestampCalculation defines the timestamp returned for a given summary
    calculation

    Detailed information is available at
    :afsdk:`AF.Data.AFTimestampCalculation
    <T_OSIsoft_AF_Data_AFTimestampCalculation.htm>`.
    https://docs.aveva.com/bundle/af-sdk/page/html/T_OSIsoft_AF_Data_AFTimestampCalculation.htm
    """

    #: The timestamp is the event time of the minimum or maximum for those
    # summaries or the beginning of the interval otherwise.
    Auto = AF.Data.AFTimestampCalculation.Auto
    #: The timestamp is always the beginning of the interval.
    EarliestTime = AF.Data.AFTimestampCalculation.EarliestTime
    #: The timestamp is always the end of the interval.
    MostRecentTime = AF.Data.AFTimestampCalculation.MostRecentTime


class EventFrameSearchMode(x):
    """EventFrameSearchMode

    EventFrameSearchMode defines the interpretation and direction from the
    start time when searching for event frames.

    Detailed information is available at https://techsupport.osisoft.com/Documentation/PI-AF-SDK/html/T_OSIsoft_AF_EventFrame_AFEventFrameSearchMode.htm # noqa
    including a graphical display of event frames that are returned for a
    given search mode.
    AF.EventFrame.AFEventFrameSearchMode
    https://docs.aveva.com/bundle/af-sdk/page/html/T_OSIsoft_AF_EventFrame_AFEventFrameSearchMode.htm
    """

    #: Backward from start time, also known as starting before
    BackwardFromStartTime = (
        AF.EventFrame.AFEventFrameSearchMode.BackwardFromStartTime
    )
    #: Forward from start time, also known as starting after
    ForwardFromStartTime = (
        AF.EventFrame.AFEventFrameSearchMode.ForwardFromStartTime
    )
    #: Backward from end time, also known as ending before
    BackwardFromEndTime = (
        AF.EventFrame.AFEventFrameSearchMode.BackwardFromEndTime
    )
    #: Forward from end time, also known as ending after
    ForwardFromEndTime = (
        AF.EventFrame.AFEventFrameSearchMode.ForwardFromEndTime
    )
    #: Backward in progress, also known as starting before and in progress
    BackwardInProgress = (
        AF.EventFrame.AFEventFrameSearchMode.BackwardInProgress
    )
    #: Forward in progress, also known as starting after and in progress
    ForwardInProgress = AF.EventFrame.AFEventFrameSearchMode.ForwardInProgress


class SearchMode (x):
    """AF.Asset.AFSearchMode
    https://docs.aveva.com/bundle/af-sdk/page/html/T_OSIsoft_AF_Asset_AFSearchMode.htm
    """

    # Includes all objects whose start time is within the specified range.
    # Also known as "Starting Between".
    StartInclusive = AF.Asset.AFSearchMode.StartInclusive
    # Includes all objects whose end time is within the specified range.
    # Also known as "Ending Between".
    EndInclusive = AF.Asset.AFSearchMode.EndInclusive
    # Includes all objects whose start and end time are within the specified range.
    # Also know as "Entirely Between".
    Inclusive = AF.Asset.AFSearchMode.Inclusive
    # Includes all objects whose time range overlaps with the specified range at any point in time.
    # Also known as "Active Between".
    Overlapped = AF.Asset.AFSearchMode.Overlapped
    # Includes all objects whose start time is within the specified range and end time is MaxValue.
    # Also known as "Starting Between and In Progress".
    InProgress = AF.Asset.AFSearchMode.InProgress


class SortField(x):
    """AF.AFSortField
    https://docs.aveva.com/bundle/af-sdk/page/html/T_OSIsoft_AF_AFSortField.htm
    """

    # The returned collection is sorted on the ID field.
    ID = AF.AFSortField.ID
    # The returned collection is sorted on the ID field.
    Name = AF.AFSortField.Name
    # The returned collection is sorted on the Type field.
    Type = AF.AFSortField.Type
    # The returned collection is sorted on the StartTime field.
    StartTime = AF.AFSortField.StartTime
    # The returned collection is sorted on the EndTime field.
    EndTime = AF.AFSortField.EndTime


class SortOrder(x):
    """AF.AFSortOrder
    https://docs.aveva.com/bundle/af-sdk/page/html/T_OSIsoft_AF_AFSortOrder.htm
    """

    # The returned collection is sorted in Ascending order.
    Ascending = AF.AFSortOrder.Ascending
    # The returned collection is sorted in Descending order.
    Descending = AF.AFSortOrder.Descending


class BoundaryType(x):
    """AF.Data.AFBoundaryType
    https://docs.aveva.com/bundle/af-sdk/page/html/T_OSIsoft_AF_Data_AFBoundaryType.htm
    """

    # Specifies to return the recorded values on the inside of the requested time range as the first and last values.
    Inside = AF.Data.AFBoundaryType.Inside
    # Specifies to return the recorded values on the outside of the requested time range as the first and last values.
    Outside = AF.Data.AFBoundaryType.Outside
    # Specifies to create an interpolated value at the end points of the requested time range
    # if a recorded value does not exist at that time.
    Interpolated = AF.Data.AFBoundaryType.Interpolated


class SearchField(x):
    """AF.AFSearchField
    https://docs.aveva.com/bundle/af-sdk/page/html/T_OSIsoft_AF_AFSearchField.htm
    """

    # The object's Name property is searched.
    Name = AF.AFSearchField.Name
    # The object's Description property is searched.
    Description = AF.AFSearchField.Description
    # The object's Categories collection property is searched by name.
    # If only searching categories and the query for the search is null or empty string,
    # then only items without a category are returned.
    Categories = AF.AFSearchField.Categories
    # The object's Template property is searched.
    # If only searching the template and the query for the search is null or empty string,
    # then only items without a template are returned.
    Template = AF.AFSearchField.Template
    # The object's Department property is searched.
    Department = AF.AFSearchField.Department
    # 	The object's EMail property is searched for objects defined by Active Directory.
    EMail = AF.AFSearchField.EMail


class PIPointType(x):
    """AF.PI.PIPointType
    https://docs.aveva.com/bundle/af-sdk/page/html/T_OSIsoft_AF_PI_PIPointType.htm
    """

    # The PIPoint's type is not defined.
    Null = AF.PI.PIPointType.Null
    # The PIPoint is numeric and is restricted to integer values.
    # Its normal values are archived as 16-bit integers.
    Int16 = AF.PI.PIPointType.Int16
    # The PIPoint is numeric and is restricted to integer values.
    # Its normal values are archived as 32-bit integers.
    Int32 = AF.PI.PIPointType.Int32
    # The PIPoint is numeric; its normal values are archived in 16-bit scaled fixed-point format.
    Float16 = AF.PI.PIPointType.Float16
    # The PIPoint is numeric; its normal values are archived in single-precision floating-point format.
    Float32 = AF.PI.PIPointType.Float32
    # The PIPoint is numeric; its normal values are archived in double-precision floating-point format
    Float64 = AF.PI.PIPointType.Float64
    # The PIPoint's normal values are members of a digital state set.
    Digital = AF.PI.PIPointType.Digital
    # The PIPoint's normal values are a time stamp.
    Timestamp = AF.PI.PIPointType.Timestamp
    # The PIPoint's normal values are character strings.
    String = AF.PI.PIPointType.String
    # The PIPoint's normal values are BLOBs (binary large objects).
    Blob = AF.PI.PIPointType.Blob
