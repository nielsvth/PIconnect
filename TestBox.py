# -*- coding: utf-8 -*-
"""
Created on Tue Sep 27 09:43:44 2022

@author: NVanthil
"""

import PIconnect

# Set up timezone info
PIconnect.PIConfig.DEFAULT_TIMEZONE = "Europe/Brussels"

with PIconnect.PIAFDatabase(
    server="ITSBEBEWSP06182 DEV", database="NuGreen"
) as afdatabase, PIconnect.PIServer() as server:

    taglist = server.find_tags("SINUSOID")
    tag = taglist[0]

    # print(tag)
    # print(tag.current_value())
    # print(tag.uom)
    # print(tag.interpolated_values('*-1d', '*', '1h', "'%tag%'>70"))
    # print(tag.recorded_values("2022-10-19 18:53:47", "2022-10-19 19:53:47", "'SINUSOID' >= 10"))
    # print(tag.summary("*-1d", "*", 4|8))
    # print(tag.summaries("*-1d", "*", "1h", 4|8))
    # print(tag.filtered_summaries("*-1d", "*", "1h", 4 | 8, "'%tag%' >= 70"))
    # EventWeighted instead of TimeWeighted avoids issues with return values outside of filter range
    # print(tag.filtered_summaries("*-1d", "*", "1h", 4 | 8, "'%tag%' >= 70", calculation_basis=1))

    from PIconnect.AFSDK import AF
    from PIconnect.time import to_af_time_range, timestamp_to_index

    afrange = to_af_time_range("1-10-2022 14:00", "1-10-2022 18:00")
    afinterval = AF.Time.AFTimeSpan.Parse("10m")

    # calculation for interpolated value at eacht interval incl start and end
    x = AF.Data.AFCalculation.CalculateAtIntervals(
        0, r"Abs('\\ITSBEBEPIHISCOL\SINUSOID'*-1)", afrange, afinterval
    )
    print([y.Value for y in x])

    # calculation over full range, for each recorded point incl start and end
    x = AF.Data.AFCalculation.CalculateAtRecordedValues(
        0, r"'\\ITSBEBEPIHISCOL\SINUSOID'*1", afrange
    )
    y = AF.Data.AFCalculation.CalculateAtRecordedValues(
        0, r"'\\ITSBEBEPIHISCOL\SINUSOID'*2", afrange
    )
    print([z.Value for z in x])
    print([z.Value for z in y])
    print([timestamp_to_index(y.Timestamp.UtcTime) for y in x])

    # include kind of filter in expression, also IF THEN ELSE, AND OR logic
    x = AF.Data.AFCalculation.CalculateAtRecordedValues(
        0,
        r"('\\ITSBEBEPIHISCOL\SINUSOID'>90)*'\\ITSBEBEPIHISCOL\SINUSOID'*2",
        afrange,
    )
    print([z.Value for z in x])
    print([timestamp_to_index(y.Timestamp.UtcTime) for y in x])

    # somehow does not function well
    afrange = to_af_time_range("1-10-2022 14:00", "1-10-2022 15:00")
    x = AF.Data.AFCalculation.CalculateAtRecordedValues(
        0,
        r"TagTot('\\ITSBEBEPIHISCOL\SINUSOID', '01-Oct-2022 14:00:00', '01-Oct-2022 22:00:00')",
        afrange,
    )
    print([y.Value for y in x])
    print([timestamp_to_index(y.Timestamp.UtcTime) for y in x])

    afrange = to_af_time_range("1-10-2022 14:00", "1-10-2022 22:00")
    x = AF.Data.AFCalculation.CalculateAtRecordedValues(
        0,
        r"IF ('\\ITSBEBEPIHISCOL\SINUSOID' > 70) THEN (Abs('\\ITSBEBEPIHISCOL\SINUSOID')) ELSE (0)",
        afrange,
    )
    print([y.Value for y in x])
    print([timestamp_to_index(y.Timestamp.UtcTime) for y in x])

    afrange = to_af_time_range("1-10-2022 14:00", "1-10-2022 22:00")
    x = AF.Data.AFCalculation.CalculateAtIntervals(
        0,
        r"TagTot('\\ITSBEBEPIHISCOL\SINUSOID', '01-Oct-2022 14:00:00', '01-Oct-2022 22:00:00')",
        afrange,
        afinterval,
    )
    print([y.Value for y in x])
    print([timestamp_to_index(y.Timestamp.UtcTime) for y in x])

    ##INTEGRAAL
    afrange = to_af_time_range("*", "*")
    afinterval = AF.Time.AFTimeSpan.Parse("1s")
    x = AF.Data.AFCalculation.CalculateAtIntervals(
        0,
        r"TagTot('\\ITSBEBEPIHISCOL\SINUSOID', '01-Oct-2022 14:00:00', '01-Oct-2022 15:00:00')",
        afrange,
        afinterval,
    )
    print([y.Value for y in x])
    print([timestamp_to_index(y.Timestamp.UtcTime) for y in x])

    # Filtered count = duration in 's' above this threshold
    x = tag.filtered_summaries(
        "*-1d", "*", "2h", 128, "'%tag%' >= 50", calculation_basis=0
    )
    print(x)

    x = tag.interpolated_value("2022-10-20 18:54:38")
    print(x)
