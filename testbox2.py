import PIconnect
from datetime import datetime
from tzlocal import get_localzone_name

# set up timezone. Pick timezone from
# https://gist.github.com/heyalexej/8bf688fd67d7199be4a1682b3eec7568
PIconnect.PIConfig.DEFAULT_TIMEZONE = get_localzone_name()

# Find AF server that contains custom "NuGreen" database
# See instructions above on how to set up the "NuGreen" database for testing purposes
afserver = [
    servName
    for servName in PIconnect.PIAFDatabase.servers
    if "NuGreen"
    in PIconnect.PIAFDatabase.servers[servName]["databases"].keys()
][0]


# Initiate connection to PI data server & PI AF database of interest by
# defining their name
with PIconnect.PIAFDatabase(
    server=afserver, database="NuGreen"
) as afdatabase, PIconnect.PIServer(
    server=list(PIconnect.PIServer.servers.keys())[1]
) as server:

    # Find 'SINUSOID' tag
    tag = server.find_tags("SINUSOID")[0]

    # calculation on recorded values
    # for overview of expression syntax: https://docs.aveva.com/bundle/pi-server-af-analytics/page/1021946.html
    calc1 = PIconnect.calc.calc_recorded(
        "1-10-2022 14:00",
        "1-10-2022 22:00",
        r"IF ('\\ITSBEBEPIHISCOL\SINUSOID' > 70) THEN (Abs('\\ITSBEBEPIHISCOL\SINUSOID')) ELSE (0)",
    )

    # calculation on interpolated values
    # returns totalized value per minute, do *1440 to get per day
    calc2 = PIconnect.calc.calc_interpolated(
        "1-10-2022 14:00",
        "1-10-2022 14:00",
        "1h",
        r"TagTot('\\ITSBEBEPIHISCOL\SINUSOID', '01-Oct-2022 14:00:00', '03-Oct-2022 14:00:00')",
    )

    taglist = server.find_tags("SINUSOID")
    tag = taglist[0]
    starttime = datetime(day=1, month=1, year=2022)

    PIconnect.PIConfig.DEFAULT_TIMEZONE = get_localzone_name()
    # PIconnect.PIConfig.DEFAULT_TIMEZONE = 'America/New_York'

    x = tag.interpolated_value(time="1-1-2022 00:00:00")[1]
    x = tag.interpolated_value(time="31-12-2021 18:00:00")[1]
    x = tag.interpolated_value(time="1-1-2022 06:00:00")[1]

    from datetime import datetime
    from tzlocal import get_localzone_name
    from pytz import timezone

    tag.interpolated_value(time=datetime(day=1, month=1, year=2022))
    tag.interpolated_value(
        time=datetime(
            day=1, month=1, year=2022, tzinfo=timezone("Europe/Brussels")
        )
    )

    datetime(
        day=1, month=1, year=2022, tzinfo=timezone("Europe/Brussels")
    ).strftime("%Y-%m-%d %H:%M:%S %Z%z")

    datetime(day=1, month=1, year=2022).astimezone(timezone("Europe/Brussels"))
    datetime(day=1, month=1, year=2022).astimezone(
        timezone("America/New_York")
    )

    timezone("America/New_York").localize(datetime(day=1, month=1, year=2022))
    timezone("Europe/Brussels").localize(datetime(day=1, month=1, year=2022))

    tag.interpolated_value(
        time=datetime(day=1, month=1, year=2022).astimezone(
            timezone("Europe/Brussels")
        )
    )
    tag.interpolated_value(
        time=datetime(day=1, month=1, year=2022).astimezone(
            timezone("America/New_York")
        )
    )
