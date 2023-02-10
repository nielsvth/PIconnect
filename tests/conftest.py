"""for configuring tests for the PIAF package"""

from typing import Tuple
import pytest
import PIconnect
from datetime import datetime
from tzlocal import get_localzone_name


@pytest.fixture(scope="package")
def afdatabase() -> Tuple[str, str]:
    """Finds NuGreen database and linked server"""
    # Find AF server that contains custom "NuGreen" database
    afserver = [
        servName
        for servName in PIconnect.PIAFDatabase.servers
        if "NuGreen"
        in PIconnect.PIAFDatabase.servers[servName]["databases"].keys()
    ]
    if len(afserver) == 0:
        raise IOError(
            "The 'NuGreen database' could not be found on any of the linked servers,make sure it is configured properly from the xml file at 'https://github.com/nielsvth/PIconnect'"
        )

    server = afserver[0]
    afdatabase = "NuGreen"
    return server, afdatabase


@pytest.fixture(scope="package")
def af_connect(
    afdatabase,
) -> Tuple[PIconnect.PIAFDatabase, PIconnect.PIServer]:
    """Connects to the PIServer and AFServer for testing"""
    server, afdatabase = afdatabase

    # Set up timezone info
    PIconnect.PIConfig.DEFAULT_TIMEZONE = get_localzone_name()

    # check if default tags are present
    try:
        PIconnect.PIServer().find_tags("SINUSOID")
    except:
        raise IOError(
            f"The default'SINUSOID' tag was not found on PIServer: {PIconnect.PIServer().name}"
        )
    try:
        PIconnect.PIServer().find_tags("SINUSOIDU")
    except:
        raise IOError(
            f"The default'SINUSOIDU' tag was not found on PIServer: {PIconnect.PIServer().name}"
        )

    # created AFDatabase & EventDatabase from '.XML' files and use default PIserver
    # Every PIserver should have default SINUSOID Tag for testing purposes
    with PIconnect.PIAFDatabase(
        server=server, database=afdatabase
    ) as afdatabase, PIconnect.PIServer() as server:
        return afdatabase, server


@pytest.fixture(scope="package")
def pi_timerange() -> Tuple[datetime, datetime]:
    start_date = datetime(day=1, month=1, year=2022)
    end_date = datetime(day=10, month=1, year=2022)
    return (start_date, end_date)


@pytest.fixture(scope="package")
def af_timerange() -> Tuple[datetime, datetime]:
    start_date = datetime(day=1, month=10, year=2022)
    end_date = datetime(day=4, month=10, year=2022)
    return (start_date, end_date)


@pytest.fixture(scope="package")
def calc_timerange() -> Tuple[datetime, datetime]:
    start_date = datetime(day=1, month=10, year=2022, hour=14)
    end_date = datetime(day=1, month=10, year=2022, hour=22)
    return (start_date, end_date)
