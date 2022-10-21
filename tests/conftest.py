"""for configuring tests for the PIAF package"""

from typing import Tuple
import pytest
import PIconnect


@pytest.fixture(scope="package")
def afdatabase() -> Tuple[str, str]:
    server = "ITSBEBEWSP06182 DEV"
    afdatabase = "NuGreen"
    return server, afdatabase


@pytest.fixture(scope="package")
def af_connect(
    afdatabase,
) -> Tuple[PIconnect.PIAFDatabase, PIconnect.PIServer]:
    """Connects to the AFServer for testing"""
    server, afdatabase = afdatabase

    # Set up timezone info
    PIconnect.PIConfig.DEFAULT_TIMEZONE = "Europe/Brussels"

    # created AFDatabase & EventDatabase from '.XML' files and use default PIserver
    # Every PIserver should have default SINUSOID Tag for testing purposes
    with PIconnect.PIAFDatabase(
        server=server, database=afdatabase
    ) as afdatabase, PIconnect.PIServer() as server:

        return afdatabase, server
