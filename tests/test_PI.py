"""Unit Tests for PIAF.py Module""" ""

import PIconnect
import datetime

# Set up timezone info
PIconnect.PIConfig.DEFAULT_TIMEZONE = "Europe/Brussels"


def test_connection():
    """Test to check for connected servers"""
    assert (
        len(PIconnect.PIServer.servers) >= 1
    ), "Should be larger or equal to 1"


def test_find_tags(af_connect):
    """Test to find Tags on AFDatabase"""
    # create AFDatabase & EventDatabase from '.XML' files and use default PIserver
    # SINUSOID is a default tag available on any PIServer
    taglist = server.find_tags("SINUSOID")
    assert len(taglist) == 1, "Should be 1"
    tag = taglist[0]
    assert tag.name == "SINUSOID", "should be 'SINUSOID'"
    assert len(tag.raw_attributes) == 58, "should be 58"
