""" JanssenPI
    Connector to the OSISoft PI and PI-AF databases.
"""
# pragma pylint: disable=unused-import
from JanssenPI.AFSDK import AF, AF_SDK_VERSION
from JanssenPI.config import PIConfig
from JanssenPI.PI import PIServer
from JanssenPI.PIAF import PIAFDatabase

# pragma pylint: enable=unused-import

__version__ = "0.9.1"
__sdk_version = tuple(int(x) for x in AF.PISystems().Version.split("."))