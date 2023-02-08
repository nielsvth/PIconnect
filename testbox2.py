import PIconnect
from datetime import datetime

# set up timezone. Pick timezone from
# https://gist.github.com/heyalexej/8bf688fd67d7199be4a1682b3eec7568
PIconnect.PIConfig.DEFAULT_TIMEZONE = "Europe/Brussels"

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
) as afdatabase, PIconnect.PIServer(server=dataservers[1]) as server:

    # Attributes can be available for both Assets and Events

    # Returns list of Assets that meets the query criteria
    # Here a query is executed for an Asset with name 'P-560'
    assetlist = afdatabase.find_assets(query="P-560")

    # Select the first Asset from the Asset list
    asset = assetlist[0]

    # select first attribute
    attribute = asset.attributes[0]

    print(attribute.source_type)
    print(attribute.path)
    print(attribute.description)
    print(attribute.current_value())

    # select first attribute that has a Tag/PIpoint as a source
    attribute = [
        attribute
        for attribute in asset.attributes
        if attribute.source_type == "PI Point"
    ][0]

    print(attribute.source_type)
    print(attribute.path)
    print(attribute.description)
    print(attribute.pipoint)
    print(attribute.current_value())
