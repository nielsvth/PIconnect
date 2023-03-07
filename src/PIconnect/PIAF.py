""" PIAF
    Core containers for connections to the PI Asset Framework.
"""
# pragma pylint: disable=unused-import, redefined-builtin
from __future__ import (
    absolute_import,
    division,
    print_function,
    unicode_literals,
)

from builtins import (
    ascii,
    bytes,
    chr,
    dict,
    filter,
    hex,
    input,
    int,
    list,
    map,
    next,
    object,
    oct,
    open,
    pow,
    range,
    round,
    str,
    super,
    zip,
)
from typing import Dict, List, Union

import clr
import pandas as pd
import numpy as np
import System

from collections import UserList

try:
    from __builtin__ import str as BuiltinStr
except ImportError:
    BuiltinStr = str
# pragma pylint: enable=unused-import, redefined-builtin
from warnings import warn

from PIconnect._utils import classproperty
from PIconnect.AFSDK import AF
from PIconnect.calc import calc_summary
from PIconnect.time import to_af_time
from PIconnect.PIConsts import (
    EventFrameSearchMode,
    SearchMode,
    SortField,
    SortOrder,
    SummaryType,
    CalculationBasis,
    TimestampCalculation,
    BoundaryType,
    ExpressionSampleType,
    SearchField,
)
from PIconnect.time import timestamp_to_index, add_timezone
from PIconnect.config import PIConfig
from PIconnect.PI import (
    PIServer,
    Tag,
    generate_pipointlist,
    convert_to_TagList,
)

from pytz import timezone, utc
from datetime import datetime, timedelta

clr.AddReference("System.Collections")
from System.Collections.Generic import List as C_List  # noqa: E402
from System import Exception as dotNetException  # type: ignore # noqa: E402

_NOTHING = object()


# TODO: This appears to need some work. E.g. Validate method. i'm not
# convinced the repr method will work.
class EventList(UserList):
    """Container for EventList object"""

    def __init__(self, data):
        self.data = data  # list of Events
        # validation step---

    def __repr__(self):
        return str([event for event in self.data])

    def __str__(self):
        return str([event for event in self.data])

    # Methods
    def to_set(self):
        """Return eventlist as set"""
        return set(self.data)

    def get_event_hierarchy(self, depth: int = 10) -> pd.DataFrame:
        """Return EventHierarchy down to the specified depth

        Args:
            depth (int, optional): depth to return to. Defaults to 10.

        Returns:
            pd.DataFrame: Dataframe of event hierarchy.
        """
        # https://docs.osisoft.com/bundle/af-sdk/page/html/T_OSIsoft_AF_AFNamedCollectionList_1.htm
        afcontainer = AF.AFNamedCollectionList[
            AF.EventFrame.AFEventFrame
        ]()  # empty container

        # option here to avoid redundancy and increase performance by checking if event is not a subevent
        for event in self.data:
            try:
                afcontainer.Add(event.af_eventframe)
            except:
                raise ("Failed to process event {}".format(event))

        df_events = pd.DataFrame(
            columns=[
                "Event",
                "Path",
                "Name",
                "Level",
                "Template",
                "Starttime",
                "Endtime",
            ]
        )

        if len(afcontainer) > 0:
            df_procedures = pd.DataFrame(
                [(y, y.GetPath()) for y in afcontainer],
                columns=["Event", "Path"],
            )

            print(
                "Fetching hierarchy data for {} Event(s)...".format(
                    len(afcontainer)
                )
            )
            # https://docs.osisoft.com/bundle/af-sdk/page/html/M_OSIsoft_AF_EventFrame_AFEventFrame_LoadEventFramesToDepth.htm
            event_depth = AF.EventFrame.AFEventFrame.LoadEventFramesToDepth(
                afcontainer, False, depth, 1000000
            )

            if len(event_depth) > 0:
                df_events = pd.DataFrame(
                    [(y, y.GetPath()) for y in event_depth],
                    columns=["Event", "Path"],
                )

            # concatenate procedures and child event frames
            df_events = pd.concat(
                [df_procedures, df_events], ignore_index=True
            )

            df_events["Event"] = df_events["Event"].apply(lambda x: Event(x))
            df_events["Name"] = df_events["Event"].apply(
                lambda x: x.name if x else np.nan
            )
            df_events["Template"] = df_events["Event"].apply(
                lambda x: x.template_name if x.af_template else np.nan
            )
            df_events["Level"] = (
                df_events["Path"].str.count(r"\\").apply(lambda x: x - 4)
            )
            df_events["Starttime"] = df_events["Event"].apply(
                lambda x: x.starttime if x else np.nan
            )
            df_events["Endtime"] = df_events["Event"].apply(
                lambda x: x.endtime if x else np.nan
            )

        return df_events.drop_duplicates("Path")


class Attribute:
    """container for Attribute object"""

    def __init__(self, attribute):
        self.attribute = attribute

    def __repr__(self):
        if self.attribute.DataReferencePlugIn:
            return f"Attribute: {self.attribute.Name} [source: {self.attribute.DataReferencePlugIn.Name}]"
        else:
            return f"Attribute: {self.attribute.Name} [source: None]"

    def __str__(self):
        if self.attribute.DataReferencePlugIn:
            return f"Attribute: {self.attribute.Name} [source: {self.attribute.DataReferencePlugIn.Name}]"
        else:
            return f"Attribute: {self.attribute.Name} [source: None]"

    # properties
    @property
    def name(self):
        """Return name of Attribute"""
        return self.attribute.Name

    @property
    def path(self):
        """Return Path"""
        return self.attribute.GetPath()

    @property
    def af_attribute(self):
        """Return AFAttribute Object"""
        return self.attribute

    @property
    def source_type(self):
        """Return name of Attribute's data reference"""
        if self.attribute.DataReference:
            return self.attribute.DataReference.Name
        else:
            return None

    @property
    def pipoint(self):
        """Return Tag object, if exists"""
        try:
            return Tag(self.attribute.PIPoint)
        except:
            return None

    @property
    def pisystem_name(self):
        """Return PISystem name"""
        return self.attribute.PISystem.Name

    @property
    def database_name(self):
        """Return Database name"""
        return self.attribute.Database.Name

    @property
    def database(self):
        """Return PIAFDatabase object"""
        return PIAFDatabase(
            server=self.pisystem_name, database=self.database_name
        )

    @property
    def description(self):
        """Return description for Attribute"""
        return self.attribute.Description

    @property
    def uom(self):
        """Return displayed Unit of Measurement (uom) for Attribute"""
        return self.attribute.DisplayUOM

    @property
    def parent(self):
        """Returns the Element that owns this Attributes (Asset or Event)"""
        # https://docs.osisoft.com/bundle/af-sdk/page/html/T_OSIsoft_AF_AFIdentity.htm
        id = self.attribute.Element.Identity
        if id == 45:
            return Asset(self.attribute.Element)
        elif id == 87:
            return Event(self.attribute.Element)

    @property
    def template_name(self):
        """Return template name for Attribute"""
        return self.attribute.Template.Name

    @property
    def af_template(self):
        """Return AF_template for Attribute"""
        return self.attribute.Template

    @property
    def type(self):
        """Return datatype of Attribute"""
        return self.attribute.Type.Name

    # Methods
    def current_value(self):
        """Return current value for Attribute"""
        result = self.attribute.GetValue().Value
        if type(result) == System.DateTime:
            result = timestamp_to_index(result)
        return result


class Asset:
    """Container for Asset object

    additional methods can be found here:
    https://docs.osisoft.com/bundle/af-sdk/page/html/T_OSIsoft_AF_Asset_AFElement.htm # noqa
    """

    def __init__(self, asset):
        self.asset = asset

    def __repr__(self):
        return "Asset:" + self.asset.GetPath()

    def __str__(self):
        return "Asset:" + self.asset.GetPath()

    # Properties
    @property
    def name(self):
        """Return name of Asset"""
        return self.asset.Name

    @property
    def path(self):
        """Return path"""
        return self.asset.GetPath()

    @property
    def pisystem_name(self):
        """Return PISystem name"""
        return self.asset.PISystem.Name

    @property
    def database_name(self):
        """Return database name"""
        return self.asset.Database.Name

    @property
    def database(self):
        """Return PIAFDatabase object"""
        return PIAFDatabase(
            server=self.pisystem_name, database=self.database_name
        )

    @property
    def af_asset(self):
        """Return AFAsset object"""
        return self.asset

    @property
    def af_template(self):
        """Return AFTemplate"""
        return self.asset.Template

    @property
    def template_name(self):
        """Return template name"""
        if self.asset.Template:
            return self.asset.Template.Name
        else:
            return None

    @property
    def attributes(self):
        """'Return list of attributes for Asset"""
        return [Attribute(attribute) for attribute in self.asset.Attributes]

    @property
    def af_attributes(self):
        """'Return list of AFAttributes for Asset"""
        return [attribute for attribute in self.asset.Attributes]

    @property
    def children(self):
        """Return List of children for Asset"""
        out = None
        if self.asset.HasChildren:
            out = [Asset(asset) for asset in self.asset.Elements]
        return out

    @property
    def parent(self):
        """Return parent Asset for Asset"""
        out = self.asset.Parent
        if out is not None:
            out = Asset(out)
        return out

    @property
    def description(self):
        """Return description for Asset"""
        return self.asset.Description

    @property
    def top_asset(self):
        """Return top-level Asset name"""
        return self.path.strip("\\").split("\\")[2]

    # methods
    def get_attribute_values(
        self,
        attribute_names_list: Union[None, List[AF.Asset.AFAttribute]] = None,
    ) -> Dict[str, float]:
        """Get attribute values for specified attributes

        Args:
            attribute_names_list (Union[None, List[AF.Asset.AFAttribute]],
                optional): List of Attributes to query. If None, will do all
                attributes within the Event. Defaults to None.

        Returns:
            Dict[str, float]: attribute Name: attribute value
        """
        if attribute_names_list is None:
            attribute_names_list = [
                asset.Name for asset in self.asset.Attributes
            ]

        attribute_dct = {
            attribute.Name: attribute.GetValue().Value
            for attribute in self.asset.Attributes
            if (attribute.Name in attribute_names_list)
            or (attribute in attribute_names_list)
        }

        return attribute_dct

    def get_events(
        self,
        query: str = None,
        starttime: Union[str, datetime] = None,
        endtime: Union[str, datetime] = "*",
        template_name: str = None,
        start_index: int = 0,
        max_count: int = 1000000,
        search_mode: SearchMode = SearchMode.INCLUSIVE,
        search_full_hierarchy: bool = True,
        sortField: SortField = SortField.STARTTIME,
        sortOrder: SortOrder = SortOrder.ASCENDING,
    ) -> EventList:
        """Return EventList of Events on Asset within specified time period
        that meets the query criteria

        Args:
            query (str, optional): string to query by. Defaults to None.
            start_time (Union[str, datetime], optional): Time to
                start search at. Defaults to None.
            end_time (Union[str, datetime], optional): Time to end
                search at. Defaults to "*".
            template_name (str, optional): Template to search.
                Defaults to None.
            start_index (int, optional): Start Index. Defaults to 0.
            max_count (int, optional): Max Count. Defaults to 1000000.
            search_mode (SearchMode, optional): Search Mode.
                Defaults to SearchMode.INCLUSIVE.
            search_full_hierarchy (bool, optional): Boolean for whether to
                search full hierarchy or not. Defaults to True.
            sortField (SortField, optional): Sorting field.
                Defaults to SortField.STARTTIME.
            sortOrder (SortOrder, optional): Sort order.
                Defaults to SortOrder.ASCENDING.

        Returns:
            EventList: Results of events
        """
        asset = self.name
        # to handle datetime
        starttime = to_af_time(starttime)
        endtime = to_af_time(endtime)
        return self.database.find_events(
            query,
            asset,
            starttime,
            endtime,
            template_name,
            start_index,
            max_count,
            search_mode,
            search_full_hierarchy,
            sortField,
            sortOrder,
        )

    def get_asset_hierarchy(self, depth: int = 10) -> pd.DataFrame:
        """Return AssetHierarchy down to the specified depth

        Args:
            depth (int, optional): depth to return to. Defaults to 10.

        Returns:
            AssetHierarchy: AssetHierarchy
        """

        afcontainer = AF.AFNamedCollectionList[
            AF.Asset.AFElement
        ]()  # empty container
        afcontainer.Add(self.asset)

        df_assets = pd.DataFrame(
            columns=["Asset", "Path", "Name", "Template", "Level"]
        )

        if len(afcontainer) > 0:
            df_roots = pd.DataFrame(
                [(y, y.GetPath()) for y in afcontainer],
                columns=["Asset", "Path"],
            )

            print(
                "Fetching hierarchy data for {} Assets(s)...".format(
                    len(afcontainer)
                )
            )
            # https://docs.osisoft.com/bundle/af-sdk/page/html/M_OSIsoft_AF_Asset_AFElement_LoadElementsToDepth.htm  # noqa
            asset_depth = AF.Asset.AFElement.LoadElementsToDepth(
                afcontainer, False, depth, 1000000
            )

            if len(asset_depth) > 0:
                df_assets = pd.DataFrame(
                    [(y, y.GetPath()) for y in asset_depth],
                    columns=["Asset", "Path"],
                )

            # concatenate procedures and child assets
            df_assets = pd.concat([df_roots, df_assets], ignore_index=True)

            df_assets["Asset"] = df_assets["Asset"].apply(lambda x: Asset(x))
            df_assets["Name"] = df_assets["Asset"].apply(
                lambda x: x.name if x else np.nan
            )
            df_assets["Template"] = df_assets["Asset"].apply(
                lambda x: x.template_name if x else np.nan
            )
            df_assets["Level"] = (
                df_assets["Path"].str.count(r"\\").apply(lambda x: x - 4)
            )
            # print('This Asset Frame has structure of "\\\\Server\\Database\\
            # {}"'.format('\\'.join([str(el) for el in df_assets['Template']
            # .unique()])))
            return df_assets
        else:
            return pd.DataFrame(
                columns=["Asset", "Path", "Name", "Template", "Level"]
            )


class AssetList(UserList):
    """Container for AssetList object"""

    def __init__(self, data):
        self.data = data  # list of Assets
        # validation step ---

    def __repr__(self):
        return str([asset for asset in self.data])

    def __str__(self):
        return str([asset for asset in self.data])

    def get_asset_hierarchy(self, depth: int = 10) -> pd.DataFrame:
        """Return AssetHierarchy down to the specified depth

        Args:
            depth (int, optional): depth to return to. Defaults to 10..

        Returns:
            pd.DataFrame: Dataframe of asset hierarchy.
        """
        afcontainer = AF.AFNamedCollectionList[
            AF.Asset.AFElement
        ]()  # empty container

        for asset in self.data:
            try:
                afcontainer.Add(asset.af_asset)
            except:
                raise ("Failed to process event {}".format(event))

        df_assets = pd.DataFrame(
            columns=["Asset", "Path", "Name", "Template", "Level"]
        )

        if len(afcontainer) > 0:
            df_roots = pd.DataFrame(
                [(y, y.GetPath()) for y in afcontainer],
                columns=["Asset", "Path"],
            )

            print(
                "Fetching hierarchy data for {} Assets(s)...".format(
                    len(afcontainer)
                )
            )
            # https://docs.osisoft.com/bundle/af-sdk/page/html/M_OSIsoft_AF_Asset_AFElement_LoadElementsToDepth.htm  # noqa
            asset_depth = AF.Asset.AFElement.LoadElementsToDepth(
                afcontainer, False, depth, 1000000
            )

            if len(asset_depth) > 0:
                df_assets = pd.DataFrame(
                    [(y, y.GetPath()) for y in asset_depth],
                    columns=["Asset", "Path"],
                )

            # concatenate procedures and child assets
            df_assets = pd.concat([df_roots, df_assets], ignore_index=True)

            df_assets["Asset"] = df_assets["Asset"].apply(lambda x: Asset(x))
            df_assets["Name"] = df_assets["Asset"].apply(
                lambda x: x.name if x else np.nan
            )
            df_assets["Template"] = df_assets["Asset"].apply(
                lambda x: x.template_name if x else np.nan
            )
            df_assets["Level"] = (
                df_assets["Path"].str.count(r"\\").apply(lambda x: x - 4)
            )
            # print('This Asset Frame has structure of "\\\\Server\\Database\\
            # {}"'.format('\\'.join([str(el) for el in df_assets['Template']
            # .unique()])))
            return df_assets
        else:
            return pd.DataFrame(
                columns=["Asset", "Path", "Name", "Template", "Level"]
            ).drop_duplicates("Path")


try:
    # delete the accessor to avoid warning
    # TODO: same question as above
    del pd.DataFrame.ahy
except AttributeError:
    pass


@pd.api.extensions.register_dataframe_accessor("ahy")
class AssetHierarchy:
    """Additional functionality for pd.DataFrame object, for working with
    AssetHierarchies"""

    def __init__(self, df):
        self.validate(df)
        self.df = df

    @staticmethod
    def validate(df):
        """Validate object meets requirements for EventHierarchy"""
        # verify that dataframe fits EventHierarchy requirements
        if not {"Asset", "Path", "Name", "Template", "Level"}.issubset(
            set(df.columns)
        ):
            raise AttributeError(
                "This dataframe does not have the correct AssetHierarchy "
                + "format"
            )

    # methods
    def add_attributes(
        self, attribute_names_list, template_name
    ) -> pd.DataFrame:
        """Add attribute values to AssetHierarchy for specified attributes
        defined for the specified template"""
        print("Fetching attribute(s)...")
        if type(template_name) == int:
            template_name = self.df.loc[
                self.df["Level"] == template_name, "Template"
            ].iloc[0]

        if template_name == None:
            for attribute in attribute_names_list:
                self.df[
                    attribute + " [" + str(template_name) + "]"
                ] = self.df.loc[self.df["Template"].isnull(), "Asset"].apply(
                    lambda x: lambda_aux_add_attributes(x, attribute)
                )
        else:
            for attribute in attribute_names_list:
                self.df[
                    attribute + " [" + str(template_name) + "]"
                ] = self.df.loc[
                    self.df["Template"] == template_name, "Asset"
                ].apply(
                    lambda x: lambda_aux_add_attributes(x, attribute)
                )

        for colname in self.df.columns:
            try:
                self.df[colname] = self.df[colname].astype(float)
            except:
                pass
        return self.df

    def condense(self) -> pd.DataFrame:
        """Condense the AssetHierarchy object to return a condensed,
        vertically layered representation of the Asset Tree

        Returns:
            pd.DataFrame: Condensed dataframe
        """
        print("Condensing...")

        df = self.df.copy()
        # merge level by level
        for level in range(int(df["Level"].min()), int(df["Level"].max() + 1)):
            # subdf per level
            df_level = df[df["Level"] == level]
            # remove empty columns
            df_level.dropna(how="all", axis=1, inplace=True)
            if df_level.empty:
                df_condensed[level] = "TempValue"
            else:
                # add auxiliary columns for merge based on path
                cols = [x for x in range(level + 1)]
                df_level[cols] = (
                    df_level["Path"].str.split("\\", expand=True).loc[:, 4:]
                )
                # remove Path columns
                df_level.drop(columns=["Path"], axis=1, inplace=True)
                # rename columns, ignore columns with number names
                df_level.columns = [
                    col_name + " [" + str(int(level)) + "]"
                    if not ((type(col_name) == int) or ("[" in col_name))
                    else col_name
                    for col_name in df_level.columns
                ]
                # merge with previous level
                if level == int(df["Level"].min()):
                    df_condensed = df_level
                else:
                    df_condensed = pd.merge(
                        df_condensed,
                        df_level,
                        how="outer",
                        left_on=cols[:-1],
                        right_on=cols[:-1],
                    )
        # drop auxiliary columns
        df_condensed.drop(
            columns=[
                col_name
                for col_name in df_condensed.columns
                if type(col_name) == int
            ],
            axis=1,
            inplace=True,
        )
        # remove duplicates
        df_condensed = df_condensed.drop_duplicates(keep="first")

        return df_condensed


class PIAFDatabase(object):
    """PIAFDatabase

    Context manager for connections to the PI Asset Framework database.
    """

    version = "0.2.0"

    _servers = _NOTHING
    _default_server = _NOTHING

    def __init__(self, server=None, database=None):
        self.server = None
        self.database = None
        self._initialise_server(server)
        self._initialise_database(database)

    @classproperty
    def servers(self):
        if self._servers is _NOTHING:
            i, j, failed_servers, failed_databases = 0, 0, 0, 0
            self._servers = {}

            for i, s in enumerate(AF.PISystems(), start=1):
                try:
                    self._servers[s.Name] = {"server": s, "databases": {}}
                    for j, d in enumerate(s.Databases, start=1):
                        try:
                            self._servers[s.Name]["databases"][d.Name] = d
                        except Exception:
                            failed_databases += 1
                        except dotNetException:
                            failed_databases += 1
                except Exception:
                    failed_servers += 1
                except dotNetException:
                    failed_servers += 1
            if failed_servers or failed_databases:
                warn(
                    "Failed loading {}/{} servers and {}/{} databases".format(
                        failed_servers, i, failed_databases, j
                    )
                )
        return self._servers

    @classproperty
    def default_server(self):
        if self._default_server is _NOTHING:
            self._default_server = None
            if AF.PISystems().DefaultPISystem:
                self._default_server = self.servers[
                    AF.PISystems().DefaultPISystem.Name
                ]
            elif len(self.servers) > 0:
                self._default_server = self.servers[list(self.servers)[0]]
            else:
                self._default_server = None
        return self._default_server

    def _initialise_server(self, server: AF.PISystem) -> None:
        """Initialize a server

        Args:
            server (AF.PISystem): server to initialize
        """
        if server and server not in self.servers:
            message = 'Server "{server}" not found, using the default server.'
            warn(message=message.format(server=server), category=UserWarning)
        server = self.servers.get(server, self.default_server)
        self.server = server["server"]

    def _initialise_database(self, database: AF.AFDatabase) -> None:
        """Initialize a provided database

        Args:
            database (AF.AFDatabase): AF Database to initialize
        """
        server = self.servers.get(self.server.Name)
        if not server["databases"]:
            server["databases"] = {x.Name: x for x in self.server.Databases}
        if database and database not in server["databases"]:
            message = (
                'Database "{database}" not found, using the default database.'
            )
            warn(
                message=message.format(database=database), category=UserWarning
            )
        default_db = self.server.Databases.DefaultDatabase
        self.database = server["databases"].get(database, default_db)

    def __enter__(self):
        self.server.Connect()
        return self

    def __exit__(self, *args):
        pass
        # Disabled disconnecting because garbage collection sometimes impedes
        # connecting to another server later
        # self.server.Disconnect()
        self.server.Refresh()
        self.database.Refresh()

    def __repr__(self):
        return "%s(\\\\%s\\%s)" % (
            self.__class__.__name__,
            self.server_name,
            self.database_name,
        )

    @property
    def server_name(self):
        """Return the name of the connected PI AF server."""
        return self.server.Name

    @property
    def database_name(self):
        """Return the name of the connected PI AF database."""
        return self.database.Name

    @property
    def children(self):
        """Return dictionary of the direct child elements of the database"""
        return {c.Name: c for c in self.database.Elements}

    def descendant(self, path):
        """Return a descendant of the database from an exact path"""
        return PIAFElement(self.database.Elements.get_Item(path))

    # https://docs.osisoft.com/bundle/af-sdk/page/html/M_OSIsoft_AF_EventFrame_AFEventFrame_FindEventFrames_1.htm # noqa
    # https://docs.osisoft.com/bundle/af-sdk/page/html/T_OSIsoft_AF_AFSearchField.htm could be implemented # noqa
    # https://pisquare.osisoft.com/s/Blog-Detail/a8r1I000000GvThQAK/using-the-afeventframesearch-class #> attributequery # noqa

    # add option to input list like PIpoint search -------------------------

    def find_events(
        self,
        query: str = None,
        asset: str = "*",
        starttime: Union[str, datetime] = None,
        endtime: Union[str, datetime] = "*",
        template_name: str = None,
        start_index: int = 0,
        max_count: int = 1000000,
        search_mode: SearchMode = SearchMode.INCLUSIVE,
        search_full_hierarchy: bool = True,
        sortField: SortField = SortField.STARTTIME,
        sortOrder: SortOrder = SortOrder.ASCENDING,
    ) -> EventList:
        """Return EventList of Events that meet query criteria

        Args:
            query (str, optional): string to query by. Defaults to None.
            asset (str, optional): asset to search. Defaults to "*".
            start_time (Union[str, datetime], optional): Time to
                start search at. Defaults to None.
            end_time (Union[str, datetime], optional): Time to end
                search at. Defaults to "*".
            template_name (str, optional): Template to search.
                Defaults to None.
            start_index (int, optional): Start Index. Defaults to 0.
            max_count (int, optional): Max Count. Defaults to 1000000.
            search_mode (SearchMode, optional): Search Mode.
                Defaults to SearchMode.INCLUSIVE.
            search_full_hierarchy (bool, optional): Boolean for whether to
                search full hierarchy or not. Defaults to True.
            sortField (SortField, optional): Sorting field.
                Defaults to SortField.STARTTIME.
            sortOrder (SortOrder, optional): Sort order.
                Defaults to SortOrder.ASCENDING.

        Raises:
            AttributeError: If no template found

        Returns:
            EventList: Results of events
        """
        if template_name:
            try:
                afsearchField = SearchField.NAME
                template = AF.Asset.AFElementTemplate.FindElementTemplates(
                    self.database,
                    template_name,
                    afsearchField,
                    sortField,
                    sortOrder,
                    max_count,
                )[0]
            except:
                raise AttributeError("Template name was not found")
        else:
            template = None

        if not starttime:
            starttime = AF.Time.AFTime.Now

        starttime = to_af_time(starttime)
        endtime = to_af_time(endtime)

        lst = AF.EventFrame.AFEventFrame.FindEventFrames(
            self.database,
            None,
            search_mode,
            starttime,
            endtime,
            query,
            asset,
            None,
            template,
            None,
            None,
            search_full_hierarchy,
            sortField,
            sortOrder,
            start_index,
            max_count,
        )

        return EventList([Event(event) for event in lst])

    # find events by path, attribute, referenced element(done) ---------------

    # https://docs.osisoft.com/bundle/af-sdk/page/html/M_OSIsoft_AF_Asset_AFElement_FindElements_2.htm # noqa
    def find_assets(
        self,
        query: str = None,
        top_asset: str = None,
        searchField: SearchField = SearchField.NAME,
        search_full_hierarchy: bool = True,
        sortField: SortField = SortField.STARTTIME,
        sortOrder: SortOrder = SortOrder.ASCENDING,
        max_count: int = 10000000,
    ) -> List[Asset]:
        """Retrieve list of Assets that meet query criteria

        Args:
            query (str, optional): Query string. Defaults to None.
            top_asset (str, optional): Name of top asset. Defaults to None.
            searchField (SearchField, optional): Search Field.
                Defaults to SearchField.NAME.
            search_full_hierarchy (bool, optional): Full Hierarchy search.
                Defaults to True.
            sortField (SortField, optional): Field to sort by.
                Defaults to SortField.STARTTIME.
            sortOrder (SortOrder, optional): Order of sort.
                Defaults to SortOrder.ASCENDING.
            max_count (int, optional): max count. Defaults to 10000000.

        Returns:
            List[Asset]: List of matching Assets
        """

        lst = AF.Asset.AFElement.FindElements(
            self.database,
            top_asset,
            query,
            searchField,
            search_full_hierarchy,
            sortField,
            sortOrder,
            max_count,
        )
        return AssetList([Asset(x) for x in lst])


class Event:
    """Container for Event object"""

    def __init__(self, event: AF.EventFrame):
        self.eventframe = event

    def __repr__(self):
        return "Event:" + self.eventframe.GetPath()

    def __str__(self):
        return "Event:" + self.eventframe.GetPath()

    # Properties
    @property
    def name(self):
        """Return name of event"""
        return self.eventframe.Name

    @property
    def path(self):
        """Return path"""
        return self.eventframe.GetPath()

    @property
    def pisystem_name(self):
        """Return PISystem name"""
        return self.eventframe.PISystem.Name

    @property
    def database_name(self):
        """Return connected database name"""
        return self.eventframe.Database.Name

    @property
    def database(self):
        """Return PIAFDatabase object"""
        return PIAFDatabase(
            server=self.pisystem_name, database=self.database_name
        )

    @property
    def af_eventframe(self):
        """Return AFEventFrame object"""
        return self.eventframe

    @property
    def af_template(self):
        """Return AFTemplate"""
        return self.eventframe.Template

    @property
    def template_name(self):
        """Return template name"""
        return self.eventframe.Template.Name

    @property
    def starttime(self):
        """Return starttime"""
        return timestamp_to_index(self.eventframe.StartTime.UtcTime)

    @property
    def endtime(self):
        """Return endtime"""
        return timestamp_to_index(self.eventframe.EndTime.UtcTime)

    @property
    def af_timerange(self):
        """Return AFTimerange"""
        return self.eventframe.TimeRange

    @property
    def attributes(self):
        """'Return list of attribute names"""
        return [
            Attribute(attribute) for attribute in self.eventframe.Attributes
        ]

    @property
    def af_attributes(self):
        """'Return list of AFAttributes for event"""
        return [attribute for attribute in self.eventframe.Attributes]

    @property
    def ref_elements(self):
        """Return list of references elements for event"""
        return [ref_el.Name for ref_el in self.eventframe.ReferencedElements]

    @property
    def children(self):
        """Return EventList of children"""
        return EventList(
            [Event(event) for event in self.eventframe.EventFrames]
        )

    @property
    def parent(self):
        """Return parent event"""
        return Event(self.eventframe.Parent)

    @property
    def description(self):
        """Return description"""
        return self.eventframe.Description

    @property
    def duration(self):
        """Return duration as datetime.timedelta object"""
        try:
            return self.endtime - self.starttime
        except:  # NaT endtime
            # return timedelta.max
            local_tz = timezone(PIConfig.DEFAULT_TIMEZONE)
            return (
                datetime.utcnow().replace(tzinfo=utc).astimezone(local_tz)
                - self.starttime
            )

    @property
    def top_event(self):
        """Return top-level event name"""
        return self.path.strip("\\").split("\\")[2]

    # Methods
    def plot_values(
        self,
        tag_list: List[Union[str, Tag]],
        nr_of_intervals: int,
        dataserver: PIServer = None,
        paging_config: AF.PI.PIPagingConfiguration = AF.PI.PIPagingConfiguration(
            AF.PI.PIPageType.TagCount, 1000
        ),
    ) -> Dict[str, pd.DataFrame]:
        """Retrieves values over the specified time range suitable for
        plotting over the number of intervals (typically represents pixels).
        Returns a Dictionary of DataFrames for Tags in Taglist with values
        that will produce the most accurate plot over the time range while
        minimizing the amount of data returned. Each interval can produce up
        to 5 values if they are unique, the first value in the interval, the
        last value, the highest value, the lowest value and at most one
        exceptional point (bad status or digital state).

        Args:
            tag_list (List[Union[str, Tag]]): list of strings/Tags
            nr_of_intervals (int): Number of intervals
            dataserver (PIServer, optional): dataserver; necessary if the list
                is strings. Defaults to None.

        Returns:
            Dict[str, pd.DataFrame]: tagName: requested dataframe
        """
        taglist = convert_to_TagList(tag_list, dataserver)
        return taglist.plot_values(
            self.starttime,
            self.endtime,
            nr_of_intervals,
            paging_config=paging_config,
        )

    def interpolated_values(
        self,
        tag_list: List[Union[str, Tag]],
        interval: str,
        filter_expression: str = "",
        dataserver: PIServer = None,
        paging_config: AF.PI.PIPagingConfiguration = AF.PI.PIPagingConfiguration(
            AF.PI.PIPageType.TagCount, 1000
        ),
    ) -> pd.DataFrame:
        """Retrieve interpolated values for each Tag in TagList

        Args:
            tag_list (List[Union[str, Tag]]): list of Tags
            interval (str): interval to interpolate to
            dataserver (PIServer, optional): dataserver; necessary if the list
                is strings. Defaults to None.
            filter_expression (str, optional): Filter expression.
                Defaults to "".

        Returns:
            pd.DataFrame: interpolated values for Tags in TagList
        """
        taglist = convert_to_TagList(tag_list, dataserver)
        endtime = self.endtime
        if type(self.endtime) == float:
            local_tz = timezone(PIConfig.DEFAULT_TIMEZONE)
            endtime = (
                datetime.utcnow().replace(tzinfo=utc).astimezone(local_tz)
            )
        return taglist.interpolated_values(
            self.starttime,
            endtime,
            interval,
            filter_expression,
            paging_config=paging_config,
        )

    def recorded_values(
        self,
        tag_list: List[Union[str, Tag]],
        dataserver: PIServer = None,
        filter_expression: str = "",
        AFBoundaryType: BoundaryType = BoundaryType.INSIDE,
        paging_config: AF.PI.PIPagingConfiguration = AF.PI.PIPagingConfiguration(
            AF.PI.PIPageType.TagCount, 1000
        ),
    ) -> Dict[str, pd.DataFrame]:
        """Retrieve recorded values for each Tag in TagList within the event

        Args:
            tag_list (List[Union[str, Tag]]): list of tags
            dataserver (PIServer, optional): dataserver; necessary if the list
                is strings. Defaults to None.
            filter_expression (str, optional): Filter expression.
                Defaults to "".
            AFBoundaryType (BoundaryType, optional): Defined BoundaryType.
                Defaults to BoundaryType.INSIDE.

        Returns:
            Dict[str, pd.DataFrame]: tagName: dataframe of requested values
        """
        taglist = convert_to_TagList(tag_list, dataserver)
        return taglist.recorded_values(
            self.starttime,
            self.endtime,
            filter_expression,
            AFBoundaryType,
            paging_config=paging_config,
        )

    def summary(
        self,
        tag_list: List[Union[str, Tag]],
        summary_types: int,
        dataserver: PIServer = None,
        calculation_basis: CalculationBasis = CalculationBasis.TIME_WEIGHTED,
        time_type: TimestampCalculation = TimestampCalculation.AUTO,
        paging_config: AF.PI.PIPagingConfiguration = AF.PI.PIPagingConfiguration(
            AF.PI.PIPageType.TagCount, 1000
        ),
    ) -> pd.DataFrame:
        """Return specified summary measure(s) for event

        Args:
            tag_list (List[Union[str, Tag]]): list of tags
            summary_types (int): integers separated by '|'. List given
                below. E.g. "summary_types = 1|8" gives TOTAL and MAXIMUM

                - TOTAL = 1: A total over the time span
                - AVERAGE = 2: Average value over the time span
                - MINIMUM = 4: The minimum value in the time span
                - MAXIMUM = 8: The maximum value in the time span
                - RANGE = 16: The range of the values (max-min) in the time
                    span
                - STD_DEV = 32 : The sample standard deviation of the values
                    over the time span
                - POP_STD_DEV = 64: The population standard deviation of the
                    values over the time span
                - COUNT = 128: The sum of the event count (when the
                    calculation is event weighted). The sum of the event time
                        duration (when the calculation is time weighted.)
                - PERCENT_GOOD = 8192: The percentage of the data with a good
                    value over the time range. Based on time for time weighted
                        calculations, based on event count for event weigthed
                        calculations.
                - TOTAL_WITH_UOM = 16384: The total over the time span, with
                    the unit of measurement that's associated with the input
                    (or no units if not defined for the input)
                - ALL = 24831: A convenience to retrieve all summary types
                - ALL_FOR_NON_NUMERIC = 8320: A convenience to retrieve all
                    summary types for non-numeric data

            dataserver (PIServer, optional): dataserver; necessary if the list
                is strings. Defaults to None.
            calculation_basis (CalculationBasis, optional): Basis by which to
                calculate the summary statistic.
                Defaults to CalculationBasis.TIME_WEIGHTED.
            time_type (TimestampCalculation, optional): How the timestamp is
                calculated. Defaults to TimestampCalculation.AUTO.

        Returns:
            pd.DataFrame: Dataframe with requested summary statistics_
        """
        taglist = convert_to_TagList(tag_list, dataserver)
        return taglist.summary(
            self.starttime,
            self.endtime,
            summary_types,
            calculation_basis,
            time_type,
            paging_config=paging_config,
        )

    # summaries
    def summaries(
        self,
        tag_list: List[Union[str, Tag]],
        interval: str,
        summary_types: int,
        dataserver: PIServer = None,
        calculation_basis: CalculationBasis = CalculationBasis.TIME_WEIGHTED,
        time_type: TimestampCalculation = TimestampCalculation.AUTO,
        paging_config: AF.PI.PIPagingConfiguration = AF.PI.PIPagingConfiguration(
            AF.PI.PIPageType.TagCount, 1000
        ),
    ) -> pd.DataFrame:
        """Return one or more summary values for Tags in Taglist
        for each interval

        Args:
            tag_list (List[Union[str, Tag]]): list of strings/Tags
            interval (str): interval to interpolate to
            summary_types (int): integers separated by '|'. List given
                below. E.g. "summary_types = 1|8" gives TOTAL and MAXIMUM

                - TOTAL = 1: A total over the time span
                - AVERAGE = 2: Average value over the time span
                - MINIMUM = 4: The minimum value in the time span
                - MAXIMUM = 8: The maximum value in the time span
                - RANGE = 16: The range of the values (max-min) in the time
                    span
                - STD_DEV = 32 : The sample standard deviation of the values
                    over the time span
                - POP_STD_DEV = 64: The population standard deviation of the
                    values over the time span
                - COUNT = 128: The sum of the event count (when the
                    calculation is event weighted). The sum of the event time
                        duration (when the calculation is time weighted.)
                - PERCENT_GOOD = 8192: The percentage of the data with a good
                    value over the time range. Based on time for time weighted
                        calculations, based on event count for event weigthed
                        calculations.
                - TOTAL_WITH_UOM = 16384: The total over the time span, with
                    the unit of measurement that's associated with the input
                    (or no units if not defined for the input)
                - ALL = 24831: A convenience to retrieve all summary types
                - ALL_FOR_NON_NUMERIC = 8320: A convenience to retrieve all
                    summary types for non-numeric data

            dataserver (PIServer, optional): dataserver; necessary if the list
                is strings. Defaults to None.
            calculation_basis (CalculationBasis, optional): Basis by which to
                calculate the summary statistic.
                Defaults to CalculationBasis.TIME_WEIGHTED.
            time_type (TimestampCalculation, optional): How the timestamp is
                calculated. Defaults to TimestampCalculation.AUTO.

        Returns:
            pd.DataFrame: Dataframe with requested summary statistics
        """
        taglist = convert_to_TagList(tag_list, dataserver)
        return taglist.summaries(
            self.starttime,
            self.endtime,
            interval,
            summary_types,
            calculation_basis,
            time_type,
            paging_config=paging_config,
        )

    # filtered summaries
    def filtered_summaries(
        self,
        tag_list: List[Union[str, Tag]],
        interval: str,
        summary_types: int,
        filter_expression: str,
        dataserver: PIServer = None,
        calculation_basis: CalculationBasis = CalculationBasis.TIME_WEIGHTED,
        time_type=TimestampCalculation.AUTO,
        AFfilter_evaluation=ExpressionSampleType.EXPRESSION_RECORDED_VALUES,
        filter_interval: str = None,
        paging_config: AF.PI.PIPagingConfiguration = AF.PI.PIPagingConfiguration(
            AF.PI.PIPageType.TagCount, 1000
        ),
    ) -> pd.DataFrame:
        """For each Tag in TagList, return one or more summary values for each
        interval, for values that meet the specified filter condition

        Args:
            tag_list (List[Union[str, Tag]]): list of strings/Tags
            interval (str): interval to interpolate to
            summary_types (int): integers separated by '|'. List given
                below. E.g. "summary_types = 1|8" gives TOTAL and MAXIMUM

                - TOTAL = 1: A total over the time span
                - AVERAGE = 2: Average value over the time span
                - MINIMUM = 4: The minimum value in the time span
                - MAXIMUM = 8: The maximum value in the time span
                - RANGE = 16: The range of the values (max-min) in the time
                    span
                - STD_DEV = 32 : The sample standard deviation of the values
                    over the time span
                - POP_STD_DEV = 64: The population standard deviation of the
                    values over the time span
                - COUNT = 128: The sum of the event count (when the
                    calculation is event weighted). The sum of the event time
                        duration (when the calculation is time weighted.)
                - PERCENT_GOOD = 8192: The percentage of the data with a good
                    value over the time range. Based on time for time weighted
                        calculations, based on event count for event weigthed
                        calculations.
                - TOTAL_WITH_UOM = 16384: The total over the time span, with
                    the unit of measurement that's associated with the input
                    (or no units if not defined for the input)
                - ALL = 24831: A convenience to retrieve all summary types
                - ALL_FOR_NON_NUMERIC = 8320: A convenience to retrieve all
                    summary types for non-numeric data

            filter_expression (str):  Filter expression._
            dataserver (PIServer, optional): dataserver; necessary if the list
                is strings. Defaults to None.
            calculation_basis (CalculationBasis, optional): Basis by which to
                calculate the summary statistic.
                Defaults to CalculationBasis.TIME_WEIGHTED.
            time_type (TimestampCalculation, optional): How the timestamp is
                calculated. Defaults to TimestampCalculation.AUTO.
            AFfilter_evaluation (ExpressionSampleType, optional): Expression
                Type. Defaults to
                ExpressionSampleType.EXPRESSION_RECORDED_VALUES.
            filter_interval (str, optional): _description_. Defaults to None.

        Returns:
            pd.DataFrame: Dataframe with requested summary statistics
        """
        taglist = convert_to_TagList(tag_list, dataserver)
        return taglist.filtered_summaries(
            self.starttime,
            self.endtime,
            interval,
            summary_types,
            filter_expression,
            calculation_basis,
            time_type,
            AFfilter_evaluation,
            filter_interval,
            paging_config=paging_config,
        )

    def get_attribute_values(
        self,
        attribute_names_list: Union[
            None, List[str], List[AF.Asset.AFAttribute]
        ] = None,
    ) -> Dict[str, float]:
        """Get attribute values for specified attributes

        Args:
            attribute_names_list (Union[None, List[AF.Asset.AFAttribute]],
                optional): List of Attributes to query. If None, will do all
                attributes within the Event. Defaults to None.

        Returns:
            Dict[str, float]: attribute Name: attribute value
        """
        if attribute_names_list is None:
            attribute_names_list = [
                att.Name for att in self.eventframe.Attributes
            ]

        attribute_dct = {
            attribute.Name: attribute.GetValue().Value
            for attribute in self.eventframe.Attributes
            if (attribute.Name in attribute_names_list)
            or (attribute in attribute_names_list)
        }

        return attribute_dct

    def get_event_hierarchy(self, depth: int = 10) -> pd.DataFrame:
        """Return EventHierarchy down to the specified depth

        Args:
            depth (int, optional): depth to return to. Defaults to 10.

        Returns:
            pd.DataFrame: Dataframe of event hierarchy.
        """
        # Level better option than template names
        df_procedures = pd.DataFrame(
            [(self.eventframe, self.eventframe.GetPath())],
            columns=["Event", "Path"],
        )

        afcontainer = AF.AFNamedCollectionList[
            AF.EventFrame.AFEventFrame
        ]()  # empty container
        afcontainer.Add(self.eventframe)

        print("Fetching hierarchy data for Event...")
        # https://docs.osisoft.com/bundle/af-sdk/page/html/M_OSIsoft_AF_EventFrame_AFEventFrame_LoadEventFramesToDepth.htm
        event_depth = AF.EventFrame.AFEventFrame.LoadEventFramesToDepth(
            afcontainer, False, depth, 1000000
        )

        df_events = pd.DataFrame()
        if len(event_depth) > 0:
            df_events = pd.DataFrame(
                [(y, y.GetPath()) for y in event_depth],
                columns=["Event", "Path"],
            )

        # concatenate procedures and child event frames
        df_events = pd.concat([df_procedures, df_events], ignore_index=True)

        df_events["Event"] = df_events["Event"].apply(lambda x: Event(x))
        df_events["Name"] = df_events["Event"].apply(
            lambda x: x.name if x else np.nan
        )
        df_events["Template"] = df_events["Event"].apply(
            lambda x: x.template_name if x.af_template else np.nan
        )
        df_events["Level"] = (
            df_events["Path"].str.count(r"\\").apply(lambda x: x - 4)
        )
        df_events["Starttime"] = df_events["Event"].apply(
            lambda x: x.starttime if x else np.nan
        )
        df_events["Endtime"] = df_events["Event"].apply(
            lambda x: x.endtime if x else np.nan
        )

        return df_events.drop_duplicates("Path")


try:
    # TODO: There's an "ehy" property? Why not just change the name slightly
    # rather than have to delete the property which could affect other modules
    # delete the accessor to avoid warning
    del pd.DataFrame.ehy
except AttributeError:
    pass


# https://pandas.pydata.org/docs/development/extending.html
# DataFrames are not meant to be subclassed, but you can implement your own
# functionality via the extension API.
@pd.api.extensions.register_dataframe_accessor("ehy")
class EventHierarchy:
    """Additional functionality for pd.DataFrame object, for working with
    EventHierarchies"""

    def __init__(self, df):
        self.validate(df)
        self.df = df

    @staticmethod
    def validate(df):
        """Validate object meets requirements for EventHierarchy"""
        # verify that dataframe fits EventHierarchy requirements
        if not {
            "Event",
            "Path",
            "Name",
            "Template",
            "Level",
            "Starttime",
            "Endtime",
        }.issubset(set(df.columns)):
            raise AttributeError(
                "This dataframe does not have the correct EventHierarchy "
                + "format"
            )

    # Methods
    def add_attributes(self, attribute_names_list, template_name):
        """Add attribute values to EventHierarchy for specified attributes
        defined for the specified template"""
        print("Fetching attribute(s)...")
        if type(template_name) == int:
            template_name = self.df.loc[
                self.df["Level"] == template_name, "Template"
            ].iloc[0]

        if template_name == None:
            for attribute in attribute_names_list:
                self.df[
                    attribute + " [" + str(template_name) + "]"
                ] = self.df.loc[self.df["Template"].isnull(), "Event"].apply(
                    lambda x: lambda_aux_add_attributes(x, attribute)
                )
        else:
            for attribute in attribute_names_list:
                self.df[
                    attribute + " [" + str(template_name) + "]"
                ] = self.df.loc[
                    self.df["Template"] == template_name, "Event"
                ].apply(
                    lambda x: lambda_aux_add_attributes(x, attribute)
                )

        for colname in self.df.columns:
            try:
                self.df[colname] = self.df[colname].astype(float)
            except:
                pass
        return self.df

    def add_ref_elements(self, template_name):
        print("Fetching referenced element(s)...")
        """Add referenced element values to EventHierarchy, defined for the
        specified template"""
        if type(template_name) == int:
            template_name = self.df.loc[
                self.df["Level"] == template_name, "Template"
            ].iloc[0]

        if template_name == None:
            ref_el = (
                self.df.loc[self.df["Template"].isnull(), "Event"]
                .apply(lambda x: x.ref_elements)
                .apply(pd.Series)
            )
        else:
            ref_el = (
                self.df.loc[self.df["Template"] == template_name, "Event"]
                .apply(lambda x: x.ref_elements)
                .apply(pd.Series)
            )

        if ref_el.empty:
            raise AttributeError("No results found for the specified template")

        for col in ref_el.columns:
            self.df[
                "Referenced_el"
                + " ["
                + str(template_name)
                + "]"
                + "("
                + str(col)
                + ")"
            ] = ref_el[col]
        return self.df

    def condense(self):
        """Condense the EventHierarchy object to return a vertically layered
        CondensedEventHierarchy object"""
        print("Condensing...")

        df = self.df.copy()

        # merge level by level
        for level in range(int(df["Level"].min()), int(df["Level"].max() + 1)):
            # subdf per level
            df_level = df[df["Level"] == level]
            # remove empty columns
            df_level.dropna(how="all", axis=1, inplace=True)
            if df_level.empty:
                df_condensed[level] = "TempValue"
            else:
                # add auxiliary columns for merge based on path
                cols = [x for x in range(level + 1)]
                df_level[cols] = (
                    df_level["Path"].str.split("\\", expand=True).loc[:, 4:]
                )
                # remove Path columns
                df_level.drop(columns=["Path"], inplace=True)
                # rename columns, ignore columns with number names
                df_level.columns = [
                    col_name + " [" + str(int(level)) + "]"
                    if not ((type(col_name) == int) or ("[" in col_name))
                    else col_name
                    for col_name in df_level.columns
                ]
                # merge with previous level
                if level == int(df["Level"].min()):
                    df_condensed = df_level
                else:
                    df_condensed = pd.merge(
                        df_condensed,
                        df_level,
                        how="outer",
                        left_on=cols[:-1],
                        right_on=cols[:-1],
                    )
        # drop auxiliary columns
        df_condensed.drop(
            columns=[
                col_name
                for col_name in df_condensed.columns
                if type(col_name) == int
            ],
            inplace=True,
        )

        # remove duplicates (issues with removing duplicates with pandas date objects)
        df_condensed = df_condensed.iloc[
            df_condensed.astype(str).drop_duplicates(keep="first").index
        ]
        df_condensed.reset_index(inplace=True, drop=True)

        # address NaT times (copy value from parent layer)
        endtime_cols = [
            col_name
            for col_name in df_condensed.columns
            if col_name.startswith("Endtime")
        ]
        for i, col in enumerate(endtime_cols):
            if (
                not i == 0
            ):  # handle naT in lower layers by inheriting from parent
                df_condensed[col].fillna(
                    df_condensed[endtime_cols[i - 1]], inplace=True
                )

        return df_condensed

    def interpol_discrete_extract(
        self,
        tag_list: List[Union[str, Tag]],
        interval: str,
        filter_expression="",
        dataserver: PIServer = None,
        col: bool = False,
        paging_config: AF.PI.PIPagingConfiguration = AF.PI.PIPagingConfiguration(
            AF.PI.PIPageType.EventCount, 1000
        ),
    ) -> pd.DataFrame:
        """Return dataframe of interpolated data for discrete events of
        EventHierarchy, for the tag(s) specified

        Args:
            tag_list (List[Union[str, Tag]]): list of Tags
            interval (str): interval to interpolate to
            filter_expression (str, optional): Filter expression.
                Defaults to "".
            dataserver (PIServer, optional): PI Data Server. Defaults to None.
            col (bool, optional): _description_. Defaults to False.

        Returns:
            pd.DataFrame: Interpolated data for discrete events
        """
        print("building discrete extract table from EventHierachy...")
        df = self.df.copy()

        # performance checks
        maxi = max(df["Event"].apply(lambda x: x.duration))
        if maxi > pd.Timedelta("60 days"):
            print(
                f"Large Event(s) with duration up to {maxi} detected, "
                + "Note that this might take some time..."
            )
        if len(df) > 50:
            print(
                f"Extracts will be made for {len(df)} Events, Note that "
                + "this might take some time..."
            )

        if not col:
            taglist = convert_to_TagList(tag_list, dataserver)
            # extract interpolated data for discrete events
            df["Time"] = df["Event"].apply(
                lambda x: list(
                    x.interpolated_values(
                        taglist,
                        interval,
                        filter_expression,
                        paging_config=paging_config,
                    ).to_records(index=True)
                )
            )

        if col:
            if len(tag_list) > 1:
                raise AttributeError(
                    f"You can only specify a single tag column at a time"
                )
            if tag_list[0] in df.columns:
                event = df.columns.get_loc("Event")
                tags = df.columns.get_loc(tag_list[0])
                # for summary one can define multiple tags in the string
                if df[tag_list[0]].str.contains(",").any():
                    raise AttributeError(
                        "Cell can only contain one Tag at a time"
                    )

                # extract interpolated data for discrete events
                df["Time"] = df.apply(
                    lambda row: list(
                        row[event]
                        .interpolated_values(
                            [row[tags]],
                            interval,
                            filter_expression,
                            dataserver,
                            paging_config=paging_config,
                        )
                        .to_records(index=True)
                    ),
                    axis=1,
                )
            else:
                raise AttributeError(
                    f"The column option was set to True, but {tag_list[0]}"
                    + " is not a valid column"
                )

        df = df.explode("Time")  # explode list to rows
        df["Time"] = df["Time"].apply(
            lambda x: [el for el in x] if not pd.isnull(x) else np.nan
        )  # numpy record to list

        if not col:
            df[["Time"] + [tag.name for tag in taglist]] = df["Time"].apply(
                pd.Series
            )  # explode list to columns
        else:
            df[["Time", "Value"]] = df["Time"].apply(
                pd.Series
            )  # explode list to columns
        df["Time"] = df["Time"].apply(
            lambda x: add_timezone(x) if not pd.isnull(x) else x
        )
        df.reset_index(drop=True, inplace=True)

        return df

    def summary_extract(
        self,
        tag_list: List[Union[str, Tag]],
        summary_types: int,
        dataserver: PIServer = None,
        calculation_basis: CalculationBasis = CalculationBasis.TIME_WEIGHTED,
        time_type=TimestampCalculation.AUTO,
        col=False,
        paging_config: AF.PI.PIPagingConfiguration = AF.PI.PIPagingConfiguration(
            AF.PI.PIPageType.EventCount, 1000
        ),
    ) -> pd.DataFrame:
        """Return dataframe of summary measures for discrete events of
        EventHierarchy, for the tag(s) specified

        Args:
            tag_list (List[Union[str, Tag]]): list of Tags
            summary_types (int): integers separated by '|'. List given
                below. E.g. "summary_types = 1|8" gives TOTAL and MAXIMUM

                - TOTAL = 1: A total over the time span
                - AVERAGE = 2: Average value over the time span
                - MINIMUM = 4: The minimum value in the time span
                - MAXIMUM = 8: The maximum value in the time span
                - RANGE = 16: The range of the values (max-min) in the time
                    span
                - STD_DEV = 32 : The sample standard deviation of the values
                    over the time span
                - POP_STD_DEV = 64: The population standard deviation of the
                    values over the time span
                - COUNT = 128: The sum of the event count (when the
                    calculation is event weighted). The sum of the event time
                        duration (when the calculation is time weighted.)
                - PERCENT_GOOD = 8192: The percentage of the data with a good
                    value over the time range. Based on time for time weighted
                        calculations, based on event count for event weigthed
                        calculations.
                - TOTAL_WITH_UOM = 16384: The total over the time span, with
                    the unit of measurement that's associated with the input
                    (or no units if not defined for the input)
                - ALL = 24831: A convenience to retrieve all summary types
                - ALL_FOR_NON_NUMERIC = 8320: A convenience to retrieve all
                    summary types for non-numeric data

            dataserver (PIServer, optional): dataserver; necessary if the list
                is strings. Defaults to None.
            calculation_basis (CalculationBasis, optional): Basis by which to
                calculate the summary statistic.
                Defaults to CalculationBasis.TIME_WEIGHTED.
            time_type (TimestampCalculation, optional): How the timestamp is
                calculated. Defaults to TimestampCalculation.AUTO.
            col (bool, optional): _description_. Defaults to False.

        Returns:
            pd.DataFrame: dataframe of summary measures
        """
        print("Building summary table from EventHierarchy...")
        df = self.df.copy()

        # performance checks
        maxi = max(df["Event"].apply(lambda x: x.duration))
        if maxi > pd.Timedelta("60 days"):
            print(
                f"Large Event(s) with duration up to {maxi} detected, "
                + "Note that this might take some time..."
            )
        if len(df) > 50:
            print(
                f"Summaries will be calculated for {len(df)} Events, Note"
                + " that this might take some time..."
            )

        if not col:
            taglist = convert_to_TagList(tag_list, dataserver)
            # extract summary data for discrete events
            df["Time"] = df["Event"].apply(
                lambda x: list(
                    x.summary(
                        taglist,
                        summary_types,
                        dataserver,
                        calculation_basis,
                        time_type,
                        paging_config=paging_config,
                    ).to_records(index=False)
                )
            )

        if col:
            if len(tag_list) > 1:
                raise AttributeError(
                    f"You can only specify a single tag column at a time"
                )
            if tag_list[0] in df.columns:
                event = df.columns.get_loc("Event")

                df.reset_index(drop=True, inplace=True)
                # just single request for each unique target
                for tg in df[tag_list[0]].unique():
                    tl = convert_to_TagList(
                        tg.replace(" ", "").split(","), dataserver
                    )
                    # https://stackoverflow.com/questions/39717809/insert-list-into-cells-which-meet-column-conditions
                    df.loc[df[tag_list[0]] == tg, "Tags"] = pd.Series(
                        [tl] * df.shape[0]
                    )

                # extract summary data for discrete events
                df["Time"] = df.apply(
                    lambda row: list(
                        row[event]
                        .summary(
                            row["Tags"],
                            summary_types,
                            calculation_basis,
                            time_type,
                            paging_config=paging_config,
                        )
                        .to_records(index=False)
                    ),
                    axis=1,
                )
            else:
                raise AttributeError(
                    f"The column option was set to True, but {tag_list[0]} "
                    + "is not a valid column"
                )

        df = df.explode("Time")  # explode list to rows
        df["Time"] = df["Time"].apply(
            lambda x: [el for el in x] if not pd.isnull(x) else np.nan
        )  # numpy record to list
        df[["Tag", "Summary", "Value", "Time"]] = df["Time"].apply(
            pd.Series
        )  # explode list to columns
        df.reset_index(drop=True, inplace=True)

        return df

    def calc_summary_extract(
        self,
        interval: str,
        summary_types: int,
        expression: str = "",
        calculation_basis: CalculationBasis = CalculationBasis.TIME_WEIGHTED,
        time_type: TimestampCalculation = TimestampCalculation.AUTO,
        AFfilter_evaluation: ExpressionSampleType = ExpressionSampleType.EXPRESSION_RECORDED_VALUES,
        filter_interval: str = None,
        col: bool = False,
    ) -> pd.DataFrame:

        """Return dataframe of summary measures of calculations specified in expression,
        for discrete events of EventHierarchy.

        Args:
            interval (str): The bounding time for the evaluation period.
            summary_types (int): integers separated by '|'. List given
                below. E.g. "summary_types = 1|8" gives TOTAL and MAXIMUM

                - TOTAL = 1: A total over the time span
                - AVERAGE = 2: Average value over the time span
                - MINIMUM = 4: The minimum value in the time span
                - MAXIMUM = 8: The maximum value in the time span
                - RANGE = 16: The range of the values (max-min) in the time
                    span
                - STD_DEV = 32 : The sample standard deviation of the values
                    over the time span
                - POP_STD_DEV = 64: The population standard deviation of the
                    values over the time span
                - COUNT = 128: The sum of the event count (when the
                    calculation is event weighted). The sum of the event time
                        duration (when the calculation is time weighted.)
                - PERCENT_GOOD = 8192: The percentage of the data with a good
                    value over the time range. Based on time for time weighted
                        calculations, based on event count for event weigthed
                        calculations.
                - TOTAL_WITH_UOM = 16384: The total over the time span, with
                    the unit of measurement that's associated with the input
                    (or no units if not defined for the input)
                - ALL = 24831: A convenience to retrieve all summary types
                - ALL_FOR_NON_NUMERIC = 8320: A convenience to retrieve all
                    summary types for non-numeric data

            expression (raw string): A string containing the expression to be evaluated.
                The syntax for the expression generally follows the
                Performance Equation syntax as described in
                the PI Data Archive documentation.
            calculation_basis (CalculationBasis, optional): Basis by which to
                calculate the summary statistic.
                Defaults to CalculationBasis.TIME_WEIGHTED.
            time_type (TimestampCalculation, optional): How the timestamp is
                calculated. Defaults to TimestampCalculation.AUTO.
            AFfilter_evaluation (ExpressionSampleType, optional): Expression
                Type. Defaults to
                ExpressionSampleType.EXPRESSION_RECORDED_VALUES.
            col (bool, optional): Parameter to toggle the column functionality.
                When set to True on can pass a column name trough the 'expression' argument.
                Expressions wil be evaluated on a row-to-row basis based on column value.
                Defaults to False.

        Returns:
            pd.DataFrame: dataframe of summary measures
        """
        print("Building calcultion summary table from EventHierarchy...")
        df = self.df.copy()

        # performance checks
        maxi = max(df["Event"].apply(lambda x: x.duration))
        if maxi > pd.Timedelta("60 days"):
            print(
                f"Large Event(s) with duration up to {maxi} detected, "
                + "Note that this might the collection limit..."
            )
        if len(df) > 50:
            print(
                f"Summaries will be calculated for {len(df)} Events, Note"
                + " that this might take some time..."
            )

        if not col:
            # extract summary data for discrete events
            df["Time"] = df["Event"].apply(
                lambda x: list(
                    calc_summary(
                        starttime=x.starttime,
                        endtime=x.endtime,
                        interval=interval,
                        summary_types=summary_types,
                        expression=expression,
                        calculation_basis=calculation_basis,
                        time_type=time_type,
                        AFfilter_evaluation=AFfilter_evaluation,
                        filter_interval=filter_interval,
                    ).to_records(index=False)
                )
            )

        if col:
            if not type(expression) == str:
                raise AttributeError(
                    "Name of expression column should be of string type"
                )
            if expression in df.columns:
                event = df.columns.get_loc("Event")
                df.reset_index(drop=True, inplace=True)

                # extract summary data for discrete events
                df["Time"] = df.apply(
                    lambda row: list(
                        calc_summary(
                            starttime=row[event].starttime,
                            endtime=row[event].endtime,
                            interval=interval,
                            summary_types=summary_types,
                            expression=row[expression],
                            calculation_basis=calculation_basis,
                            time_type=time_type,
                            AFfilter_evaluation=AFfilter_evaluation,
                            filter_interval=filter_interval,
                        ).to_records(index=False)
                    ),
                    axis=1,
                )
            else:
                raise AttributeError(
                    f"The column option was set to True, but {expression} "
                    + "is not a valid column name"
                )

        df = df.explode("Time")  # explode list to rows
        df["Time"] = df["Time"].apply(
            lambda x: [el for el in x] if not pd.isnull(x) else np.nan
        )  # numpy record to list
        df[["Summary", "Value", "Time"]] = df["Time"].apply(
            pd.Series
        )  # explode list to columns
        df.reset_index(drop=True, inplace=True)

        return df


try:
    # delete the accessor to avoid warning
    # TODO: Same as above. Why delete? what warning?
    del pd.DataFrame.ecd
except AttributeError:
    pass


@pd.api.extensions.register_dataframe_accessor("ecd")
class CondensedEventHierarchy:
    """Additional functionality for pd.DataFrame object, for working with
    CondensedHierarchies"""

    def __init__(self, df):
        self.df = df
        self.validate()

    def validate(self):
        """Validate input meets requirements for CondensedHierarchy"""
        if not {
            "Event",
            "Name",
            "Template",
            "Level",
            "Starttime",
            "Endtime",
        }.issubset({x.split(" ")[0] for x in self.df.columns}):
            raise AttributeError(
                "This dataframe does not have the correct EventHierarchy "
                + "format"
            )
        for event in self.df.columns[
            self.df.columns.str.contains(r"Event\s\[.*]", regex=True)
        ]:
            if set(self.df[event].apply(lambda x: type(x)).unique()) == {
                Event
            }:
                pass
            elif set(self.df[event].apply(lambda x: type(x)).unique()) == {
                Event,
                float,
            }:
                print(
                    "Attention: this CondensedHierarchy contains 'NAN' events, 'NAN' events will be dropped for the method execution"
                )
                # drop rows that contain NAN value in any of the Event columns
                self.df = self.df[
                    self.df[
                        self.df.columns[
                            self.df.columns.str.contains(
                                r"Event\s\[.*]", regex=True
                            )
                        ]
                    ]
                    .notnull()
                    .all(1)
                ]
            else:
                raise AttributeError(
                    "This dataframe does not have the correct "
                    + "CondensedHierarchy format"
                )

    # Methods

    def interpol_discrete_extract(
        self,
        tag_list: List[Union[str, Tag]],
        interval: str,
        filter_expression="",
        dataserver: PIServer = None,
        col: bool = False,
        paging_config: AF.PI.PIPagingConfiguration = AF.PI.PIPagingConfiguration(
            AF.PI.PIPageType.EventCount, 1000
        ),
    ) -> pd.DataFrame:
        """dataframe of interpolated values for discrete events on bottom
        level of condensed hierarchy

        Args:
            tag_list (List[Union[str, Tag]]): list of tags
            interval (str): interval to interpolate to
            filter_expression (str, optional): Filter expression.
                Defaults to "".
            dataserver (PIServer, optional): dataserver; necessary if the list
                is strings. Defaults to None.
            col (bool, optional): _description_. Defaults to False.

        Returns:
            pd.DataFrame: dataframe of interpolated values
        """
        print("building discrete extract table from condensed hierachy...")
        # select events on bottem level of condensed hierarchy
        col_event = [
            col_name
            for col_name in self.df.columns
            if col_name.startswith("Event")
        ][-1]

        # based on list of tags
        if not col:
            df = self.df[[col_event]].copy()
            df.columns = ["Event"]

            # performance checks
            maxi = max(df["Event"].apply(lambda x: x.duration))
            if maxi > pd.Timedelta("60 days"):
                print(
                    f"Large Event(s) with duration up to {maxi} detected, "
                    + "Note that this might take some time..."
                )
            if len(df) > 50:
                print(
                    f"Summaries will be calculated for {len(df)} Events, "
                    + "Note that this might take some time..."
                )

            # add procedure names
            df["Procedure"] = df["Event"].apply(lambda x: x.top_event)
            df = df[["Procedure", "Event"]]
            df.reset_index(drop=True, inplace=True)

            taglist = convert_to_TagList(tag_list, dataserver)
            # extract interpolated data for discrete events
            df["Time"] = df["Event"].apply(
                lambda x: list(
                    x.interpolated_values(
                        taglist,
                        interval,
                        filter_expression,
                        paging_config=paging_config,
                    ).to_records(index=True)
                )
            )

        # based on column with tags
        if col:
            if len(tag_list) > 1:
                raise AttributeError(
                    f"You can only specify a single tag column at a time"
                )
            if tag_list[0] in self.df.columns:
                df = self.df[[col_event, tag_list[0]]].copy()
                df.columns = ["Event", "Tags"]
            else:
                raise AttributeError(
                    f"The column option was set to True, but {tag_list} is "
                    + "not a valid column"
                )

            # performance checks
            maxi = max(df["Event"].apply(lambda x: x.duration))
            if maxi > pd.Timedelta("60 days"):
                print(
                    f"Large Event(s) with duration up to {maxi} detected, "
                    + "Note that this might take some time..."
                )
            if len(df) > 50:
                print(
                    f"Summaries will be calculated for {len(df)} Events, "
                    + "Note that this might take some time..."
                )

            # add procedure names
            df["Procedure"] = df["Event"].apply(lambda x: x.top_event)
            df = df[["Procedure", "Event", "Tags"]]
            df.reset_index(drop=True, inplace=True)

            event = df.columns.get_loc("Event")
            tags = df.columns.get_loc("Tags")
            if df["Tags"].str.contains(",").any():
                raise AttributeError("Cell can only contain one Tag at a time")

            # extract interpolated data for discrete events
            df["Time"] = df.apply(
                lambda row: list(
                    row[event]
                    .interpolated_values(
                        [row[tags]],
                        interval,
                        filter_expression,
                        dataserver,
                        paging_config=paging_config,
                    )
                    .to_records(index=True)
                ),
                axis=1,
            )

            taglist = convert_to_TagList(list(df["Tags"].unique()), dataserver)

        df = df.explode("Time")  # explode list to rows
        df["Time"] = df["Time"].apply(
            lambda x: [el for el in x] if not pd.isnull(x) else np.nan
        )  # numpy record to list
        if not col:
            df[["Time"] + [tag.name for tag in taglist]] = df["Time"].apply(
                pd.Series
            )  # explode list to columns
        else:
            df[["Time", "Value"]] = df["Time"].apply(
                pd.Series
            )  # explode list to columns
        df["Time"] = df["Time"].apply(
            lambda x: add_timezone(x) if not pd.isnull(x) else x
        )
        df.reset_index(drop=True, inplace=True)

        return df

    def interpol_continuous_extract(
        self,
        tag_list: List[Union[str, Tag]],
        interval: str,
        filter_expression: str = "",
        dataserver: PIServer = None,
        paging_config: AF.PI.PIPagingConfiguration = AF.PI.PIPagingConfiguration(
            AF.PI.PIPageType.EventCount, 1000
        ),
    ) -> pd.DataFrame:
        """dataframe of continous, interpolated values from the start of the
        first filtered event to the end of the last filtered event, for each
        procedure, on bottom level of condensed hierarchy

        Args:
            tag_list (List[Union[str, Tag]]): list of tags
            interval (str): interval to interpolate to
            filter_expression (str, optional): Filter expression.
                Defaults to "".
            dataserver (PIServer, optional): dataserver; necessary if the list
                is strings. Defaults to None.

        Returns:
            pd.DataFrame: Resultant dataframe
        """
        taglist = convert_to_TagList(tag_list, dataserver)

        # select events on bottem level of condensed hierarchy
        col_start = [
            col_name
            for col_name in self.df.columns
            if col_name.startswith("Starttime")
        ][-1]
        # sort chronologically by starttime
        self.df.sort_values(by=[col_start], ascending=True, inplace=True)

        print("building continuous extract table from condensed hierachy...")
        # select events on bottem level of condensed hierarchy
        col_event = [
            col_name
            for col_name in self.df.columns
            if col_name.startswith("Event")
        ][-1]

        df_base = self.df[[col_event]].copy()
        df_base.columns = ["Event"]
        # add procedure names
        df_base["Procedure"] = df_base["Event"].apply(lambda x: x.top_event)
        df_base = df_base[["Procedure", "Event"]]
        df_base.reset_index(drop=True, inplace=True)

        # extract interpolated data for continuous events, per procedure
        df_cont = pd.DataFrame()
        for proc, df_proc in df_base.groupby("Procedure"):
            starttime = df_proc["Event"].iloc[0].starttime
            endtime = df_proc["Event"].iloc[-1].endtime
            values = list(
                taglist.interpolated_values(
                    starttime,
                    endtime,
                    interval,
                    filter_expression,
                    paging_config=paging_config,
                ).to_records(index=True)
            )
            df_cont = pd.concat(
                [
                    df_cont,
                    pd.DataFrame(
                        [[proc, values]], columns=["Procedure", "Time"]
                    ),
                ],
                ignore_index=True,
            )

        df_cont = df_cont.explode("Time")  # explode list to rows
        df_cont["Time"] = df_cont["Time"].apply(
            lambda x: [el for el in x]
        )  # numpy record to list
        # pd.DataFrame(df['b'].tolist(), index=df.index) instead of
        # apply(pd.Series) could be faster
        df_cont[["Time"] + [tag.name for tag in taglist]] = df_cont[
            "Time"
        ].apply(
            pd.Series
        )  # explode list to columns
        df_cont["Time"] = df_cont["Time"].apply(lambda x: add_timezone(x))

        # add Event info back
        df_cont["Event"] = np.nan
        for event in df_base["Event"]:
            df_cont["Event"].loc[
                (df_cont["Time"] >= event.starttime)
                & (df_cont["Time"] <= event.endtime)
            ] = event

        # format
        df_cont = df_cont[
            ["Procedure", "Event", "Time"] + [tag.name for tag in taglist]
        ]
        df_cont.sort_values(by=["Time"], ascending=True, inplace=True)
        df_cont.reset_index(drop=True, inplace=True)

        return df_cont

    def recorded_extract(
        self,
        tag_list: List[Union[str, Tag]],
        filter_expression="",
        AFBoundaryType: BoundaryType = BoundaryType.INTERPOLATED,
        dataserver: PIServer = None,
        paging_config: AF.PI.PIPagingConfiguration = AF.PI.PIPagingConfiguration(
            AF.PI.PIPageType.EventCount, 1000
        ),
    ) -> Dict[str, Dict[str, pd.DataFrame]]:
        """Return nested dictionary (level 1: Procedures, Level 2: Tags) of
        recorded data extracts from the start of the first filtered event to
        the end of the last filtered event, for each procedure, on bottom
        level of condensed hierarchy

        Args:
            tag_list (List[Union[str, Tag]]): list of tags
            filter_expression (str, optional): Filter Expression.
                Defaults to "".
            AFBoundaryType (BoundaryType, optional): Boundary Type.
                Defaults to BoundaryType.INTERPOLATED.
            dataserver (PIServer, optional): dataserver; necessary if the list
                is strings. Defaults to None.

        Returns:
            Dict[str, Dict[str, pd.DataFrame]]: nested dictionary
                ProcedureName: {tagName: tagData}
        """
        taglist = convert_to_TagList(tag_list, dataserver)

        # select events on bottem level of condensed hierarchy
        col_start = [
            col_name
            for col_name in self.df.columns
            if col_name.startswith("Starttime")
        ][-1]
        # sort chronologically by starttime
        self.df.sort_values(by=[col_start], ascending=True, inplace=True)

        print("building recorded extract dict from condensed hierachy...")
        # select events on bottem level of condensed hierarchy
        col_event = [
            col_name
            for col_name in self.df.columns
            if col_name.startswith("Event")
        ][-1]

        df_base = self.df[[col_event]].copy()
        df_base.columns = ["Event"]
        # add procedure names
        df_base["Procedure"] = df_base["Event"].apply(lambda x: x.top_event)
        df_base = df_base[["Procedure", "Event"]]
        df_base.reset_index(drop=True, inplace=True)

        # extract recorded data for continuous events, per procedure
        dct = {}
        for proc, df_proc in df_base.groupby("Procedure"):
            starttime = df_proc["Event"].iloc[0].starttime
            endtime = df_proc["Event"].iloc[-1].endtime
            values = taglist.recorded_values(
                starttime,
                endtime,
                filter_expression,
                AFBoundaryType=AFBoundaryType,
                paging_config=paging_config,
            )
            for tag, df_rec in values.items():
                # add Event info back
                df_rec["Event"] = np.nan
                df_rec["Time"] = df_rec.index
                for event in df_base["Event"]:
                    df_rec["Event"].loc[
                        (df_rec["Time"] >= event.starttime)
                        & (df_rec["Time"] <= event.endtime)
                    ] = event
                    df_rec.reset_index(drop=True, inplace=True)
                values[tag] = df_rec[
                    [
                        "Event",
                        "Time",
                        "Data",
                    ]
                ]
            dct[proc] = values
        return dct

    def plot_continuous_extract(
        self,
        tag_list: List[Union[str, Tag]],
        nr_of_intervals: int,
        dataserver: PIServer = None,
        paging_config: AF.PI.PIPagingConfiguration = AF.PI.PIPagingConfiguration(
            AF.PI.PIPageType.EventCount, 1000
        ),
    ) -> Dict[str, Dict[str, pd.DataFrame]]:
        """Return nested dictionary (level 1: Procedures, Level 2: Tags) of
        continuous plot values from the start of the first filtered event to
        the end of the last filtered event for each procedure on the bottom level
        of condensed hierarchy. Each interval can produce up to 5 values if
        they are unique, the first value in the interval, the last value, the
        highest value, the lowest value and at most one exceptional point (bad
        status or digital state)

        Args:
            tag_list (List[Union[str, Tag]]): list of tags
            nr_of_intervals (int): number of intervals
            dataserver (PIServer, optional): dataserver; necessary if the list
                is strings. Defaults to None.

        Returns:
            Dict[str,Dict[str,pd.DataFrame]]:  nested dictionary
                ProcedureName: {tagName: tagData}
        """
        taglist = convert_to_TagList(tag_list, dataserver)

        # select events on bottem level of condensed hierarchy
        col_start = [
            col_name
            for col_name in self.df.columns
            if col_name.startswith("Starttime")
        ][-1]
        # sort chronologically by starttime
        self.df.sort_values(by=[col_start], ascending=True, inplace=True)

        print(
            "building continuous plot extract dict from condensed hierachy..."
        )
        # select events on bottem level of condensed hierarchy
        col_event = [
            col_name
            for col_name in self.df.columns
            if col_name.startswith("Event")
        ][-1]

        df_base = self.df[[col_event]].copy()
        df_base.columns = ["Event"]
        # add procedure names
        df_base["Procedure"] = df_base["Event"].apply(lambda x: x.top_event)
        df_base = df_base[["Procedure", "Event"]]
        df_base.reset_index(drop=True, inplace=True)

        # extract plot data for continuous events, per procedure
        dct = {}
        for proc, df_proc in df_base.groupby("Procedure"):
            starttime = df_proc["Event"].iloc[0].starttime
            endtime = df_proc["Event"].iloc[-1].endtime
            values = taglist.plot_values(
                starttime,
                endtime,
                nr_of_intervals,
                paging_config=paging_config,
            )
            for tag, df_rec in values.items():
                # add Event info back
                df_rec["Event"] = np.nan
                df_rec["Time"] = df_rec.index
                for event in df_base["Event"]:
                    df_rec["Event"].loc[
                        (df_rec["Time"] >= event.starttime)
                        & (df_rec["Time"] <= event.endtime)
                    ] = event
                    df_rec.reset_index(drop=True, inplace=True)
                values[tag] = df_rec[
                    [
                        "Event",
                        "Time",
                        "Data",
                    ]
                ]
            dct[proc] = values
        return dct

    def summary_extract(
        self,
        tag_list: List[Union[str, Tag]],
        summary_types: int,
        dataserver: PIServer = None,
        calculation_basis: CalculationBasis = CalculationBasis.TIME_WEIGHTED,
        time_type: TimestampCalculation = TimestampCalculation.AUTO,
        col: bool = False,
        paging_config: AF.PI.PIPagingConfiguration = AF.PI.PIPagingConfiguration(
            AF.PI.PIPageType.EventCount, 1000
        ),
    ) -> pd.DataFrame:
        """Return dataframe of summary values for events on bottom level of
        condensed hierarchy

        Args:
            tag_list (List[Union[str, Tag]]): list of tags
            summary_types (int): integers separated by '|'. List given
                below. E.g. "summary_types = 1|8" gives TOTAL and MAXIMUM

                - TOTAL = 1: A total over the time span
                - AVERAGE = 2: Average value over the time span
                - MINIMUM = 4: The minimum value in the time span
                - MAXIMUM = 8: The maximum value in the time span
                - RANGE = 16: The range of the values (max-min) in the time
                    span
                - STD_DEV = 32 : The sample standard deviation of the values
                    over the time span
                - POP_STD_DEV = 64: The population standard deviation of the
                    values over the time span
                - COUNT = 128: The sum of the event count (when the
                    calculation is event weighted). The sum of the event time
                        duration (when the calculation is time weighted.)
                - PERCENT_GOOD = 8192: The percentage of the data with a good
                    value over the time range. Based on time for time weighted
                        calculations, based on event count for event weigthed
                        calculations.
                - TOTAL_WITH_UOM = 16384: The total over the time span, with
                    the unit of measurement that's associated with the input
                    (or no units if not defined for the input)
                - ALL = 24831: A convenience to retrieve all summary types
                - ALL_FOR_NON_NUMERIC = 8320: A convenience to retrieve all
                    summary types for non-numeric data

            dataserver (PIServer, optional): dataserver; necessary if the list
                is strings. Defaults to None.
            calculation_basis (CalculationBasis, optional): Basis by which to
                calculate the summary statistic.
                Defaults to CalculationBasis.TIME_WEIGHTED.
            time_type (TimestampCalculation, optional): How the timestamp is
                calculated. Defaults to TimestampCalculation.AUTO.
            col (bool, optional): _description_. Defaults to False.

        Returns:
            pd.DataFrame: Resultant dataframe
        """

        print("building summary table from condensed hierarchy...")
        df = self.df.copy()

        # select events on bottom level of condensed hierarchy
        col_event = [
            col_name
            for col_name in self.df.columns
            if col_name.startswith("Event")
        ][-1]

        # performance checks
        maxi = max(df[col_event].apply(lambda x: x.duration))
        if maxi > pd.Timedelta("60 days"):
            print(
                f"Large Event(s) with duration up to {maxi} detected, "
                + "Note that this might take some time..."
            )
        if len(df) > 50:
            print(
                f"Summaries will be calculated for {len(df)} Events, Note"
                + " that this might take some time..."
            )

        # based on list of tags
        if not col:
            df = self.df[[col_event]].copy()
            df.columns = ["Event"]

            # add procedure names
            df["Procedure"] = df["Event"].apply(lambda x: x.top_event)
            df = df[["Procedure", "Event"]]
            df.reset_index(drop=True, inplace=True)

            taglist = convert_to_TagList(tag_list, dataserver)

            # extract summary data for discrete events
            df["Time"] = df["Event"].apply(
                lambda x: list(
                    x.summary(
                        taglist,
                        summary_types,
                        calculation_basis,
                        time_type,
                        paging_config=paging_config,
                    ).to_records(index=False)
                )
            )

        # based on column with tags
        if col:
            if len(tag_list) > 1:
                raise AttributeError(
                    f"You can only specify a single tag column at a time"
                )
            if tag_list[0] in self.df.columns:
                df = self.df[[col_event, tag_list[0]]].copy()
                df.columns = ["Event", "Tags_in"]
            else:
                raise AttributeError(
                    f"The column option was set to True, but {tag_list} is "
                    + "not a valid column name"
                )

            # add procedure names
            df["Procedure"] = df["Event"].apply(lambda x: x.top_event)
            df = df[["Procedure", "Event", "Tags_in"]]
            df.reset_index(drop=True, inplace=True)

            # just single request for each unique target
            for tg in df["Tags_in"].unique():
                tl = convert_to_TagList(
                    tg.replace(" ", "").split(","), dataserver
                )
                # https://stackoverflow.com/questions/39717809/insert-list-into-cells-which-meet-column-conditions
                df.loc[df["Tags_in"] == tg, "Tags"] = pd.Series(
                    [tl] * df.shape[0]
                )
            df.drop(columns="Tags_in", inplace=True)

            event = df.columns.get_loc("Event")
            tags = df.columns.get_loc("Tags")
            # extract summary data for discrete events
            df["Time"] = df.apply(
                lambda row: list(
                    row[event]
                    .summary(
                        row[tags],
                        summary_types,
                        calculation_basis,
                        time_type,
                        paging_config=paging_config,
                    )
                    .to_records(index=False)
                ),
                axis=1,
            )

        df = df.explode("Time")  # explode list to rows
        df["Time"] = df["Time"].apply(
            lambda x: [el for el in x]
        )  # numpy record to list
        df[["Tag", "Summary", "Value", "Time"]] = df["Time"].apply(
            pd.Series
        )  # explode list to columns
        df.reset_index(drop=True, inplace=True)

        return df

    def calc_summary_extract(
        self,
        interval: str,
        summary_types: int,
        expression: str = "",
        calculation_basis: CalculationBasis = CalculationBasis.TIME_WEIGHTED,
        time_type: TimestampCalculation = TimestampCalculation.AUTO,
        AFfilter_evaluation: ExpressionSampleType = ExpressionSampleType.EXPRESSION_RECORDED_VALUES,
        filter_interval: str = None,
        col: bool = False,
    ) -> pd.DataFrame:

        """Return dataframe of summary measures of calculations specified in expression,
        for discrete events at bottom level of the CondensedHierarchy.

        Args:
            interval (str): The bounding time for the evaluation period.
            summary_types (int): integers separated by '|'. List given
                below. E.g. "summary_types = 1|8" gives TOTAL and MAXIMUM

                - TOTAL = 1: A total over the time span
                - AVERAGE = 2: Average value over the time span
                - MINIMUM = 4: The minimum value in the time span
                - MAXIMUM = 8: The maximum value in the time span
                - RANGE = 16: The range of the values (max-min) in the time
                    span
                - STD_DEV = 32 : The sample standard deviation of the values
                    over the time span
                - POP_STD_DEV = 64: The population standard deviation of the
                    values over the time span
                - COUNT = 128: The sum of the event count (when the
                    calculation is event weighted). The sum of the event time
                        duration (when the calculation is time weighted.)
                - PERCENT_GOOD = 8192: The percentage of the data with a good
                    value over the time range. Based on time for time weighted
                        calculations, based on event count for event weigthed
                        calculations.
                - TOTAL_WITH_UOM = 16384: The total over the time span, with
                    the unit of measurement that's associated with the input
                    (or no units if not defined for the input)
                - ALL = 24831: A convenience to retrieve all summary types
                - ALL_FOR_NON_NUMERIC = 8320: A convenience to retrieve all
                    summary types for non-numeric data

            expression (raw string): A string containing the expression to be evaluated.
                The syntax for the expression generally follows the
                Performance Equation syntax as described in
                the PI Data Archive documentation.
            calculation_basis (CalculationBasis, optional): Basis by which to
                calculate the summary statistic.
                Defaults to CalculationBasis.TIME_WEIGHTED.
            time_type (TimestampCalculation, optional): How the timestamp is
                calculated. Defaults to TimestampCalculation.AUTO.
            AFfilter_evaluation (ExpressionSampleType, optional): Expression
                Type. Defaults to
                ExpressionSampleType.EXPRESSION_RECORDED_VALUES.
            col (bool, optional): Parameter to toggle the column functionality.
                When set to True on can pass a column name trough the 'expression' argument.
                Expressions wil be evaluated on a row-to-row basis based on column value.
                Defaults to False.

        Returns:
            pd.DataFrame: dataframe of summary measures
        """
        print("building calculation summary table from condensed hierarchy...")
        df = self.df.copy()

        # select events on bottom level of condensed hierarchy
        col_event = [
            col_name
            for col_name in self.df.columns
            if col_name.startswith("Event")
        ][-1]

        # performance checks
        maxi = max(df[col_event].apply(lambda x: x.duration))
        if maxi > pd.Timedelta("60 days"):
            print(
                f"Large Event(s) with duration up to {maxi} detected, "
                + "Note that this might trigger the collection limit..."
            )
        if len(df) > 50:
            print(
                f"Summaries will be calculated for {len(df)} Events, Note"
                + " that this might take some time..."
            )

        if not col:
            df = self.df[[col_event]].copy()
            df.columns = ["Event"]

            # add procedure names
            df["Procedure"] = df["Event"].apply(lambda x: x.top_event)
            df = df[["Procedure", "Event"]]
            df.reset_index(drop=True, inplace=True)

            # extract summary data for discrete events
            df["Time"] = df["Event"].apply(
                lambda x: list(
                    calc_summary(
                        starttime=x.starttime,
                        endtime=x.endtime,
                        interval=interval,
                        summary_types=summary_types,
                        expression=expression,
                        calculation_basis=calculation_basis,
                        time_type=time_type,
                        AFfilter_evaluation=AFfilter_evaluation,
                        filter_interval=filter_interval,
                    ).to_records(index=False)
                )
            )

        if col:
            if not type(expression) == str:
                raise AttributeError(
                    "Name of expression column should be of string type"
                )
            if expression in df.columns:
                df = self.df[[col_event, expression]].copy()
                df.columns = ["Event", "Expression"]

                df.reset_index(drop=True, inplace=True)

                # add procedure names
                df["Procedure"] = df["Event"].apply(lambda x: x.top_event)
                df = df[["Procedure", "Event", "Expression"]]
                df.reset_index(drop=True, inplace=True)

                event = df.columns.get_loc("Event")
                exp = df.columns.get_loc("Expression")
                # extract summary data for discrete events
                df["Time"] = df.apply(
                    lambda row: list(
                        calc_summary(
                            starttime=row[event].starttime,
                            endtime=row[event].endtime,
                            interval=interval,
                            summary_types=summary_types,
                            expression=row[exp],
                            calculation_basis=calculation_basis,
                            time_type=time_type,
                            AFfilter_evaluation=AFfilter_evaluation,
                            filter_interval=filter_interval,
                        ).to_records(index=False)
                    ),
                    axis=1,
                )
            else:
                raise AttributeError(
                    f"The column option was set to True, but {expression} "
                    + "is not a valid column name"
                )

        df = df.explode("Time")  # explode list to rows
        df["Time"] = df["Time"].apply(
            lambda x: [el for el in x] if not pd.isnull(x) else np.nan
        )  # numpy record to list
        df[["Summary", "Value", "Time"]] = df["Time"].apply(
            pd.Series
        )  # explode list to columns
        df.reset_index(drop=True, inplace=True)

        return df


# aux functions


def lambda_aux_add_attributes(x, attribute):
    try:
        return x.get_attribute_values([attribute])[attribute]
    except:
        return np.nan
