#
# Copyright 2020 IBM Corp.
# SPDX-License-Identifier: Apache-2.0
#
import httpfs
import os

from afm.config import Config
from afm.pep import registry, consolidate_actions
from afm.s3 import s3filesystem_from_config

from pyarrow.fs import LocalFileSystem, PyFileSystem, FSSpecHandler

def httpfs_from_config(httpfs_config):
    fs = httpfs.fs.HttpFs(httpfs_config.get('endpoint_url'))
    return PyFileSystem(FSSpecHandler(fs))

class Asset:
    def __init__(self, config: Config, asset_name: str):
        asset_config = config.for_asset(asset_name)
        self._config = asset_config
        self._filesystem = Asset._filesystem_for_asset(asset_config)
        self._actions = Asset._actions_for_asset(asset_config)
        self._format = asset_config.get("format")
        self._path = asset_config.get("path")

    @property
    def filesystem(self):
        return self._filesystem

    @property
    def actions(self):
        return self._actions

    @property
    def name(self):
        return self._config.get("name")

    @property
    def format(self):
        return self._config.get("format")

    @property
    def path(self):
        return self._config.get("path")

    @staticmethod
    def _filesystem_for_asset(asset_config: dict):
        connection = asset_config['connection']
        connection_type = connection['type']
        if connection_type == "s3":
            return s3filesystem_from_config(connection["s3"])
        elif connection_type == "localfs":
            return LocalFileSystem()
        elif connection_type == "httpfs":
            return httpfs_from_config(connection["httpfs"])
        raise ValueError(
            "Unsupported connection type: {}".format(connection_type))

    @staticmethod
    def _actions_for_asset(asset_config: dict):
        def build_action(x):
            cls = registry[x["action"]]
            return cls(description=x["description"], columns=x.get("columns"), options=x.get("options"))

        # Create a list of Action objects from the transformations configuration
        actions = [build_action(x) for x in asset_config.get("transformations", [])]
        # Consolidate identical actions to keep the asset.actions efficient
        return consolidate_actions(actions)
