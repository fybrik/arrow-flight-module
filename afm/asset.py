#
# Copyright 2020 IBM Corp.
# SPDX-License-Identifier: Apache-2.0
#
import importlib
import os
import sys

from afm.config import Config
from afm.pep import registry, consolidate_actions
from afm.filesystems.s3 import s3filesystem_from_config
from afm.filesystems.httpfs import httpfs_from_config
from afm.flight.flight import flight_from_config
from afm.environment.environment import get_cacert_path, get_certs, get_min_tls_version
from fybrik_python_logging import logger, DataSetID
from pyarrow.fs import LocalFileSystem

def asset_from_config(config: Config, asset_name: str, partition_path=None, capability=""):
    connection_type = config.connection_type(asset_name, capability)
    if connection_type in ['s3', 'httpfs', 'localfs']:
        return FileSystemAsset(config, asset_name, partition_path, capability)
    elif connection_type == 'flight':
        return FlightAsset(config, asset_name, capability=capability)
    raise ValueError(
        "Unsupported connection type: {}".format(config.connection_type))

class Asset:
    def __init__(self, config: Config, asset_name: str, partition_path=None, capability=""):
        asset_config = config.for_asset(asset_name, capability=capability)
        self._config = asset_config
        self._actions = Asset._actions_for_asset(asset_config, config.plugin_dir)
        self._format = asset_config.get("format")
        if partition_path:
            self._path = partition_path
        else:
            self._path = asset_config.get("path")
        self._name = asset_config.get("name")

    def add_action(self, action):
        self._actions.insert(0, action)

    @property
    def actions(self):
        return self._actions

    @property
    def name(self):
        return self._name

    @property
    def format(self):
        return self._format

    @property
    def path(self):
        return self._path

    @property
    def connection_type(self):
        return self._config['connection']['type']

    @staticmethod
    def _try_to_find_plugin(plugin_name: str, plugin_dir: str):
        if plugin_dir:
            python_filename = plugin_name + ".py"
            if python_filename in os.listdir(plugin_dir):
                module = importlib.import_module(plugin_name)
                cls = getattr(module, plugin_name)
                registry[plugin_name] = cls
                return cls
        logger.error("plugin " + plugin_name + " not found")
        return None

    @staticmethod
    def _actions_for_asset(asset_config: dict, plugin_dir: str):
        def build_action(x):
            action_name = x["action"]
            if action_name in registry:
                cls = registry[action_name]
            else:
                cls = Asset._try_to_find_plugin(action_name, plugin_dir)
            return cls(description=x["description"], columns=x.get("columns"), options=x.get("options"))

        transformations = asset_config.get("transformations")
        if not transformations:
            transformations = []
        # Create a list of Action objects from the transformations configuration
        actions = [build_action(x) for x in transformations]
        # Consolidate identical actions to keep the asset.actions efficient
        return consolidate_actions(actions)

class FileSystemAsset(Asset):
    def __init__(self, config: Config, asset_name: str, partition_path=None, capability=""):
        super().__init__(config, asset_name, partition_path, capability)
        self._filesystem = FileSystemAsset._filesystem_for_asset(self._config)

    @staticmethod
    def _filesystem_for_asset(asset_config: dict):
        connection = asset_config['connection']
        connection_type = connection['type']
        dataSetID = asset_config['name']
        if connection_type == "s3":
            verify = None
            ca_cert_path = get_cacert_path()
            if ca_cert_path != "":
                logger.trace("set cacert path to " + ca_cert_path, extra={DataSetID: dataSetID})
                verify = ca_cert_path

            cert = None
            certs_tuple = get_certs()
            if certs_tuple:
                st = ' '
                logger.trace("set certs tuple to: " + st.join(certs_tuple), extra={DataSetID: dataSetID})
                cert = certs_tuple
            tls_min_version = get_min_tls_version()
            return s3filesystem_from_config(connection["s3"], dataSetID, tls_min_version,
                                            verify, cert)
        elif connection_type == "localfs":
            return LocalFileSystem()
        elif connection_type == "httpfs":
            return httpfs_from_config()
        raise ValueError(
            "Unsupported connection type: {}".format(connection_type))

    @property
    def filesystem(self):
        return self._filesystem

class FlightAsset(Asset):
    def __init__(self, config: Config, asset_name: str, capability=""):
        super().__init__(config, asset_name, capability=capability)
        self._flight = flight_from_config(self._config['connection']['flight'])

    @property
    def flight(self):
        return self._flight
