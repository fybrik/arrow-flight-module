#
# Copyright 2020 IBM Corp.
# SPDX-License-Identifier: Apache-2.0
#
import os

import pyarrow as pa
import pyarrow.flight as fl
import pyarrow.parquet as pq
import pyarrow.csv as csv
import pyarrow.dataset as ds
from pyarrow.fs import FileSelector

from .asset import Asset
from .command import AFMCommand
from .config import Config
from .pep import transform, transform_schema
from .ticket import AFMTicket


class AFMFlightServer(fl.FlightServerBase):
    def __init__(self, config_path: str, port: int, *args, **kwargs):
        super(AFMFlightServer, self).__init__(
            "grpc://0.0.0.0:{}".format(port), *args, **kwargs)
        self.config_path = config_path

    def _get_dataset(self, asset):
        # FIXME(roee88): bypass https://issues.apache.org/jira/browse/ARROW-7867
        data_files = [f.path for f in asset.filesystem.get_file_info(FileSelector(asset.path, allow_not_found=True, recursive=True)) if f.size]
        if not data_files:
            data_files = [asset.path] # asset.path is probably a single file

        if asset.format == "csv" or asset.format == "parquet":
            return ds.dataset(data_files, format=asset.format, filesystem=asset.filesystem)

        raise ValueError("unsupported format {}".format(asset.format))

    def _infer_schema(self, asset):
        dataset = self._get_dataset(asset)
        return dataset.schema
    
    def _read_asset(self, asset, columns=None):
        # TODO(roee88): should use `batches = scanner.to_batches()` to avoid loading all to memory after tuning performance
        dataset = self._get_dataset(asset)
        scanner = ds.Scanner.from_dataset(dataset, columns=columns, batch_size=64*2**20)
        table = scanner.to_table()
        batches = table.to_batches(max_chunksize=64*2**20)
        return dataset.schema, batches

    def get_flight_info(self, context, descriptor):
        cmd = AFMCommand(descriptor.command)

        with Config(self.config_path) as config:
            asset = Asset(config, cmd.asset_name)

        # Infer schema
        schema = self._infer_schema(asset)
        schema = transform_schema(asset.actions, schema)

        # Build endpoint to this server
        endpoints = []
        ticket = AFMTicket(cmd.asset_name, schema.names)
        locations = []
        local_address = os.getenv("MY_POD_IP")
        if local_address:
            locations += "grpc://{}:{}".format(local_address, self.port)
        endpoints.append(fl.FlightEndpoint(ticket.toJSON(), locations))

        return fl.FlightInfo(schema, descriptor, endpoints, -1, -1)

    def do_get(self, context, ticket: fl.Ticket):
        ticket_info: AFMTicket = AFMTicket.fromJSON(ticket.ticket)
        if ticket_info.columns is None:
            raise ValueError("Columns must be specified in ticket")

        with Config(self.config_path) as config:
            asset = Asset(config, ticket_info.asset_name)

        schema, batches = self._read_asset(asset, ticket_info.columns)

        schema = transform_schema(asset.actions, schema)
        batches = transform(asset.actions, batches)        
        return fl.GeneratorStream(schema, batches)

    def do_put(self, context, descriptor, reader, writer):
        raise NotImplementedError("do_put")

    def get_schema(self, context, descriptor):
        info = self.get_flight_info(context, descriptor)
        return fl.SchemaResult(info.schema)

    def list_flights(self, context, criteria):
        raise NotImplementedError("list_flights")

    def list_actions(self, context):
        raise NotImplementedError("list_actions")

    def do_action(self, context, action):
        raise NotImplementedError("do_action")
