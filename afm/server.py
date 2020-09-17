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
from pyarrow.fs import FileType

from .asset import Asset
from .command import AFMCommand
from .config import Config
from .pep.actions import transform
from .ticket import AFMTicket


class AFMFlightServer(fl.FlightServerBase):
    def __init__(self, config_path: str, port: int, *args, **kwargs):
        super(AFMFlightServer, self).__init__(
            "grpc://0.0.0.0:{}".format(port), *args, **kwargs)
        self.config_path = config_path

    def _remove_hidden_columns(self, schema: pa.Schema, asset: Asset):
        remove_columns = [action for action in asset.actions if action.name == "RemoveColumns"]
        for action in remove_columns:
            for column in action.columns:
                schema = schema.remove(schema.get_field_index(column))
        return schema

    def _infer_schema(self, asset):
        # TODO: change to always use just the dataset API
        if asset.format == "csv":
            return ds.dataset(asset.path, format=asset.format, filesystem=asset.filesystem).schema
        elif asset.format == "parquet":
            file_info = asset.filesystem.get_file_info([asset.path])[0]
            path = asset.path
            if file_info.type == FileType.Directory:
                path = os.path.join(path, "_metadata")
            with asset.filesystem.open_input_file(path) as f:
                return pq.read_schema(f)
        else:
            raise ValueError("unsupported format {}".format(self.format))

    def _read_asset(self, asset, columns=None):
        if asset.format == "parquet":
            # TODO: switch to using the dataset API directly to avoid loading entire table
            table = pq.read_table(
                asset.path, columns=columns, filesystem=asset.filesystem)
            batches = table.to_batches(max_chunksize=64*2**20)
            schema = table.schema
        elif asset.format == "csv":
            # TODO: I'm not sure if batch_size does anything here
            dataset = ds.dataset(asset.path, format="csv", filesystem=asset.filesystem)
            batches = ds.Scanner.from_dataset(dataset, columns=columns, batch_size=64*2**20).to_batches()
            schema = dataset.schema
        else:
            raise ValueError("unsupported format {}".format(asset.format))
            
        return schema, batches

    def get_flight_info(self, context, descriptor):
        cmd = AFMCommand(descriptor.command)

        with Config(self.config_path) as config:
            asset = Asset(config, cmd.asset_name)

        # Infer schema
        schema = self._infer_schema(asset)
        if cmd.columns is not None:
            schema = pa.schema([schema.field(name) for name in cmd.columns])
        self._remove_hidden_columns(schema, asset)

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
        #TODO: must also apply remove column actions here for security

        ticket_info: AFMTicket = AFMTicket.fromJSON(ticket.ticket)

        if ticket_info.columns is None:
            raise ValueError("Columns must be specified in ticket")

        with Config(self.config_path) as config:
            asset = Asset(config, ticket_info.asset_name)

        schema, batches = self._read_asset(asset, ticket_info.columns)
        print("read asset", schema, batches)
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
