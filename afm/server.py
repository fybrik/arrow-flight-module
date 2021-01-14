#
# Copyright 2020 IBM Corp.
# SPDX-License-Identifier: Apache-2.0
#

import logging
import os

import pyarrow as pa
import pyarrow.flight as fl
import pyarrow.parquet as pq
import pyarrow.csv as csv
import pyarrow.dataset as ds
from pyarrow.fs import FileSelector

from .asset import asset_from_config
from .command import AFMCommand
from .config import Config
from .pep import transform, transform_schema, actions
from .ticket import AFMTicket
from .worker import workers_from_config

class AFMFlightServer(fl.FlightServerBase):
    def __init__(self, config_path: str, port: int, *args, **kwargs):
        super(AFMFlightServer, self).__init__(
            "grpc://0.0.0.0:{}".format(port), *args, **kwargs)
        self.config_path = config_path

    def _get_dataset(self, asset):
        # FIXME(roee88): bypass https://issues.apache.org/jira/browse/ARROW-7867
        selector = FileSelector(asset.path, allow_not_found=True, recursive=True)
        try:
            data_files = [f.path for f in asset.filesystem.get_file_info(selector) if f.size]
        except NotADirectoryError:
            data_files = None
        if not data_files:
            data_files = [asset.path] # asset.path is probably a single file

        if asset.format == "csv" or asset.format == "parquet":
            return ds.dataset(data_files, format=asset.format, filesystem=asset.filesystem), data_files

        raise ValueError("unsupported format {}".format(asset.format))

    def _infer_schema(self, asset):
        dataset, data_files = self._get_dataset(asset)
        return dataset.schema, data_files

    def _filter_columns(self, schema, columns):
        return pa.schema([pa.field(c, schema.field(c).type)
			for c in columns])

    def _read_asset(self, asset, columns=None):
        dataset, data_files = self._get_dataset(asset)
        scanner = ds.Scanner.from_dataset(dataset, columns=columns, batch_size=64*2**20)
        batches = scanner.to_batches()
        if columns:
           return self._filter_columns(dataset.schema, columns), batches
        return dataset.schema, batches

    def _get_endpoints(self, tickets, locations):
        endpoints = []
        i = 0
        for ticket in tickets:
            if locations:
                endpoints.append(fl.FlightEndpoint(ticket.toJSON(), [locations[i]]))
                i = (i + 1) % len(locations)
            else:
                endpoints.append(fl.FlightEndpoint(ticket.toJSON(), []))
        return endpoints


    def _get_locations(self, workers):
        locations = []
        if workers:
            for worker in workers:
                locations.append("grpc://{}:{}".format(worker.address, worker.port))
        else:
            local_address = os.getenv("MY_POD_IP")
            if local_address:
                locations += "grpc://{}:{}".format(local_address, self.port)

        return locations

    def get_flight_info(self, context, descriptor):
        logging.info('get_flight_info: command={}'.format(descriptor.command))
        cmd = AFMCommand(descriptor.command)

        with Config(self.config_path) as config:
            asset = asset_from_config(config, cmd.asset_name)
            workers = workers_from_config(config.workers)

        if asset.connection_type == 'flight':
            passthrough_flight_info = asset.flight.get_flight_info()
            schema = passthrough_flight_info.schema
        else:
            # Infer schema
            schema, data_files = self._infer_schema(asset)

        if cmd.columns:
            schema = self._filter_columns(schema, cmd.columns)
        schema = transform_schema(asset.actions, schema)

        # Build endpoint to this server
        locations = self._get_locations(workers)

        tickets = []
        if asset.connection_type == 'flight':
            for endpoint in passthrough_flight_info.endpoints:
                tickets.append(AFMTicket(cmd.asset_name, schema.names, endpoint.ticket.ticket.decode()))
        else:
            # Build endpoint to this server
            for f in data_files:
                tickets.append(AFMTicket(cmd.asset_name, schema.names, partition_path=f))

        endpoints = self._get_endpoints(tickets, locations)
        return fl.FlightInfo(schema, descriptor, endpoints, -1, -1)

    def do_get(self, context, ticket: fl.Ticket):
        logging.info('do_get: ticket={}'.format(ticket.ticket))
        ticket_info: AFMTicket = AFMTicket.fromJSON(ticket.ticket)
        if ticket_info.columns is None:
            raise ValueError("Columns must be specified in ticket")

        with Config(self.config_path) as config:
            asset = asset_from_config(config, ticket_info.asset_name, partition_path=ticket_info.partition_path)

        if asset.connection_type == "flight":
            schema, batches = asset.flight.do_get(context, ticket)
            if ticket_info.columns:
                asset.add_action(actions.FilterColumns(
					columns=ticket_info.columns,
					description="filter columns",
					options=None))
        else:
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
