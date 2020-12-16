#
# Copyright 2020 IBM Corp.
# SPDX-License-Identifier: Apache-2.0
#

import pyarrow.flight as fl
import json

from pyarrow.flight import Ticket, FlightInfo, FlightEndpoint

class Passthrough:
    def __init__(self, endpoint, port):
        self.flight_client = fl.connect("grpc://{}:{}".format(endpoint, port))

    def batches(self, reader):
        for chunk in reader:
            yield chunk.data

    def get_flight_info(self, cmd, path):
        cmd_dict = json.loads(cmd.decode())
        asset_name = cmd_dict["asset"]
        cmd_dict["asset"] = path
        flight_info = self.flight_client.get_flight_info(fl.FlightDescriptor.for_command(json.dumps(cmd_dict)))

        endpoints = []
        for endpoint in flight_info.endpoints:
            ticket_dict = json.loads(endpoint.ticket.ticket.decode())
            ticket_dict["passthrough_asset"] = asset_name
            endpoints.append(FlightEndpoint(Ticket(json.dumps(ticket_dict)), endpoint.locations))
        return FlightInfo(flight_info.schema, flight_info.descriptor,
                endpoints, flight_info.total_records,
                flight_info.total_bytes)

    def do_get(self, context, ticket):
        flight_stream_reader = self.flight_client.do_get(ticket)
        return flight_stream_reader.schema, self.batches(flight_stream_reader)

    def do_put(self, context, descriptor, reader, writer):
        raise NotImplementedError("do_put not implemented")

    def get_schema(self, context, descriptor):
        raise NotImplementedError("get_schema not implemented")
        #info = self.get_flight_info(context, descriptor)
        #return fl.SchemaResult(info.schema)

    def list_flights(self, context, criteria):
        raise NotImplementedError("list_flights not implemented")

    def list_actions(self, context):
        raise NotImplementedError("list_actions not implemented")

    def do_action(self, context, action):
        raise NotImplementedError("do_action not implemented")

def passthrough_from_config(passthrough_config):
    endpoint = passthrough_config.get('endpoint_url')
    port = passthrough_config.get('port')  
    return Passthrough(endpoint, port)
