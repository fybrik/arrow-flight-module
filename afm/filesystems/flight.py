#
# Copyright 2020 IBM Corp.
# SPDX-License-Identifier: Apache-2.0
#

import pyarrow.flight as fl
import json

from pyarrow.flight import FlightEndpoint, Ticket

class Flight:
    def __init__(self, endpoint, port, flight_command):
        self.flight_client = fl.connect("grpc://{}:{}".format(endpoint, port))
        self.flight_command = flight_command

    def batches(self, reader):
        for chunk in reader:
            yield chunk.data

    def get_flight_info(self, flight_command):
        return self.flight_client.get_flight_info(fl.FlightDescriptor.for_command(flight_command))

    def do_get(self, context, ticket):
        ticket_dict = json.loads(ticket.ticket.decode())
        flight_stream_reader = self.flight_client.do_get(Ticket(ticket_dict["flight_ticket"]))
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

def flight_from_config(flight_config):
    endpoint = flight_config.get('endpoint_url')
    port = flight_config.get('port')
    flight_command = flight_config.get('flight_command')
    return Flight(endpoint, port, flight_command)
