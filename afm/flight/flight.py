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

    def get_flight_info(self):
        return self.flight_client.get_flight_info(fl.FlightDescriptor.for_command(self.flight_command))

    def do_get(self, context, ticket):
        ticket_dict = json.loads(ticket.ticket.decode())
        flight_stream_reader = self.flight_client.do_get(Ticket(ticket_dict["flight_ticket"]))
        return flight_stream_reader.schema, self.batches(flight_stream_reader)

def flight_from_config(flight_config):
    endpoint = flight_config.get('endpoint_url')
    port = flight_config.get('port')
    flight_command = flight_config.get('flight_command')
    return Flight(endpoint, port, flight_command)
