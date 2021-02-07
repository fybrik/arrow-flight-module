#
# Copyright 2020 IBM Corp.
# SPDX-License-Identifier: Apache-2.0
#
import json


# TODO: replace with a real ticket and not just a copy of command
class AFMTicket:
    def __init__(self, asset_name, columns, transformations, flight_ticket=None, partition_path=None):
        self._asset_name = asset_name
        self._columns = columns
        self._transformations = transformations
        self._flight_ticket = flight_ticket
        self._partition_path = partition_path

    @staticmethod
    def fromJSON(raw):
        return AFMTicket(**json.loads(raw))

    def toJSON(self):
        return json.dumps({
            "asset_name": self.asset_name,
            "columns": self.columns,
            "transformations": self.transformations,
            "flight_ticket": self.flight_ticket,
            "partition_path": self.partition_path,
        })

    @property
    def asset_name(self) -> str:
        return self._asset_name

    @property
    def columns(self) -> list:
        return self._columns

    @property
    def transformations(self) -> list:
        return self._transformations

    @property
    def flight_ticket(self) -> str:
        return self._flight_ticket

    @property
    def partition_path(self) -> str:
        return self._partition_path
