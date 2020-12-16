#
# Copyright 2020 IBM Corp.
# SPDX-License-Identifier: Apache-2.0
#
import json


# TODO: replace with a real ticket and not just a copy of command
class AFMTicket:
    def __init__(self, asset_name, columns, passthrough_ticket=None):
        self._asset_name = asset_name
        self._columns = columns
        self._passthrough_ticket = passthrough_ticket

    @staticmethod
    def fromJSON(raw):
        return AFMTicket(**json.loads(raw))

    def toJSON(self):
        return json.dumps({
            "asset_name": self.asset_name,
            "columns": self.columns,
            "passthrough_ticket": self.passthrough_ticket,
        })

    @property
    def asset_name(self) -> str:
        return self._asset_name

    @property
    def columns(self) -> list:
        return self._columns

    @property
    def passthrough_ticket(self) -> str:
        return self._passthrough_ticket
