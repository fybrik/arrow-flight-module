#
# Copyright 2020 IBM Corp.
# SPDX-License-Identifier: Apache-2.0
#
import json


class AFMCommand:
    def __init__(self, cmd):
        self.raw = json.loads(cmd)

    @property
    def asset_name(self) -> str:
        return self.raw['asset']

    @property
    def columns(self) -> list:
        return self.raw.get('columns', None)

    @property
    def transformations(self) -> list:
        return self.raw.get('transformations', None)
