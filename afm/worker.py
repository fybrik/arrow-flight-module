#
# Copyright 2020 IBM Corp.
# SPDX-License-Identifier: Apache-2.0
#

class Worker:
    def __init__(self, config: dict):
        self._name = config.get("name")
        self._address = config.get("address")
        self._port = config.get("port")

    @property
    def name(self):
        return self._name

    @property
    def address(self):
        return self._address

    @property
    def port(self):
        return self._port

    @property
    def path(self):
        return self._path

def workers_from_config(workers_list: list):
    workers = []
    for w in workers_list:
        workers.append(Worker(w))
    return workers
