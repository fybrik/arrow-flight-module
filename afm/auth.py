#
# Copyright 2020 IBM Corp.
# SPDX-License-Identifier: Apache-2.0
#
from fybrik_python_logging import logger
from pyarrow import flight
from pyarrow.flight import ServerAuthHandler
from .auth_handlers.auth_servers import NoopAuthHandler, HttpBasicServerAuthHandler

class AFMAuthHandler(ServerAuthHandler):
    def __init__(self, auth_config):
        super().__init__()
        if not auth_config:
            logger.info("no authentication configuration. Using NoopAuthHandler")
            self.auth_handler = NoopAuthHandler()
        elif 'basic' in auth_config:
            logger.info("basic authentication configuration. Using HttpBasicServerAuthHandler")
            self.auth_handler = HttpBasicServerAuthHandler(auth_config['basic'].get('credentials', None))
        else:
            raise NotImplementedError("Unknown authenticaion type")

    def authenticate(self, outgoing, incoming):
        self.auth_handler.authenticate(outgoing, incoming)

    def is_valid(self, token):
        return self.auth_handler.is_valid(token)
