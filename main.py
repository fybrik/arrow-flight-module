#
# Copyright 2020 IBM Corp.
# SPDX-License-Identifier: Apache-2.0
#
import argparse
from afm.server import AFMFlightServer
import logging

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='AFM Flight Server')
    parser.add_argument(
        '-p', '--port', type=int, default=8080, help='Listening port')
    parser.add_argument(
        '-c', '--config', type=str, default='/etc/conf/conf.yaml', help='Path to config file')
    parser.add_argument(
        '-l', '--loglevel', type=str, default='warning', help='logging level', 
        choices=['info', 'debug', 'warning', 'error', 'critical'])
    args = parser.parse_args()

    loglevel = getattr(logging, args.loglevel.upper(), logging.WARNING)
    logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%d/%m/%Y %H:%M:%S', level=loglevel)

    server = AFMFlightServer(args.config, args.port)
    server.serve()
