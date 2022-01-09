#
# Copyright 2020 IBM Corp.
# SPDX-License-Identifier: Apache-2.0
#
import argparse
from afm.server import AFMFlightServer
from afm.logging import init_logger, logger

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='AFM Flight Server')
    parser.add_argument(
        '-p', '--port', type=int, default=8080, help='Listening port')
    parser.add_argument(
        '-c', '--config', type=str, default='/etc/conf/conf.yaml', help='Path to config file')
    parser.add_argument(
        '-l', '--loglevel', type=str, default='warning', help='logging level', 
        choices=['info', 'debug', 'warning', 'error', 'critical'])
    parser.add_argument('-r', '--color',
        default=False, type=lambda x: (str(x).lower() == 'true'),
        help='Color logs (True/False)')
    args = parser.parse_args()

    init_logger(args.loglevel.upper(), args.color)

    logger.info('about to run AFMFlightServer')
    server = AFMFlightServer(args.config, args.port)
    server.serve()
