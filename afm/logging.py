#
# Copyright 2020 IBM Corp.
# SPDX-License-Identifier: Apache-2.0
#
import logging

logger = logging.getLogger('arrow-flight-module')

def init_logger(loglevel_arg):
    loglevel = getattr(logging, loglevel_arg, logging.WARNING)
    logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%d/%m/%Y %H:%M:%S', level=loglevel)
