#
# Copyright 2020 IBM Corp.
# SPDX-License-Identifier: Apache-2.0
#
import logging
import json_log_formatter
import time

logger = logging.getLogger('arrow-flight-module')

class FybrikFormatter(json_log_formatter.JSONFormatter):
    def json_record(self, message: str, extra: dict, record: logging.LogRecord) -> dict:
        extra['message'] = message
        extra['level'] = record.levelname
        extra['name'] = record.name
        extra['file'] = record.filename
        extra['lineno'] = record.lineno
        extra['funcName'] = record.funcName
        extra['time'] = time.ctime(record.created)
        return extra

def init_logger(loglevel_arg, json_format=True):
    loglevel = getattr(logging, loglevel_arg, logging.WARNING)
    logger.setLevel(loglevel)
    if json_format:
        ch = logging.StreamHandler()
        ch.setLevel(loglevel)
        ch.setFormatter(FybrikFormatter())
        logger.addHandler(ch)
    else:
        logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%d/%m/%Y %H:%M:%S', level=loglevel)
