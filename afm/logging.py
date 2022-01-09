#
# Copyright 2020 IBM Corp.
# SPDX-License-Identifier: Apache-2.0
#
import logging

FORMATS = {}

class FybrikFormatter(logging.Formatter):
    def format(self, record):
        log_fmt = FORMATS.get(record.levelno)
        formatter = logging.Formatter(log_fmt, "%H:%M%p")
        return formatter.format(record)

logger = logging.getLogger('arrow-flight-module')

def init_logger(loglevel_arg, color=False):
    if color:
        bright_black = "\x1b[90m"
        cyan = "\x1b[36m"
        yellow = "\x1b[33m"
        green = "\x1b[32m"
        red = "\x1b[31m"
        bold_red = "\x1b[31;1m"
        reset = "\x1b[0m"
        prefix = bright_black + "%(asctime)s" + reset + " "
        postfix = " %(filename)s:%(lineno)d " + cyan + ">" + reset + " %(message)s"

        FORMATS[logging.DEBUG] = prefix + yellow + "DBG" + reset + postfix
        FORMATS[logging.INFO] = prefix + green + "INF" + reset + postfix
        FORMATS[logging.WARNING] = prefix + red + "WRN" + reset + postfix
        FORMATS[logging.ERROR] = prefix + bold_red + "ERR" + reset + postfix
        FORMATS[logging.CRITICAL] = prefix + bold_red + "FTL" + reset + postfix
    else:
        prefix = "%(asctime)s "
        postfix = " %(filename)s:%(lineno)d > %(message)s"

        FORMATS[logging.DEBUG] = prefix + "DBG" + postfix
        FORMATS[logging.INFO] = prefix + "INF" + postfix
        FORMATS[logging.WARNING] = prefix + "WRN" + postfix
        FORMATS[logging.ERROR] = prefix + "ERR" + postfix
        FORMATS[logging.CRITICAL] = prefix + "FTL" + postfix

    loglevel = getattr(logging, loglevel_arg, logging.WARNING)
    logger.setLevel(loglevel)
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)
    ch.setFormatter(FybrikFormatter())
    logger.addHandler(ch)
