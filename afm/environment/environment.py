#
# Copyright 2020 IBM Corp.
# SPDX-License-Identifier: Apache-2.0
#

import os
import ssl
from os.path import exists
from fybrik_python_logging import logger


# constants
MIN_TLS_VERSION = "MIN_TLS_VERSION"
DATA_DIR = "DATA_DIR"

CERT_FILE_PATH = os.environ.get(DATA_DIR) + "/tls-cert/tls.crt"
CERT_FILE_KEY = os.environ.get(DATA_DIR) + "/tls-cert/tls.key"
CACERTS_FILE = os.environ.get(DATA_DIR) + "/tls-cacert/ca.crt"

def get_env_var_value(name):
    return os.environ.get(name)

def print_env_vars():
    data_dir = os.environ.get(DATA_DIR)
    if data_dir:
      logger.trace("DATA_DIR: " + data_dir)
    min_tls_version = os.environ.get(MIN_TLS_VERSION)
    if min_tls_version:
      logger.trace("MIN_TLS_VERSION: " + min_tls_version)

def get_min_tls_version():
    min_version = os.environ.get(MIN_TLS_VERSION)
    # ref: https://docs.python.org/3/library/ssl.html#ssl.TLSVersion.MINIMUM_SUPPORTED
    rv = None

    if min_version == "SSL-3":
      rv = ssl.TLSVersion.SSLv3
    elif min_version == "TLS-1":
      rv = ssl.TLSVersion.TLSv1
    elif min_version == "TLS-1.1":
      rv = ssl.TLSVersion.TLSv1_1
    elif min_version == "TLS-1.2":
      rv = ssl.TLSVersion.TLSv1_2
    elif min_version == "TLS-1.3":
      rv = ssl.TLSVersion.TLSv1_3
    else:
      logger.trace('MinTLSVersion is set to the system default value')
      return rv
            
    logger.trace("MinTLSVersion is set to " + min_version)
    return rv
  
def get_certs():
    """
    returns the private key and certificate if such provided.
    """
    if not exists(CERT_FILE_PATH) or not exists(CERT_FILE_KEY):
       return ()
    return (CERT_FILE_PATH, CERT_FILE_KEY)

def get_cacert_path():
    """
    returns the CA certificate file if such provided.
    """
    if exists(CACERTS_FILE):
        return CACERTS_FILE
    return ""
