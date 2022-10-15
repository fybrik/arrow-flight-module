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

def get_min_tls_version():
    min_version = os.environ.get(MIN_TLS_VERSION)
    # ref: https://docs.python.org/3/library/ssl.html#ssl.TLSVersion.MINIMUM_SUPPORTED
    rv = None
    match min_version:
        case "SSL-3":
          rv = ssl.TLSVersion.SSLv3
        case "TLS-1":
          rv = ssl.TLSVersion.TLSv1
        case "TLS-1.1":
          rv = ssl.TLSVersion.TLSv1_1
        case "TLS-1.2":
          rv = ssl.TLSVersion.TLSv1_2
        case "TLS-1.3":
          rv = ssl.TLSVersion.TLSv1_3
        case _:
            logger.debug('MinTLSVersion is set to the system default value')
            return rv
            
    logger.debug("MinTLSVersion is set to " + min_version)
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

        
          
        
