#
# Copyright 2020 IBM Corp.
# SPDX-License-Identifier: Apache-2.0
#
from urllib.parse import urlparse, quote
import requests
from fybrik_python_logging import logger, DataSetID, ForUser
from pyarrow.fs import S3FileSystem
from fybrik_python_vault import get_jwt_from_file, get_raw_secret_from_vault


def get_s3_credentials_from_vault(vault_credentials, datasetID, tls_min_version=None, verify=True, cert=None):
    """Get S3 credentials from Vault

    Args:
        vault_credentials (dictonary): Properties used for getting s3 credentials from Vault.
        datasetID (string): dataset ID.
        tls_min_version (string, optional): tls minimum version to use in the connection to Vault. Defaults to None.
        verify (optional): Either a boolean, in which case it controls whether we verify
        the Vault server's TLS certificate, or a string, in which case it must be a path
        to a CA bundle to use. Defaults to ``True``.
        cert (tuple, optional): the module ('cert', 'key') pair.

    Returns:
        S3 (access_key, secret_key, session_token).
    """
    jwt_file_path = vault_credentials.get('jwt_file_path', '/var/run/secrets/kubernetes.io/serviceaccount/token')
    jwt = get_jwt_from_file(jwt_file_path)
    vault_address = vault_credentials.get('address', 'https://localhost:8200')
    secret_path = vault_credentials.get('secretPath', '/v1/secret/data/cred')
    vault_auth = vault_credentials.get('authPath', '/v1/auth/kubernetes/login')
    role = vault_credentials.get('role', 'demo')

    credentials = get_raw_secret_from_vault(jwt, secret_path, vault_address, vault_auth,
                                            role, datasetID, tls_min_version, verify, cert)
    if not credentials:
        raise ValueError("Vault credentials are missing")
    if 'access_key' in credentials and 'secret_key' in credentials:
        session_token = None
        if 'session_token' in credentials and credentials['session_token']:
            session_token = credentials['session_token']
            logger.trace("session_token was provided in credentials read from Vault",
                         extra={DataSetID: datasetID})
        else:
            logger.trace("session_token was NOT provided in credentials read from Vault",
                         extra={DataSetID: datasetID})
        if credentials['access_key'] and credentials['secret_key']:
            return credentials['access_key'], credentials['secret_key'], session_token
        else:
            if not credentials['access_key']:
                logger.error("'access_key' must be non-empty",
                             extra={DataSetID: datasetID, ForUser: True})
            if not credentials['secret_key']:
                logger.error("'secret_key' must be non-empty",
                             extra={DataSetID: datasetID, ForUser: True})
    logger.error("Expected both 'access_key' and 'secret_key' fields in vault secret",
                 extra={DataSetID: datasetID, ForUser: True})
    raise ValueError("Vault credentials are missing")

def s3filesystem_from_config(s3_config, datasetID, tls_min_version=None, verify=True, cert=None):
    """Construct and return object of type S3FileSystem based on properties from the configuration

    Args:
        s3_config (dictinary): s3 configuration.
        datasetID (string): dataset ID.
        tls_min_version (string, optional): tls minimum version to use in the connection to Vault. Defaults to None.
        verify (optional): Either a boolean, in which case it controls whether we verify
        the Vault server's TLS certificate, or a string, in which case it must be a path
        to a CA bundle to use. Defaults to ``True``.
        cert (tuple, optional): the module ('cert', 'key') pair.

    Returns:
        An object of type S3FileSystem
    """
    endpoint = s3_config.get('endpoint_url')
    region = s3_config.get('region')

    credentials = s3_config.get('credentials', {})
    access_key = credentials.get('accessKey')
    secret_key = credentials.get('secretKey')

    if 'vault_credentials' in s3_config:
        logger.trace("reading s3 configuration from vault",
                     extra={DataSetID: datasetID})
        access_key, secret_key, session_token = get_s3_credentials_from_vault(
                s3_config.get('vault_credentials'), datasetID, tls_min_version, verify, cert)

    scheme, endpoint_override = _split_endpoint(endpoint)
    anonymous = not access_key

    return S3FileSystem(
        region=region,
        endpoint_override=endpoint_override,
        scheme=scheme,
        access_key=access_key,
        secret_key=secret_key,
        session_token=session_token,
        anonymous=anonymous
    )


def _split_endpoint(endpoint):
    if endpoint:
        parsed_endpoint = urlparse(endpoint)
        return parsed_endpoint.scheme, parsed_endpoint.netloc
    return None, None
