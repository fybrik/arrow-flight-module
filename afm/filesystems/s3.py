#
# Copyright 2020 IBM Corp.
# SPDX-License-Identifier: Apache-2.0
#
from urllib.parse import urlparse, quote
import requests
import json
from fybrik_python_logging import logger, DataSetID, ForUser
from pyarrow.fs import S3FileSystem
# from afm.filesystems.vault import get_credentials_from_vault
from fybrik_python_vault import get_jwt_from_file, get_raw_secret_from_vault


def get_s3_credentials_from_vault(vault_credentials, datasetID):
    jwt_file_path = vault_credentials.get('jwt_file_path', '/var/run/secrets/kubernetes.io/serviceaccount/token')
    jwt = get_jwt_from_file(jwt_file_path)
    vault_address = vault_credentials.get('address', 'https://localhost:8200')
    secret_path = vault_credentials.get('secretPath', '/v1/secret/data/cred')
    vault_auth = vault_credentials.get('authPath', '/v1/auth/kubernetes/login')
    role = vault_credentials.get('role', 'demo')
    credentials = get_raw_secret_from_vault(jwt, secret_path, vault_address, vault_auth, role, datasetID)
    if not credentials:
        raise ValueError("Vault credentials are missing")
    if 'access_key' in credentials and 'secret_key' in credentials:
        if credentials['access_key'] and credentials['secret_key']:
            return credentials['access_key'], credentials['secret_key']
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

def s3filesystem_from_config(s3_config, datasetID):
    endpoint = s3_config.get('endpoint_url')
    region = s3_config.get('region')

    credentials = s3_config.get('credentials', {})
    access_key = credentials.get('accessKey')
    secret_key = credentials.get('secretKey')

    secret_provider = credentials.get('secretProvider')

    if 'vault_credentials' in s3_config:
        logger.trace("reading s3 configuration from vault",
                     extra={DataSetID: datasetID})
        access_key, secret_key = get_s3_credentials_from_vault(
                s3_config.get('vault_credentials'), datasetID)
    elif secret_provider:
        logger.trace("reading s3 configuration from secret provider",
                     extra={DataSetID: datasetID})
        r = requests.get(secret_provider)
        r.raise_for_status()
        response = r.json()
        endpoint = response.get('endpoint_url') or endpoint
        region = response.get('region') or region
        access_key = response.get('access_key') or access_key
        secret_key = response.get('secret_key') or secret_key

    scheme, endpoint_override = _split_endpoint(endpoint)
    anonymous = not access_key

    return S3FileSystem(
        region=region,
        endpoint_override=endpoint_override,
        scheme=scheme,
        access_key=access_key,
        secret_key=secret_key,
        anonymous=anonymous
    )


def _split_endpoint(endpoint):
    if endpoint:
        parsed_endpoint = urlparse(endpoint)
        return parsed_endpoint.scheme, parsed_endpoint.netloc
    return None, None
