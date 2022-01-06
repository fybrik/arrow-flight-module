# Copyright 2020 IBM Corp.
# SPDX-License-Identifier: Apache-2.0

from afm.logging import logger
import requests

def get_jwt_from_file(file_name):
    """
    Getting a jwt from a file.
    Typically, an SA token, which would be at: /var/run/secrets/kubernetes.io/serviceaccount/token
    """
    with open(file_name) as f:
        return f.read()

def vault_jwt_auth(jwt, vault_address, vault_path, role):
    """Authenticate against Vault using a JWT token (i.e., k8s sa token)"""
    full_auth_path = vault_address + vault_path
    logger.debug("full_auth_path = %s", str(full_auth_path))
    json = {"jwt": jwt, "role": role}
    response = requests.post(full_auth_path, json=json)
    if response.status_code == 200:
        return response.json()
    logger.critical("Got error code %d from Vault authentication", response.status_code)
    logger.critical("Error response: %s", str(response.json()))
    return None

def get_raw_secret_from_vault(jwt, secret_path, vault_address, vault_path, role):
    """Get a raw secret from vault by providing a valid jwt token"""
    vault_auth_response = vault_jwt_auth(jwt, vault_address, vault_path, role)
    if vault_auth_response is None:
        logger.critical("Empty vault authorization response")
        return None
    if not "auth" in vault_auth_response or not "client_token" in vault_auth_response["auth"]:
        logger.critical("Malformed vault authorization response")
        return None
    client_token = vault_auth_response["auth"]["client_token"]
    logger.debug("client_token: %s", str(client_token))
    secret_full_path = vault_address + secret_path
    logger.debug("secret_full_path = %s", str(secret_full_path))
    response = requests.get(secret_full_path, headers={"X-Vault-Token" : client_token})
    logger.critical("Status code from Vault response: " + str(response.status_code))
    if response.status_code == 200:
        response_json = response.json()
        if 'data' in response_json:
            return response_json['data']
        else:
            logger.critical("Malformed secret response. Expected the 'data' field in JSON")
    else:
        logger.critical("Got error code %d requesting Vault secret", response.status_code)
        logger.critical("Error response: %s", str(response.json()))
    return None

def get_credentials_from_vault(vault_credentials):
    jwt_file_path = vault_credentials.get('jwt_file_path', '/var/run/secrets/kubernetes.io/serviceaccount/token')
    logger.critical("jwt_file_path = %s", str(jwt_file_path))
    jwt = get_jwt_from_file(jwt_file_path)
    vault_address = vault_credentials.get('address', 'https://localhost:8200')
    logger.critical("vault_address = %s", str(vault_address))
    secret_path = vault_credentials.get('secretPath', '/v1/secret/data/cred')
    logger.critical("secret_path = %s", str(secret_path))
    vault_auth = vault_credentials.get('authPath', '/v1/auth/kubernetes/login')
    logger.critical("vault_auth = %s", str(vault_auth))
    role = vault_credentials.get('role', 'demo')
    logger.critical("role = %s", str(role))
    credentials = get_raw_secret_from_vault(jwt, secret_path, vault_address, vault_auth, role)
    if not credentials:
        return None, None
    if 'access_key' in credentials and 'secret_key' in credentials:
        if credentials['access_key'] and credentials['secret_key']:
            return credentials['access_key'], credentials['secret_key']
        else:
            if not credentials['access_key']:
                logger.credentials("'access_key' must be non-empty")
            if not credentials['secret_key']:
                logger.credentials("'secret_key' must be non-empty")
    logger.critical("Expected both 'access_key' and 'secret_key' fields in vault secret")
    return None, None
