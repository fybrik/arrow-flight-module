# Copyright 2020 IBM Corp.
# SPDX-License-Identifier: Apache-2.0

import logging
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
    logging.debug("full_auth_path = %s", str(full_auth_path))
    json = {"jwt": jwt, "role": role}
    response = requests.post(full_auth_path, json=json)
    if response.status_code == 200:
        return response.json()
    return None

def get_raw_secret_from_vault(jwt, secret_path, vault_address, vault_path, role):
    """Get a raw secret from vault by providing a valid jwt token"""
    vault_auth_response = vault_jwt_auth(jwt, vault_address, vault_path, role)
    if vault_auth_response is None:
        return None
    client_token = vault_auth_response["auth"]["client_token"]
    logging.debug("client_token: %s", str(client_token))
    secret_full_path = vault_address + secret_path
    logging.debug("secret_full_path = %s", str(secret_full_path))
    response = requests.get(secret_full_path, headers={"X-Vault-Token" : client_token})
    logging.debug("response: %s", str(response.json()))
    if response.status_code == 200:
        return response.json()['data']
    return None

def get_credentials_from_vault(vault_credentials):
    jwt_file_path = vault_credentials.get('jwt_file_path', '/var/run/secrets/kubernetes.io/serviceaccount/token')
    jwt = get_jwt_from_file(jwt_file_path)
    vault_address = vault_credentials.get('address', 'https://localhost:8200')
    secret_path = vault_credentials.get('secretPath', '/v1/secret/data/cred')
    vault_auth = vault_credentials.get('credentialsPath', '/v1/auth/kubernetes/login')
    role = vault_credentials.get('role', 'demo')
    credentials = get_raw_secret_from_vault(jwt, secret_path, vault_address, vault_auth, role)['data']
    return credentials['access_key'], credentials['secret_key']
