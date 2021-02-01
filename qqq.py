from kubernetes import client
from kubernetes import config

# Configs can be set in Configuration class directly or using helper utility
config.load_kube_config()

configuration = client.Configuration()

with client.ApiClient(configuration) as api_client:
    api_instance = client.AuthenticationV1beta1Api(api_client)
    body = client.V1beta1TokenReview()
