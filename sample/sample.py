#
# Copyright 2020 IBM Corp.
# SPDX-License-Identifier: Apache-2.0
#
from timeit import repeat
import pyarrow.flight as fl
import json
import threading
from base64 import b64encode

# taken from https://github.com/apache/arrow/blob/master/python/pyarrow/tests/test_flight.py#L450
class HttpBasicClientAuthHandler(fl.ClientAuthHandler):
    """An example implementation of HTTP basic authentication."""

    def __init__(self, username, password):
        super().__init__()
        self.basic_auth = fl.BasicAuth(username, password)
        self.token = None

    def authenticate(self, outgoing, incoming):
        auth = self.basic_auth.serialize()
        outgoing.write(auth)
        self.token = incoming.read()

    def get_token(self):
        return self.token

def get_transformation_text():
    with open('sample/transformation.txt', 'r') as f:
        operation_string = f.read()
        operation_string = b64encode(operation_string.encode('utf-8'))
        return operation_string.decode('utf-8')

request = {
    "asset": "nyc-taxi.parquet", 
    "columns": ["vendor_id", "pickup_at", "dropoff_at", "payment_type"],
    "transformations": [{"name": "Redact", "transformation": get_transformation_text(), "description": "description", "columns": ["dropoff_at"], "options": {"redactValue": "ABC"}}]
}

def read_from_endpoint(endpoint):
    if endpoint.locations:
        client = fl.connect(endpoint.locations[0])
    else:
        client = fl.connect("grpc://localhost:{}".format(args.port))
    if args.username or args.password:
        client.authenticate(
                HttpBasicClientAuthHandler(args.username, args.password))
    result: fl.FlightStreamReader = client.do_get(endpoint.ticket)
    print(result.read_all().to_pandas())
    #for s in result:
    #    pass

def read_dataset():
    threads = []
    for endpoint in info.endpoints:
        t = threading.Thread(target=read_from_endpoint, args=(endpoint,))
        threads.append(t)
        t.start()
    for t in threads:
        t.join()

def main(port, num_repeat, username, password):
    global client, info
    client = fl.connect("grpc://localhost:{}".format(port))
    if username or password:
        client.authenticate(HttpBasicClientAuthHandler(username, password))
    info = client.get_flight_info(
        fl.FlightDescriptor.for_command(json.dumps(request)))

    print("Timing " + str(num_repeat) + " runs of retrieving the dataset:" +
          str(repeat(stmt="read_dataset()",
              setup="from __main__ import read_dataset",
              repeat=num_repeat, number=1)))

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description='arrow-flight-module sample')
    parser.add_argument(
        '--port', type=int, default=8080, help='Listening port')
    parser.add_argument(
        '--repeat', type=int, default=3, help='Number of times we measure the time to go over dataset')
    parser.add_argument(
        '--username', type=str, default=None, help='Authentication username')
    parser.add_argument(
        '--password', type=str, default=None, help='Authentication password')
    args = parser.parse_args()

    main(args.port, args.repeat, args.username, args.password)
