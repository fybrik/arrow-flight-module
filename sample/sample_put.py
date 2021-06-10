#
# Copyright 2020 IBM Corp.
# SPDX-License-Identifier: Apache-2.0
#
import pyarrow.flight as fl
import pyarrow as pa
import json

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

request = {
    "asset": "new-dataset", 
}

def main(port, num_repeat, username, password):
    global client, info
    client = fl.connect("grpc://localhost:{}".format(port))
    if username or password:
        client.authenticate(HttpBasicClientAuthHandler(username, password))

    # write the new dataset
    data = pa.Table.from_arrays([pa.array(range(0, 10 * 1024))], names=['a'])
    writer, _ = client.do_put(fl.FlightDescriptor.for_command(json.dumps(request)),
                              data.schema)
    writer.write_table(data, 10 * 1024 * 1024)
    writer.close()

    # now that the dataset is in place, let's try to read it
    info = client.get_flight_info(
        fl.FlightDescriptor.for_command(json.dumps(request)))

    endpoint = info.endpoints[0]
    result: fl.FlightStreamReader = client.do_get(endpoint.ticket)
    print(result.read_all().to_pandas())

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
