#
# Copyright 2020 IBM Corp.
# SPDX-License-Identifier: Apache-2.0
#
import pyarrow.flight as fl
import pyarrow as pa
import json
from faker import Faker

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
    # write_mode can be append or overwrite. The default is overwrite.
    # "write_mode": "append",
}

def fake_dataset(num_entries):
    Faker.seed(1234)
    f = Faker()
    arrays = []
    column_names = []

    arr = []
    for i in range(num_entries):
        arr.append(f.name())
    arrays.append(arr)
    column_names.append("Name")

    arr = []
    for i in range(num_entries):
        arr.append(f.email())
    arrays.append(arr)
    column_names.append("Email")

    arr = []
    for i in range(num_entries):
        arr.append(f.address())
    arrays.append(arr)
    column_names.append("Address")

    arr = []
    for i in range(num_entries):
        arr.append(f.country())
    arrays.append(arr)
    column_names.append("Country")

    arr = []
    for i in range(num_entries):
        arr.append(f.date_of_birth())
    arrays.append(arr)
    column_names.append("Date of Birth")

    return arrays, column_names

def main(port, username, password):
    client = fl.connect("grpc://localhost:{}".format(port))
    if username or password:
        client.authenticate(HttpBasicClientAuthHandler(username, password))

    # write the new dataset
    arrays, names = fake_dataset(1000)
    data = pa.Table.from_arrays(arrays, names=names)
    writer, _ = client.do_put(fl.FlightDescriptor.for_command(json.dumps(request)),
                              data.schema)
    writer.write_table(data, 1024)
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
        '--username', type=str, default=None, help='Authentication username')
    parser.add_argument(
        '--password', type=str, default=None, help='Authentication password')
    args = parser.parse_args()

    main(args.port, args.username, args.password)
