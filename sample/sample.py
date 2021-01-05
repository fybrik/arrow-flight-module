#
# Copyright 2020 IBM Corp.
# SPDX-License-Identifier: Apache-2.0
#
from timeit import repeat
import pyarrow.flight as fl
import json

request = {
    "asset": "nyc-taxi.parquet", 
    "columns": ["vendor_id", "pickup_at", "dropoff_at", "payment_type"]
}

def read_dataset():
    if info.endpoints[0].locations:
        client = fl.connect(info.endpoints[0].locations[0])
    result: fl.FlightStreamReader = client.do_get(info.endpoints[0].ticket)
    print(result.read_all().to_pandas())
    #for s in result:
    #    pass

def main(port, num_repeat):
    global client, info
    client = fl.connect("grpc://localhost:{}".format(port))
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
    args = parser.parse_args()

    main(args.port, args.repeat)
