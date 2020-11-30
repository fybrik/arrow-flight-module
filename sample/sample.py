#
# Copyright 2020 IBM Corp.
# SPDX-License-Identifier: Apache-2.0
#
from timeit import repeat

def main(port, num_repeat):
    setup = '''
import pyarrow.flight as fl
import json

request = {
    "asset": "nyc-taxi.parquet", 
    "columns": ["vendor_id", "pickup_at", "dropoff_at", "payment_type"]
}

client = fl.connect("grpc://localhost:{}".format(
''' + str(port) + '''))
info: fl.FlightInfo = client.get_flight_info(
    fl.FlightDescriptor.for_command(json.dumps(request)))
'''

    stmt = '''
result: fl.FlightStreamReader = client.do_get(info.endpoints[0].ticket)
for s in result:
    pass
'''

    print("Timing " + str(num_repeat) + " runs of retrieving the dataset:" +
          str(repeat(stmt=stmt, setup=setup, repeat=num_repeat, number=1)))

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description='arrow-flight-module sample')
    parser.add_argument(
        '--port', type=int, default=8080, help='Listening port')
    parser.add_argument(
        '--repeat', type=int, default=3, help='Number of times we measure the time to go over dataset')
    args = parser.parse_args()

    main(args.port, args.repeat)
