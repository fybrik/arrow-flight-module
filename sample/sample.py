#
# Copyright 2020 IBM Corp.
# SPDX-License-Identifier: Apache-2.0
#
import pyarrow.flight as fl
import pandas as pd
import json

def main(port):
    client = fl.connect("grpc://localhost:{}".format(port))
    request = {
        "asset": "nyc-taxi.parquet", 
        "columns": ["vendor_id", "pickup_at", "dropoff_at", "payment_type"]
    }
    info: fl.FlightInfo = client.get_flight_info(
        fl.FlightDescriptor.for_command(json.dumps(request)))
    print(info.schema)
    result: fl.FlightStreamReader = client.do_get(info.endpoints[0].ticket)

    df: pd.DataFrame = result.read_pandas()
    print(df)

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description='arrow-flight-module sample')
    parser.add_argument(
        '--port', type=int, default=8080, help='Listening port')
    args = parser.parse_args()

    main(args.port)