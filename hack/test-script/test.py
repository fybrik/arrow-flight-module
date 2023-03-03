#
# Copyright 2020 IBM Corp.
# SPDX-License-Identifier: Apache-2.0
#



import json
import pyarrow.flight as fl
import pandas as pd
import sys
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("-e", "--endpoint", required=True)
parser.add_argument("-p", "--port", required=True)
args = parser.parse_args()
# Create a Flight client
client = fl.connect("grpc://{}:{}".format(args.endpoint, args.port))

# Prepare the request
request = {
    "asset": "fybrik-notebook-sample/paysim-csv",
    # To request specific columns add to the request a "columns" key with a list of column names
    "columns": ["amount", "oldbalanceOrg"]
}

# Send request and fetch result as a pandas DataFrame
info = client.get_flight_info(fl.FlightDescriptor.for_command(json.dumps(request)))
reader: fl.FlightStreamReader = client.do_get(info.endpoints[0].ticket)
df: pd.DataFrame = reader.read_pandas()
print(df)

