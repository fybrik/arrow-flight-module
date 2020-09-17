#
# Copyright 2020 IBM Corp.
# SPDX-License-Identifier: Apache-2.0
#
import argparse
import os
from urllib.parse import urlparse

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pyarrow.fs import S3FileSystem, LocalFileSystem


def sample_table():
    df = pd.DataFrame(
        {'gender': ["Female", "Male", "Male"], 'weight': [-1, 5, 9.5], 'age': [1, 2, 3]})
    return pa.Table.from_pandas(df)


def import_table(source: str):
    if not source:
        return sample_table()
    if source.endswith(".csv"):
        from pyarrow import csv
        return csv.read_csv(source)
    if source.endswith(".json"):
        from pyarrow import json
        return json.read_json(source)
    if source.endswith(".parquet"):
        return pq.read_table(source)
    raise ValueError("source must be csv, json or parquet")


def main():
    parser = argparse.ArgumentParser(
        description="Generate sample parquet data")
    parser.add_argument('path', type=str, nargs='?',
                        help='path to save data to', default="./data/data.parquet")
    parser.add_argument('--source', type=str, help='local path to import data from (optional; can be csv, json or parquet)')
    parser.add_argument('--endpoint', type=str,
                        help='S3 endpoint (e.g.: https://s3.eu-de.cloud-object-storage.appdomain.cloud')
    parser.add_argument('--access_key', type=str, help='S3 access key')
    parser.add_argument('--secret_key', type=str, help='S3 secret key')
    args = parser.parse_args()

    if args.endpoint:
        print("Using S3 file system")
        parsed_endpoint = urlparse(args.endpoint)
        fs = S3FileSystem(endpoint_override=parsed_endpoint.netloc,
                          scheme=parsed_endpoint.scheme,
                          access_key=args.access_key,
                          secret_key=args.secret_key,
                          background_writes=False)
    else:
        print("Using local file system")
        os.makedirs(os.path.dirname(args.path), exist_ok=True)
        fs = LocalFileSystem()

    table = import_table(args.source)

    with fs.open_output_stream(args.path) as f:
        pq.write_table(table, f)
    print("Table written to", args.path)
    print(table.to_pandas())


if __name__ == '__main__':
    main()
