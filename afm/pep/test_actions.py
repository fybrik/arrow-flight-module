#
# Copyright 2020 IBM Corp.
# SPDX-License-Identifier: Apache-2.0
#
import unittest
import pandas as pd
import pyarrow as pa

from .actions import Redact
from .actions import HashRedact


class TestActions(unittest.TestCase):

    def test_redact(self):
        df = pd.DataFrame(
            {'gender': ["Female", "Male", "Male"], 'weight': [-1, 5, 9.5], 'age': [1, 2, 3]})
        table = pa.Table.from_pandas(df)

        action = Redact("redact stuff", columns=["gender", "age"], options={"redactValue": "XXX"})
        for record_batch in table.to_batches():
            result = action(record_batch)
            self.assertEqual(result.schema.field("gender").type, pa.string())
            self.assertEqual(result.schema.field("age").type, pa.string())
            self.assertEqual(result.schema.field("weight").type, pa.float64())

            self.assertEqual(result.to_pandas()["gender"][0], "XXX")
            self.assertEqual(result.to_pandas()["age"][0], "XXX")

    def test_hash_redact_md5(self):
        df = pd.DataFrame(
            {'col1': [1, 2, 3], 'col2': ["abcdefghijklmnopqrstuvwxyz", "bcdefghijklmnopqrstuvwxyza", "cdefghijklmnopqrstuvwxyzab"], 'col3': [1.0, 2.0, 3.0]})
        table = pa.Table.from_pandas(df)

        action = HashRedact("Hash redact", columns=["col2"], options={"algo": "md5"})
        for record_batch in table.to_batches():
            result = action(record_batch)
            self.assertEqual(result.schema.field("col1").type, pa.int64())
            self.assertEqual(result.schema.field("col2").type, pa.string())
            self.assertEqual(result.schema.field("col3").type, pa.float64())

            self.assertEqual(result.to_pandas()["col2"][0], "c3fcd3d76192e4007dfb496cca67e13b")
            self.assertEqual(result.to_pandas()["col2"][1], "07694ef19cf359bfd74556dc0cc7956d")
            self.assertEqual(result.to_pandas()["col2"][2], "8dda2bba265b7478676bf9526e79c91c")

    def test_hash_redact_sha1(self):
        df = pd.DataFrame(
            {'col1': [1, 2, 3], 'col2': ["abcdefghijklmnopqrstuvwxyz", "bcdefghijklmnopqrstuvwxyza", "cdefghijklmnopqrstuvwxyzab"], 'col3': [1.0, 2.0, 3.0]})
        table = pa.Table.from_pandas(df)

        action = HashRedact("Hash redact", columns=["col2"], options={"algo": "sha1"})
        for record_batch in table.to_batches():
            result = action(record_batch)
            self.assertEqual(result.schema.field("col1").type, pa.int64())
            self.assertEqual(result.schema.field("col2").type, pa.string())
            self.assertEqual(result.schema.field("col3").type, pa.float64())

            self.assertEqual(result.to_pandas()["col2"][0], "32d10c7b8cf96570ca04ce37f2a19d84240d3a89")
            self.assertEqual(result.to_pandas()["col2"][1], "03d630bd344c60bca6b4e1e96237f371a52fc462")
            self.assertEqual(result.to_pandas()["col2"][2], "29216e59de907aab9a587c3424f393e6912baed2")



if __name__ == '__main__':
    unittest.main()
