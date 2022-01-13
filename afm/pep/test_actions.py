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

    def test_hash_redact_sha256(self):
        df = pd.DataFrame(
            {'col1': [1, 2, 3], 'col2': ["abcdefghijklmnopqrstuvwxyz", "bcdefghijklmnopqrstuvwxyza", "cdefghijklmnopqrstuvwxyzab"], 'col3': [1.0, 2.0, 3.0]})
        table = pa.Table.from_pandas(df)

        action = HashRedact("Hash redact", columns=["col2"], options={"algo": "sha256"})
        for record_batch in table.to_batches():
            result = action(record_batch)
            self.assertEqual(result.schema.field("col1").type, pa.int64())
            self.assertEqual(result.schema.field("col2").type, pa.string())
            self.assertEqual(result.schema.field("col3").type, pa.float64())

            self.assertEqual(result.to_pandas()["col2"][0], "71c480df93d6ae2f1efad1447c66c9525e316218cf51fc8d9ed832f2daf18b73")
            self.assertEqual(result.to_pandas()["col2"][1], "e40957dd33bd9da6053d78bea4da6c7cde1fac92614bfd03d8b0c422e021651c")
            self.assertEqual(result.to_pandas()["col2"][2], "fa732dae244c6d0b946e096d05167539a4b6ec2cc72f13a86a7fd657ef523d07")

    
    def test_hash_redact_sha512(self):
        df = pd.DataFrame(
            {'col1': [1, 2, 3], 'col2': ["abcdefghijklmnopqrstuvwxyz", "bcdefghijklmnopqrstuvwxyza", "cdefghijklmnopqrstuvwxyzab"], 'col3': [1.0, 2.0, 3.0]})
        table = pa.Table.from_pandas(df)

        action = HashRedact("Hash redact", columns=["col2"], options={"algo": "sha512"})
        for record_batch in table.to_batches():
            result = action(record_batch)
            self.assertEqual(result.schema.field("col1").type, pa.int64())
            self.assertEqual(result.schema.field("col2").type, pa.string())
            self.assertEqual(result.schema.field("col3").type, pa.float64())

            self.assertEqual(result.to_pandas()["col2"][0], "4dbff86cc2ca1bae1e16468a05cb9881c97f1753bce3619034898faa1aabe429955a1bf8ec483d7421fe3c1646613a59ed5441fb0f321389f77f48a879c7b1f1")
            self.assertEqual(result.to_pandas()["col2"][1], "6cf15b5b147ed859119df308a3e22a3958ecf1056b9cab135a1ce722ec57f1b65a03983a183141db9cb68817d57fab964be3068fe05eac8ff3d5f24ca34c6524")
            self.assertEqual(result.to_pandas()["col2"][2], "5d63cd2920fdbf1f67d2a55a7d5b792331f9e21cc9965419170176e98a221d3a68080225f0e781734304c1ef6f162dade36acf463b137e6767416c1c53fa845d")


if __name__ == '__main__':
    unittest.main()