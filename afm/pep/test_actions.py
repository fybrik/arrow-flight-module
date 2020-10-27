#
# Copyright 2020 IBM Corp.
# SPDX-License-Identifier: Apache-2.0
#
import unittest
import pandas as pd
import pyarrow as pa

from .actions import Redact

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

if __name__ == '__main__':
    unittest.main()
