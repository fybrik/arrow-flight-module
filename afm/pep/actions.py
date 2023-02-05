#
# Copyright 2020 IBM Corp.
# SPDX-License-Identifier: Apache-2.0
#
import pandas as pd
import pyarrow as pa
import hashlib
from time import time
from datetime import datetime

from fybrik_python_transformation import Action, PandasAction

class Filter(PandasAction):
    def __init__(self, description, columns, options):
        super().__init__(description, columns, options)
        self.query = options.get('query', '')

    def __dftransform__(self, df: pd.DataFrame) -> pd.DataFrame:
        if self.query:
            return df.query(self.query)
        else:
            return df

class AgeFilter(PandasAction):
    def __init__(self, description, columns, options):
        super().__init__(description, columns, options)
        age = int(options.get('age', 18))
        now = datetime.fromtimestamp(time())
        self.cutoff = now.replace(year=(now.year-age))

    def __dftransform__(self, df: pd.DataFrame) -> pd.DataFrame:
        if self.columns:
            for column in self.columns:
                df = df[pd.to_datetime(df[column]) < self.cutoff]
            return df
        else:
            return df

class Redact(Action):
    def __init__(self, description, columns, options):
        super().__init__(description, columns, options)
        self.redact_value = options.get("redactValue", "XXXXXXXXXX")

    def __call__(self, records: pa.RecordBatch) -> pa.RecordBatch:
        """Transformation logic for Redact action.

        Args:
            records (pa.RecordBatch): record batch to transform

        Returns:
            pa.RecordBatch: transformed record batch
        """
        columns = [column for column in self.columns if column in records.schema.names]
        indices = [records.schema.get_field_index(c) for c in columns]
        constColumn = pa.array([self.redact_value] * len(records), type=pa.string())
        new_columns = records.columns
        for i in indices:
            new_columns[i] = constColumn
        new_schema = self.schema(records.schema)
        return pa.RecordBatch.from_arrays(new_columns, schema=new_schema)

    def field_type(self):
        """Overrides field_type to calculate transformed schema correctly."""
        return pa.string() # redacted value is a string

class RemoveColumns(Action):
    def __call__(self, records: pa.RecordBatch) -> pa.RecordBatch:
        """Overrides __call__ to verify no removed columns exist."""
        columns = [column for column in self.columns if column in records.schema.names]
        if columns:
            raise RuntimeError("Access to {} is forbidden".format(columns))
        return records # no transformation needed

    def schema(self, original):
        """Removes configured columns from the schema."""
        schema: pa.Schema = original
        columns = [column for column in self.columns if column in schema.names]
        for column in columns:
            schema = schema.remove(schema.get_field_index(column))
        return schema

class FilterColumns(Action):
    def __init__(self, description, columns, options):
        super().__init__(description, columns, options)
        self._schema = None

    def __call__(self, records: pa.RecordBatch) -> pa.RecordBatch:
        columns = [column for column in self.columns if column in records.schema.names]
        indices = [records.schema.get_field_index(c) for c in columns]
        column_array = records.columns
        if not self._schema:
            self.schema(records.schema)
        return pa.RecordBatch.from_arrays(
			[column_array[i] for i in indices],
			schema=self._schema)

    def schema(self, original):
        if self._schema:
            return self.schema
        columns = [column for column in self.columns if column in original.names]
        self._schema = pa.schema([pa.field(c, original.field(c).type) for c in columns])
        return self._schema


class HashRedact(Action):
    def __init__(self, description, columns, options):
        super().__init__(description, columns, options)
        if options == None:
            self.hash_algo = "md5"
        else:
            self.hash_algo = options.get("algo", "md5")

    def __call__(self, records: pa.RecordBatch) -> pa.RecordBatch:
        """Transformation logic for HashRedact action.

        Args:
            records (pa.RecordBatch): record batch to transform

        Returns:
            pa.RecordBatch: transformed record batch
        """
        columns = [column for column in self.columns if column in records.schema.names]
        indices = [records.schema.get_field_index(c) for c in columns]
        new_columns = records.columns
        algo = self.hash_algo.lower()
        hashFunc = hashlib.md5
        if algo == "md5":
            hashFunc = hashlib.md5
        elif algo == "sha256":
            hashFunc = hashlib.sha256
        elif algo == "sha512":
            hashFunc = hashlib.sha512
        else:
            raise ValueError(f"Algorithm {algo} is not supported!")
        for i in indices:
            new_columns[i] = pa.array([hashFunc(v.as_py().encode()).hexdigest() for v in records.column(i)])

        new_schema = self.schema(records.schema)
        return pa.RecordBatch.from_arrays(new_columns, schema=new_schema)

    def field_type(self):
        """Overrides field_type to calculate transformed schema correctly."""
        return pa.string() # redacted value is a string

