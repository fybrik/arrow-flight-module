#
# Copyright 2020 IBM Corp.
# SPDX-License-Identifier: Apache-2.0
#
import pandas as pd
import pyarrow as pa

from .base import Action

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
        df: pd.DataFrame = records.to_pandas()
        df[columns] = self.redact_value
        new_schema = self.schema(records.schema)
        return pa.RecordBatch.from_pandas(df=df, schema=new_schema, preserve_index=False)

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
