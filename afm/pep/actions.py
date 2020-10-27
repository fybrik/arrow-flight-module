#
# Copyright 2020 IBM Corp.
# SPDX-License-Identifier: Apache-2.0
#
import functools
import itertools
import json

import pandas as pd
import pyarrow as pa


class Action:
    """
    Base class for actions.

    There are two types of actions:
    1. Callable actions
        Implement `__call__(self, records: pa.RecordBatch) -> pa.RecordBatch`
        Perform transformation on the data itself
    2. Non-callable actions
        Just hold metadata about the action

    Action.registry is a map from action name to class implementing the action
    for all supported action types.
    """

    registry = {}

    def __init__(self, description, columns, options):
        self.metadata = {
            "name": self.__class__.__name__,
            "description": description
        }
        self.columns = columns
        self.options = options

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        cls.registry[cls.__name__] = cls

    def __repr__(self):
        return str((action_key(self), self.columns))

    @property
    def name(self):
        return self.metadata["name"]

    @property
    def description(self):
        return self.metadata["description"]

    def field_type(self):
        return None # unchanged from original schema

    def field_nullable(self):
        return None # unchanged from original schema

    def schema(self, original):
        """
        Computes a schema of the result after this action runs
        @param original - the original schema
        @returns new schema
        """
        schema: pa.Schema = original
        columns = [column for column in self.columns if column in schema.names]
        for column in columns:
            field_index = schema.get_field_index(column)
            field = schema.field(field_index)
            new_field = pa.field(field.name, 
                self.field_type() or field.type, 
                self.field_nullable() or field.nullable, 
                field.metadata).with_metadata(self._build_metadata(field))
            schema = schema.set(field_index, new_field)
        return schema
    

    def _build_metadata(self, field: pa.Field) -> dict:
        metadata = dict(field.metadata or {})
        transformations = json.loads(metadata.get(b"transformations") or "[]")
        transformations.append(self.metadata)
        metadata[b"transformations"] = json.dumps(transformations)
        return metadata

# Actions can be consolidated if they have the same key, which is determined
# by type of action and options
def action_key(action):
    return str(type(action)) + str(action.options)

# consolidation of two actions with the same type and options simply merges
# their column lists
def reduce_actions(a, b):
    a.columns = (a.columns or []) + (b.columns or [])
    return a

# Consolidate actions which are the same type and have the same action.
# For example, if you have two redact actions with the same redactValue
# on two different columns, consolidate them to a single action on both
# columns
def consolidate_actions(actions):
    # sort actions by key
    actions = sorted(actions, key=lambda x: action_key(x))
    ret = []
    for k, g in itertools.groupby(actions, action_key):
        ret.append(functools.reduce(reduce_actions, list(g)))
    return ret

def transform(actions, record_batches):
    for record_batch in record_batches:
        item = record_batch
        for action in actions:
            if callable(action):
                item = action(item)
        yield item

class Redact(Action):
    def __init__(self, description, columns, options):
        super().__init__(description, columns, options)
        self.redact_value = options.get("redactValue", "XXXXXXXXXX")

    def __call__(self, records: pa.RecordBatch) -> pa.RecordBatch:
        columns = [column for column in self.columns if column in records.schema.names]
        df: pd.DataFrame = records.to_pandas()
        df[columns] = self.redact_value
        new_schema = self.schema(records.schema)
        return pa.RecordBatch.from_pandas(df=df, schema=new_schema, preserve_index=False)

    def field_type(self):
        return pa.string() # redact value is a string

class RemoveColumns(Action):
    def schema(self, original):
        schema: pa.Schema = original
        columns = [column for column in self.columns if column in schema.names]
        for column in columns:
            schema = schema.remove(schema.get_field_index(column))
        return schema


def test():
    df = pd.DataFrame(
        {'gender': ["Female", "Male", "Male"], 'weight': [-1, 5, 9.5], 'age': [1, 2, 3]})
    table = pa.Table.from_pandas(df)
    action = Redact("redact stuff", columns=["gender", "age"], options={})
    for record_batch in table.to_batches():
        result = action(record_batch)
        print(result.schema)
        print(result.to_pandas())

def test_consolidate_actions():
    actions = [Redact("redact stuff", columns=["gender"], options={"redactValue": "GGG"}),
               RemoveColumns("redact stuff", columns=["gender", "weight"], options={}),
               Redact("redact stuff2", columns=["age"], options={"redactValue": "GGG"}),
               Redact("redact stuff3", columns=["weight"], options={"redactValue": "YYYY"})]
    print([actions])
    actions = consolidate_actions(actions)
    print([actions])
    print(str(consolidate_actions([])))

if __name__ == '__main__':
    test()
    test_consolidate_actions()
