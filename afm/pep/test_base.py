#
# Copyright 2020 IBM Corp.
# SPDX-License-Identifier: Apache-2.0
#
import unittest
from fybrik_python_transformation import Action, consolidate_actions, action_key

class ActionOne(Action):
    pass

class ActionTwo(Action):
    pass

class TestBase(unittest.TestCase):

    def test_consolidate_actions(self):
        actions = [
            ActionOne("1.1", columns=["gender"], options={"redactValue": "XXX"}),
            ActionTwo("2.1", columns=["gender", "weight"], options={}),
            ActionOne("1.2", columns=["age"], options={"redactValue": "XXX"}),
            ActionOne("1.3", columns=["weight"], options={"redactValue": "YYYY"})
        ]
        actual = consolidate_actions(actions)
        expected = [
            ActionOne("1.1", columns=["gender", "age"], options={"redactValue": "XXX"}),
            ActionTwo("2.1", columns=["gender", "weight"], options={}),
            ActionOne("1.3", columns=["weight"], options={"redactValue": "YYYY"})
        ]
        self.assertEqual(str(sorted(actual, key=action_key)), str(sorted(expected, key=action_key)))

        self.assertEqual([], consolidate_actions([]))


if __name__ == '__main__':
    unittest.main()
