#
# Copyright 2020 IBM Corp.
# SPDX-License-Identifier: Apache-2.0
#
from .base import Action, consolidate_actions, transform, transform_schema
from .actions import Redact, RemoveColumns

registry = Action.registry
