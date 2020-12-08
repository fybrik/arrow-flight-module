#
# Copyright 2020 IBM Corp.
# SPDX-License-Identifier: Apache-2.0
#
from fsspec.implementations.http import HTTPFileSystem
from pyarrow.fs import PyFileSystem, FSSpecHandler

def httpfs_from_config():
    return PyFileSystem(FSSpecHandler(HTTPFileSystem()))
