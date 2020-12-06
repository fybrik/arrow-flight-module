#
# Copyright 2020 IBM Corp.
# SPDX-License-Identifier: Apache-2.0
#
import httpfs
from pyarrow.fs import PyFileSystem, FSSpecHandler

def httpfs_from_config(httpfs_config):
    fs = httpfs.fs.HttpFs(httpfs_config.get('endpoint_url'))
    return PyFileSystem(FSSpecHandler(fs))
