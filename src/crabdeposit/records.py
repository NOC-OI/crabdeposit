#!/bin/python3

# Copyright 2025, A Baldwin, National Oceanography Centre
#
# This file is part of crabdeposit.
#
# crabdeposit is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, version 3 of the License specifically.
#
# crabdeposit is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Lesser General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public License
# along with crabdeposit.  If not, see <http://www.gnu.org/licenses/>.

import os
import re
import csv
import json
import numpy
import io
import struct
import hashlib
import uuid
import pyarrow
import pyarrow.parquet
import pyarrow.compute
from datetime import datetime
from .udt import string_udt, binary_udt, small_udt

class DataRecord:
    def __init__(self, udt, data, last_modified, data_uri=None, bin_udt=None, bin_compact_udt=None, mime_type=None, domain_types=None, numerical_format=None, bit_depth=None, value_domain="magnitude", extents=None, sha256=None):
        self.extents = extents
        self.__byte_data = None
        self.mime_type = None
        if isinstance(data, io.BufferedIOBase):
            self.raw_data = data
            self.mime_type = mime_type
        elif isinstance(data, numpy.ndarray):
            self.raw_data = data
            self.mime_type = "application/octet-stream"
            if extents is None:
                self.extents = list(data.shape)
        elif data is None:
            self.raw_data = None
            self.mime_type = mime_type
        else:
            raise RuntimeError("Data must be a NumPy NDArray, a file-like (BufferedIO-derived) object, or None/null")
        if (self.raw_data is None) and (self.data_uri is None):
            raise RuntimeError("Need at least Data or Data URI")
        if self.extents is None:
            raise RuntimeError("Data extents missing, and could not be inferred")
        if self.mime_type is None:
            raise RuntimeError("Mime type missing, and could not be inferred")
        self.data_uri = data_uri
        self.udt = udt
        self.last_modified = last_modified
        self.bin_udt = bin_udt
        if self.bin_udt is None:
            self.bin_udt = binary_udt(self.udt)
        self.bin_compact_udt = bin_compact_udt
        if self.bin_compact_udt is None:
            self.bin_compact_udt = small_udt(self.bin_udt)

        if numerical_format is None:
            if isinstance(self.raw_data, numpy.ndarray):
                numerical_format = str(self.raw_data.dtype)
            else:
                raise RuntimeError("Numerical format missing, and could not be inferred")

        if domain_types is None:
            raise RuntimeError("Domain types missing")
        elif type(domain_types) is not list:
            raise RuntimeError("Domain types must be a list of string definitions")
        else:
            if isinstance(self.raw_data, numpy.ndarray):
                if len(domain_types) != self.raw_data.ndim:
                    raise RuntimeError("Number of dimensions in domain type do not match dimensions of input array")
            self.domain_types = domain_types

        self.numerical_format = numerical_format

        if bit_depth is None:
            dtype_numbers = re.findall(r'\d+', self.numerical_format)
            bit_depth = int(dtype_numbers[0])
        self.bit_depth = bit_depth
        self.value_domain = value_domain

        self.__sha256 = None
        if sha256 is not None:
            if type(sha256) is str:
                self.__sha256 = bytes.fromhex(sha256)
            elif type(sha256) is bytes:
                self.__sha256 = sha256

    def sha256(self):
        if self.__sha256 is None:
            m = hashlib.sha256()
            m.update(self.as_bytes())
            self.__sha256 = m.digest()
        return self.__sha256

    def as_bytes(self):
        if self.__byte_data is None:
            if isinstance(self.raw_data, io.BufferedIOBase):
                self.__byte_data = self.raw_data.read()
            elif isinstance(self.raw_data, numpy.ndarray):
                self.__byte_data = self.raw_data.tobytes(order="C")
            else:
                raise RuntimeError("Cloud retrieval unimplemented")
        return self.__byte_data

    def as_array(self):
        if isinstance(self.raw_data, io.BufferedIOBase):
            raise RuntimeError("Unimplemented")
        elif isinstance(self.raw_data, numpy.ndarray):
            return self.raw_data
        else:
            raise RuntimeError("Cloud retrieval unimplemented")

class AnnotationRecord:
    def __init__(self, udt, last_modified, extents, origin_extents, sha256, uuid = None, annotator = None, annotation_software = None, bin_udt=None, bin_compact_udt=None, discard_in_favour=None, field_dict={}, discard_field_list=[]):
        self.udt = udt
        self.uuid = str(uuid)
        if self.uuid is None:
            self.uuid = uuid.uuid4()
        self.last_modified = last_modified
        self.bin_udt = bin_udt
        if self.bin_udt is None:
            self.bin_udt = binary_udt(self.udt)
        self.bin_compact_udt = bin_compact_udt
        if self.bin_compact_udt is None:
            self.bin_compact_udt = small_udt(self.bin_udt)
        self.extents = extents
        self.origin_extents = origin_extents
        if sha256 is None:
            raise RuntimeError("Hash is neccesary for annotations!")
        self.sha256 = sha256
        self.annotator = annotator
        self.annotation_software = annotation_software
        self.discard_in_favour = discard_in_favour
        self.field_dict = field_dict
        self.discard_field_list = discard_field_list


