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
from .records import DataRecord, ROIRecord, AnnotationRecord

class DepositBuilder:
    def __init__(self):
        self.__data_provider = iter([])
        self.__roi_provider = iter([])
        self.__annotation_provider = iter([])
        self.__domain_types = []
        self.__dataset_compact_udts = None
        self.__data_out_uri = "crabdata.parquet"
        self.__roi_out_uri = "crabroi.parquet"
        self.__annotation_out_uri = "crabannotation.parquet"

    def set_data_provider(self, iterable):
        self.__data_provider = iterable
        return self

    def set_export_uri(self, uri):
        self.__data_out_uri = uri
        return self

    def set_compact_binary_udts(self, udts):
        self.__dataset_compact_udts = udts
        return self

    def set_bit_depth(self, bit_depth):
        self.__bit_depth = bit_depth
        return self

    def set_bit_data_type(self, dtype):
        self.__data_type = dtype
        return self

    def add_udt(self, udt):
        if self.__dataset_compact_udts is None:
            self.__dataset_compact_udts = []
        self.__dataset_compact_udts.append(small_udt(binary_udt(udt)))
        return self

    def build(self):
        try:
            record = next(self.__data_provider)

            fst_pull = True

            data_size_approx_kb = 32
            batch_num = 0
            data_batch_size = int(65536 / data_size_approx_kb) # Targeting batch size of ~ 64mb assuming a per-record size of about 32kb
            #data_batch_size = 256

            data_schema = pyarrow.schema([
                ("udt", pyarrow.string()),
                ("udt_bin", pyarrow.binary()),
                ("data", pyarrow.binary()),
                ("data_uri", pyarrow.string()),
                ("sha256", pyarrow.binary()),
                ("mime_type", pyarrow.string()),
                ("numerical_format", pyarrow.string()),
                ("domain_types", pyarrow.string()),
                ("bit_depth", pyarrow.uint64()),
                ("last_modified", pyarrow.timestamp('s', tz='UTC')),
                ("extents", pyarrow.list_(pyarrow.uint64())),
            ])
            data_parquet_writer = pyarrow.parquet.ParquetWriter(self.__data_out_uri, data_schema)
            exhausted = False

            build_stats = False
            if self.__dataset_compact_udts is None:
                self.__dataset_compact_udts = set()
                build_stats = True

            while not exhausted:
                data_batch_udt = []
                data_batch_udt_bin = []

                data_batch_data = []
                data_batch_data_uri = []
                data_batch_sha256 = []
                data_batch_mime_type = []
                data_batch_numerical_format = []
                data_batch_domain_types = []
                data_batch_bit_depth = []

                data_batch_last_modified = []
                data_batch_extents = []
                try:
                    for i in range(data_batch_size):
                        if fst_pull:
                            fst_pull = False
                        else:
                            record = next(self.__data_provider)
                        data_batch_udt.append(record.udt)
                        data_batch_udt_bin.append(record.bin_udt)
                        if build_stats:
                            self.__dataset_compact_udts.add(small_udt(record.bin_udt))

                        if record.raw_data is None:
                            data_batch_data.append(None)
                        else:
                            data_batch_data.append(record.as_bytes())

                        data_batch_data_uri.append(record.data_uri)
                        data_batch_sha256.append(record.sha256())
                        data_batch_mime_type.append(record.mime_type)
                        data_batch_numerical_format.append(record.numerical_format)
                        data_batch_domain_types.append(json.dumps(record.domain_types))
                        data_batch_bit_depth.append(record.bit_depth)

                        data_batch_last_modified.append(record.last_modified)
                        data_batch_extents.append(pyarrow.array(record.extents, type=pyarrow.uint64()))

                except StopIteration:
                    exhausted = True

                data_batch = pyarrow.RecordBatch.from_pydict({
                    "udt": data_batch_udt,
                    "udt_bin": data_batch_udt_bin,
                    "data": data_batch_data,
                    "data_uri": data_batch_data_uri,
                    "sha256": data_batch_sha256,
                    "mime_type": data_batch_mime_type,
                    "numerical_format": data_batch_numerical_format,
                    "domain_types": data_batch_domain_types,
                    "bit_depth": data_batch_bit_depth,
                    "last_modified": data_batch_last_modified,
                    "extents": data_batch_extents
                    }, schema=data_schema)
                data_parquet_writer.write_batch(data_batch)
                batch_num += 1
                #print("Writing " + str(len(data_batch_udt)) + " records to batch " + str(batch_num))


                #"domain_types": json.dumps(self.__domain_types),
                #"bit_depth": struct.pack("<Q", self.__bit_depth),
                #"numerical_format": self.__data_type,

            data_metadata = {
                    "data_type": "CRAB_DATA_V1",
                    "last_modified": struct.pack("<Q", int(datetime.utcnow().timestamp())),
                    "contains_udts": b"".join(self.__dataset_compact_udts),
                }
            data_parquet_writer.add_key_value_metadata(data_metadata)
            data_parquet_writer.close()
        except StopIteration:
            print("No raw data!")

        try:
            record = next(self.__roi_provider)

            fst_pull = True

            roi_size_approx_kb = 1
            batch_num = 0
            roi_batch_size = int(65536 / data_size_approx_kb) # Targeting batch size of ~ 64mb assuming a per-record size of about 32kb
            #data_batch_size = 256

            roi_schema = pyarrow.schema([
                ("udt", pyarrow.string()),
                ("udt_bin", pyarrow.binary()),
                ("uuid", pyarrow.binary()),
                ("last_modified", pyarrow.timestamp('s', tz='UTC')),
                ("extents", pyarrow.list_(pyarrow.uint64())),
                ("annotator", pyarrow.string()),
                ("annotation_software", pyarrow.string())
            ])
            roi_parquet_writer = pyarrow.parquet.ParquetWriter(self.__roi_out_uri, roi_schema)
            exhausted = False

            build_stats = False
            if self.__roiset_compact_udts is None:
                self.__roiset_compact_udts = set()
                build_stats = True

            while not exhausted:
                roi_batch_udt = []
                roi_batch_udt_bin = []
                roi_batch_uuid = []
                roi_batch_last_modified = []
                roi_batch_extents = []
                roi_batch_annotator = []
                roi_batch_annotation_software = []
                try:
                    for i in range(data_batch_size):
                        if fst_pull:
                            fst_pull = False
                        else:
                            record = next(self.__roi_provider)
                        roi_batch_udt.append(record.udt)
                        roi_batch_udt_bin.append(record.bin_udt)
                        if build_stats:
                            self.__roiset_compact_udts.add(small_udt(record.bin_udt))
                        roi_batch_uuid.append(uuid.UUID(record.uuid).bytes)
                        roi_batch_last_modified.append(record.last_modified)
                        roi_batch_extents.append(pyarrow.array(list(sum(record.extents, ())), type=pyarrow.uint64()))
                        roi_batch_last_modified.append(record.annotator)
                        roi_batch_last_modified.append(record.annotation_software)
                except StopIteration:
                    exhausted = True

                data_batch = pyarrow.RecordBatch.from_pydict({
                    "udt": roi_batch_udt,
                    "udt_bin": roi_batch_udt_bin,
                    "uuid": roi_batch_uuid,
                    "last_modified": data_batch_last_modified,
                    "extents": data_batch_extents,
                    "annotator": data_batch_annotator,
                    "annotation_software": data_batch_annotation_software
                    }, schema=data_schema)
                data_parquet_writer.write_batch(data_batch)
                batch_num += 1
                #print("Writing " + str(len(data_batch_udt)) + " records to batch " + str(batch_num))

            data_metadata = {
                    "data_type": "CRAB_ROI_V1",
                    "last_modified": struct.pack("<Q", int(datetime.utcnow().timestamp())),
                    "references_udts": b"".join(self.__roiset_compact_udts),
                }
            data_parquet_writer.add_key_value_metadata(data_metadata)
            data_parquet_writer.close()
        except StopIteration:
            print("No ROIs!")

        return True
