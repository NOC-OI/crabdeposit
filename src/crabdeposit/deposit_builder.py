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
from .records import DataRecord, AnnotationRecord
from .deposit import Deposit

class DepositBuilder:
    def __init__(self):
        self.__data_provider = iter([])
        self.__annotation_provider = iter([])
        self.__domain_types = []
        self.__dataset_compact_udts = None
        self.__data_out_uri = "crabdata.parquet"
        self.__annotation_out_uri = "crabannotation.parquet"
        self.__custom_fields = []
        self.__custom_discarded_fields = []

    def set_data_provider(self, iterable):
        self.__data_provider = iterable
        return self

    def set_annotation_provider(self, iterable):
        self.__annotation_provider = iterable
        return self

    def set_data_export_uri(self, uri):
        self.__data_out_uri = uri
        return self

    def set_annotation_export_uri(self, uri):
        self.__annotation_out_uri = uri
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

    def register_field(self, field_name, dtype = float):
        self.__custom_fields.append((field_name, pyarrow.from_numpy_dtype(dtype)))
        return self

    def register_fields(self, field_names, dtype = float):
        for field_name in field_names:
            self.register_field(field_name, dtype)
        return self

    def register_discard_field(self, field_name):
        self.__custom_discarded_fields.append(field_name)
        return self

    def build(self):
        build_stats = False
        if self.__dataset_compact_udts is None:
            self.__dataset_compact_udts = set()
            build_stats = True

        data_parquet_writer = None
        annotation_parquet_writer = None

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
                ("value_domain", pyarrow.string()),
                ("bit_depth", pyarrow.uint64()),
                ("last_modified", pyarrow.timestamp('s', tz='UTC')),
                ("extents", pyarrow.list_(pyarrow.uint64())),
            ])
            data_parquet_writer = pyarrow.parquet.ParquetWriter(self.__data_out_uri, data_schema)
            exhausted = False

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
                data_batch_value_domain = []

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
                        data_batch_value_domain.append(record.value_domain)

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
                    "value_domain": data_batch_value_domain,
                    "last_modified": data_batch_last_modified,
                    "extents": data_batch_extents
                    }, schema=data_schema)
                data_parquet_writer.write_batch(data_batch)
                batch_num += 1
                #print("Writing " + str(len(data_batch_udt)) + " records to batch " + str(batch_num))
        except StopIteration:
            print("No raw data!")

        try:
            record = next(self.__annotation_provider)

            fst_pull = True

            annotation_size_approx_kb = 1
            batch_num = 0
            annotation_batch_size = int(65536 / data_size_approx_kb) # Targeting batch size of ~ 64mb assuming a per-record size of about 32kb
            #data_batch_size = 256


            pyarrow_fields = [
                ("udt", pyarrow.string()),
                ("udt_bin", pyarrow.binary()),
                ("uuid", pyarrow.binary()),
                ("sha256", pyarrow.binary()),
                ("last_modified", pyarrow.timestamp('s', tz='UTC')),
                ("extents", pyarrow.list_(pyarrow.uint64())),
                ("origin_extents", pyarrow.list_(pyarrow.uint64())),
                ("annotator", pyarrow.string()),
                ("annotation_software", pyarrow.string()),
                ("discard_in_favour", pyarrow.binary())
            ]
            for field_def in self.__custom_fields:
                pyarrow_fields.append((("field_" + field_def[0]), field_def[1]))
            for discard_field_def in self.__custom_discarded_fields:
                pyarrow_fields.append((("discard_field_" + field_def[0]), pyarrow.bool_()))
            annotation_schema = pyarrow.schema(pyarrow_fields)
            annotation_parquet_writer = pyarrow.parquet.ParquetWriter(self.__annotation_out_uri, annotation_schema)
            exhausted = False

            while not exhausted:
                annotation_batch_udt = []
                annotation_batch_udt_bin = []
                annotation_batch_uuid = []
                annotation_batch_sha256 = []
                annotation_batch_last_modified = []
                annotation_batch_extents = []
                annotation_batch_origin_extents = []
                annotation_batch_annotator = []
                annotation_batch_annotation_software = []
                annotation_batch_discard_in_favour = []
                annotation_extra_fields_dict = {}
                for field_def in self.__custom_fields:
                    annotation_extra_fields_dict["field_" + field_def[0]] = []
                for discard_field_def in self.__custom_discarded_fields:
                    annotation_extra_fields_dict["discard_field_" + discard_field_def] = []
                try:
                    for i in range(data_batch_size):
                        if fst_pull:
                            fst_pull = False
                        else:
                            record = next(self.__annotation_provider)
                        annotation_batch_udt.append(record.udt)
                        annotation_batch_udt_bin.append(record.bin_udt)
                        if build_stats:
                            self.__dataset_compact_udts.add(small_udt(record.bin_udt))
                        annotation_batch_sha256.append(record.sha256)
                        annotation_batch_uuid.append(uuid.UUID(record.uuid).bytes)
                        annotation_batch_last_modified.append(record.last_modified)
                        annotation_batch_extents.append(pyarrow.array(list(sum(record.extents, ())), type=pyarrow.uint64()))
                        annotation_batch_origin_extents.append(pyarrow.array(list(record.origin_extents), type=pyarrow.uint64()))
                        annotation_batch_annotator.append(record.annotator)
                        annotation_batch_annotation_software.append(record.annotation_software)
                        annotation_batch_discard_in_favour.append(record.discard_in_favour)

                        for field_def in self.__custom_fields:
                            val = None
                            if field_def[0] in record.field_dict.keys():
                                val = record.field_dict[field_def[0]]
                            annotation_extra_fields_dict["field_" + field_def[0]].append(val)
                        for discard_field_def in self.__custom_discarded_fields:
                            val = None
                            if discard_field_def in record.discard_field_list:
                                val = True
                            annotation_extra_fields_dict["discard_field_" + discard_field_def].append(val)
                except StopIteration:
                    exhausted = True

                annotation_batch_dict = {
                        "udt": annotation_batch_udt,
                        "udt_bin": annotation_batch_udt_bin,
                        "uuid": annotation_batch_uuid,
                        "sha256": annotation_batch_sha256,
                        "last_modified": annotation_batch_last_modified,
                        "extents": annotation_batch_extents,
                        "origin_extents": annotation_batch_origin_extents,
                        "annotator": annotation_batch_annotator,
                        "annotation_software": annotation_batch_annotation_software,
                        "discard_in_favour": annotation_batch_discard_in_favour
                    }
                for annotation_extra_field_key in annotation_extra_fields_dict.keys():
                    annotation_batch_dict[annotation_extra_field_key] = annotation_extra_fields_dict[annotation_extra_field_key]
                annotation_batch = pyarrow.RecordBatch.from_pydict(annotation_batch_dict, schema=annotation_schema)
                annotation_parquet_writer.write_batch(annotation_batch)
                batch_num += 1
                #print("Writing " + str(len(data_batch_udt)) + " records to batch " + str(batch_num))


        except StopIteration:
            print("No Annotations!")

        if data_parquet_writer is not None:
            data_metadata = {
                "data_type": "CRAB_DATA_V1",
                "last_modified": struct.pack("<Q", int(datetime.utcnow().timestamp())),
                "contains_udts": b"".join(self.__dataset_compact_udts),
            }
            data_parquet_writer.add_key_value_metadata(data_metadata)
            data_parquet_writer.close()

        if annotation_parquet_writer is not None:
            annotation_metadata = {
                "data_type": "CRAB_ANNOTATION_V1",
                "last_modified": struct.pack("<Q", int(datetime.utcnow().timestamp())),
                "references_udts": b"".join(self.__dataset_compact_udts),
            }
            annotation_parquet_writer.add_key_value_metadata(annotation_metadata)
            annotation_parquet_writer.close()

        out_deposit = Deposit()
        out_deposit.set_deposit_files([self.__data_out_uri, self.__annotation_out_uri])
        return out_deposit
