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

'''
native_interface.py

An interface for working with CRAB deposit parquet files
'''

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

class Deposit:
    def __init__(self):
        self.__parquet_uris = []
        self.__parquet_files = []
        self.__data_indicies = []
        self.__roi_indicies = []
        self.__annotation_indicies = []
        self.__udt_map = {}

    def set_deposit_files(self, parquet_uris):
        self.__parquet_uris = parquet_uris
        for pfi, parquet_uri in enumerate(self.__parquet_uris):
            pfo = pyarrow.parquet.ParquetFile(parquet_uri)

            #print(pfo.metadata.metadata)
            if pfo.metadata.metadata[b"data_type"] == b"CRAB_DATA_V1":
                self.__data_indicies.append(pfi)
            elif pfo.metadata.metadata[b"data_type"] == b"CRAB_ROI_V1":
                self.__roi_indicies.append(pfi)
            elif pfo.metadata.metadata[b"data_type"] == b"CRAB_ANNOTATION_V1":
                self.__annotation_indicies.append(pfi)
            else:
                raise RuntimeError("Unrecognised CRAB deposit file")

            self.__parquet_files.append(pfo)
            #print(pfo.metadata.metadata[b"numerical_format"].decode("utf-8"))
            #self.__parquet_dtypes.append(numpy.dtype(pfo.metadata.metadata[b"numerical_format"].decode("utf-8")))

            pf_udts_str = pfo.metadata.metadata[b"contains_udts"]
            pf_udts = [pf_udts_str[i:i+21] for i in range(0, len(pf_udts_str), 21)]
            for pf_udt in pf_udts:
                if not pf_udt in self.__udt_map.keys():
                    self.__udt_map[pf_udt] = []
                self.__udt_map[pf_udt].append(pfi)

    def get_all_compact_udts(self):
        return list(map(string_udt, self.__udt_map.keys()))

    def get_referencing_indicies(self, udt):
        sbudt = small_udt(binary_udt(udt))
        if sbudt in self.__udt_map.keys():
            refs = []
            for pfi in self.__udt_map[sbudt]:
                refs.append(pfi)
            return refs
        else:
            return []

    def get_data_record(self, udt, full_string_match=False):
        budt = binary_udt(udt)
        if full_string_match:
            if budt == udt:
                raise RuntimeError("Cannot use input binary UDT with full_string_match option")
        try_pfis = self.get_referencing_indicies(budt)
        for pfi in try_pfis:
            if pfi in self.__data_indicies:
                pfo = self.__parquet_files[pfi]
                for row_group in range(pfo.num_row_groups):
                    rgt = pfo.read_row_group(row_group)
                    filtered_rgt = rgt.filter(pyarrow.compute.equal(rgt["udt_bin"], budt))
                    filtered_rgt = filtered_rgt.to_pylist(maps_as_pydicts="strict")
                    for matched_record_def in filtered_rgt:
                        if (not full_string_match) or (matched_record_def["udt"] == udt): # Optional check for full string match


                            data = None
                            numerical_format = numpy.dtype(matched_record_def["numerical_format"])

                            if matched_record_def["data"] is not None:
                                if matched_record_def["mime_type"] == "application/octet-stream":
                                    numpy_array = numpy.frombuffer(matched_record_def["data"], dtype=numerical_format)
                                    data = numpy.reshape(numpy_array, shape=matched_record_def["extents"], order="C")
                                else:
                                    data = io.BytesIO(matched_record_def["data"])

                            return DataRecord(
                                    udt = matched_record_def["udt"],
                                    data = data,
                                    last_modified = matched_record_def["last_modified"],
                                    data_uri = matched_record_def["data_uri"],
                                    bin_udt = matched_record_def["udt_bin"],
                                    mime_type = matched_record_def["mime_type"],
                                    domain_types = json.loads(matched_record_def["domain_types"]),
                                    numerical_format = numerical_format,
                                    bit_depth = matched_record_def["bit_depth"],
                                    extents = matched_record_def["extents"],
                                    sha256 = matched_record_def["sha256"]
                                )

    def get_prefixed_udts(self, udt_prefix, full_string_match=False):
        budt = binary_udt(udt_prefix)
        if full_string_match:
            if budt == udt_prefix:
                raise RuntimeError("Cannot use input binary UDT with full_string_match option")
        records = []
        #print(string_udt(budt))
        tbudt = bytes([(1 | budt[0])]) + budt[1:] # When doing a literal match, we explicitly want to match for binary UDTs matching the extended form too, so we mangle the search string here
        #print(tbudt)
        #print(budt)
        try_pfis = self.get_referencing_indicies(budt)
        for pfi in try_pfis:
            if pfi in self.__data_indicies:
                pfo = self.__parquet_files[pfi]
                for row_group in range(pfo.num_row_groups):
                    rgt = pfo.read_row_group(row_group)
                    filtered_rgt = rgt.filter(pyarrow.compute.or_(pyarrow.compute.starts_with(rgt["udt_bin"], tbudt), pyarrow.compute.equal(rgt["udt_bin"], budt)))
                    filtered_rgt = filtered_rgt.to_pylist(maps_as_pydicts="strict")
                    for matched_record_def in filtered_rgt:
                        if (not full_string_match) or (matched_record_def["udt"].startswith(udt_prefix)): # Optional check for full string match
                            records.append(matched_record_def["udt"])
        return records

    def __get_coherence(self):
        return False

    #def get_entry(self, udt):


    coherent = property(
            fget = __get_coherence,
            doc = "Check if all properties referenced can be accessed. True if all data files avaliable, False if some missing."
        )

class DataRecord:
    def __init__(self, udt, data, last_modified, data_uri=None, bin_udt=None, bin_compact_udt=None, mime_type=None, domain_types=None, numerical_format=None, bit_depth=None, extents=None, sha256=None):
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
        return self.__byte_data

    def as_array(self):
        if self.raw_data is None:
            raise RuntimeError("Unimplemented")
        elif isinstance(self.raw_data, io.BufferedIOBase):
            raise RuntimeError("Unimplemented")
        elif isinstance(self.raw_data, numpy.ndarray):
            return self.raw_data

class ROIRecord:
    def __init__(self, udt, last_modified, extents, uuid = None, annotator = None, annotation_software = None, bin_udt=None, bin_compact_udt=None):
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
        self.annotator = annotator
        self.annotation_software = annotation_software

class AnnotationRecord:
    pass


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
