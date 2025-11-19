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
import struct
import pyarrow
import pyarrow.parquet
import pyarrow.compute
from datetime import datetime
from .udt import string_udt, binary_udt, small_udt

class Deposit:
    def __init__(self):
        self.__parquet_uris = []
        self.__parquet_files = []
        self.__parquet_dtypes = []
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
            self.__parquet_dtypes.append(numpy.dtype(pfo.metadata.metadata[b"numerical_format"].decode("utf-8")))

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
                dtype = self.__parquet_dtypes[pfi]
                for row_group in range(pfo.num_row_groups):
                    rgt = pfo.read_row_group(row_group)
                    filtered_rgt = rgt.filter(pyarrow.compute.equal(rgt["udt_bin"], budt))
                    filtered_rgt = filtered_rgt.to_pylist(maps_as_pydicts="strict")
                    for matched_record_def in filtered_rgt:
                        if (not full_string_match) or (matched_record_def["udt"] == udt): # Optional check for full string match
                            numpy_array = numpy.frombuffer(matched_record_def["data"], dtype=dtype)
                            numpy_array = numpy.reshape(numpy_array, shape=matched_record_def["extents"], order="C")
                            return DataRecord(matched_record_def["udt"], numpy_array, matched_record_def["last_modified"])

    def get_data_records(self, udt, full_string_match=False):
        budt = binary_udt(udt)
        if full_string_match:
            if budt == udt:
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
                dtype = self.__parquet_dtypes[pfi]
                for row_group in range(pfo.num_row_groups):
                    rgt = pfo.read_row_group(row_group)
                    filtered_rgt = rgt.filter(pyarrow.compute.or_(pyarrow.compute.starts_with(rgt["udt_bin"], tbudt), pyarrow.compute.equal(rgt["udt_bin"], budt)))
                    filtered_rgt = filtered_rgt.to_pylist(maps_as_pydicts="strict")
                    for matched_record_def in filtered_rgt:
                        if (not full_string_match) or (matched_record_def["udt"].startswith(udt)): # Optional check for full string match
                            numpy_array = numpy.frombuffer(matched_record_def["data"], dtype=dtype)
                            numpy_array = numpy.reshape(numpy_array, shape=matched_record_def["extents"], order="C")
                            records.append(DataRecord(matched_record_def["udt"], numpy_array, matched_record_def["last_modified"]))
        return records

    def __get_coherence(self):
        return False

    #def get_entry(self, udt):


    coherent = property(
            fget = __get_coherence,
            doc = "Check if all properties referenced can be accessed. True if all data files avaliable, False if some missing."
        )

class DataRecord:
    def __init__(self, udt, numpy_array, last_modified, bin_udt=None, bin_compact_udt=None):
        self.data = numpy_array
        self.udt = udt
        self.last_modified = last_modified
        self.bin_udt = bin_udt
        if self.bin_udt is None:
            self.bin_udt = binary_udt(self.udt)
        self.bin_compact_udt = bin_compact_udt
        if self.bin_compact_udt is None:
            self.bin_compact_udt = small_udt(self.bin_udt)


class ROIRecord:
    pass

class AnnotationRecord:
    pass


class DepositBuilder:
    def __init__(self):
        self.__data_provider = iter([])
        self.__roi_provider = iter([])
        self.__annotation_provider = iter([])
        self.__domain_types = []
        self.__dataset_compact_udts = None
        self.__bit_depth = None
        self.__data_type = None
        self.__data_out_uri = "crabdata.parquet"

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
        except StopIteration:
            raise StopIteration("Cannot operate on an empty data provider")
        fst_pull = True

        if self.__data_type == None:
            self.__data_type = str(record.data.dtype)
        if self.__bit_depth == None:
            dtype_numbers = re.findall(r'\d+', self.__data_type)
            self.__bit_depth = int(dtype_numbers[0])

        pa_dtype = pyarrow.from_numpy_dtype(numpy.dtype(self.__data_type))

        data_size_approx_kb = 32
        batch_num = 0
        data_batch_size = int(65536 / data_size_approx_kb) # Targeting batch size of ~ 64mb assuming a per-record size of about 32kb
        #data_batch_size = 256

        data_schema = pyarrow.schema([
            ("udt", pyarrow.string()),
            ("last_modified", pyarrow.timestamp('s', tz='UTC')),
            ("extents", pyarrow.list_(pyarrow.uint64())),
            ("udt_bin", pyarrow.binary()),
            ("data", pyarrow.binary())
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
                    data_batch_data.append(record.data.tobytes(order="C"))
                    data_batch_last_modified.append(record.last_modified)
                    data_batch_extents.append(pyarrow.array(list(record.data.shape), type=pyarrow.uint64()))

            except StopIteration:
                exhausted = True

            data_batch = pyarrow.RecordBatch.from_pydict({
                "udt": data_batch_udt,
                "udt_bin": data_batch_udt_bin,
                "data": data_batch_data,
                "last_modified": data_batch_last_modified,
                "extents": data_batch_extents
                }, schema=data_schema)
            data_parquet_writer.write_batch(data_batch)
            batch_num += 1
            #print("Writing " + str(len(data_batch_udt)) + " records to batch " + str(batch_num))

        data_metadata = {
                "data_type": "CRAB_DATA_V1",
                "last_modified": struct.pack("<Q", int(datetime.utcnow().timestamp())),
                "domain_types": json.dumps(self.__domain_types),
                "bit_depth": struct.pack("<Q", self.__bit_depth),
                "numerical_format": self.__data_type,
                "contains_udts": b"".join(self.__dataset_compact_udts),
            }
        data_parquet_writer.add_key_value_metadata(data_metadata)
        data_parquet_writer.close()

        return True
