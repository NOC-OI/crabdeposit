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
import hashlib
import pyarrow
import pyarrow.parquet as pyarrow_parquet
from datetime import datetime

def binary_udt(big_udt):
    vdp = None
    did = None
    ts = None
    imid = None
    if isinstance(big_udt, (bytes, bytearray)):
        return big_udt
    if big_udt.startswith("udt1__"):
        udt_cmps = big_udt[6:].split("__")
        vendor = udt_cmps[0]
        device = udt_cmps[1]
        vdp = hashlib.sha256((vendor + "__" + device).encode("utf-8")).digest()[0:6]
        device_id = udt_cmps[2].lower()
        did = hashlib.sha256((device_id).encode("utf-8")).digest()[0:8]
        timestamp = int(udt_cmps[3])
        ts = struct.pack(">Q", timestamp)[2:8]
        if len(udt_cmps) > 4:
            imid = hashlib.sha256((udt_cmps[4]).encode("utf-8")).digest()[0:8]
    elif big_udt.startswith("udt1_"):
        udt_cmps = big_udt[5:].split("_")
        vdp = bytes.fromhex(udt_cmps[0])
        did = bytes.fromhex(udt_cmps[1])
        ts = bytes.fromhex(udt_cmps[2])
        if len(udt_cmps) > 3:
            imid = bytes.fromhex(udt_cmps[3])
    else:
        return udt
    if imid is None:
        return b'\x02' + vdp + did + ts
    else:
        return b'\x03' + vdp + did + ts + imid

def small_udt(big_udt):
    if isinstance(big_udt, (bytes, bytearray)):
        if big_udt[0] == 2:
            return big_udt
        elif big_udt[0] == 3:
            vdp = big_udt[1:7]
            did = big_udt[7:15]
            ts = big_udt[15:21]
            return b'\x02' + vdp + did + ts
        else:
            raise RuntimeError("Unrecognised Binary UDT")
    elif big_udt.startswith("udt1__"):
        udt_cmps = big_udt[6:].split("__")
        vendor = udt_cmps[0]
        device = udt_cmps[1]
        vdp = hashlib.sha256((vendor + "__" + device).encode("utf-8")).hexdigest()[0:12]
        device_id = udt_cmps[2].lower()
        did = hashlib.sha256((device_id).encode("utf-8")).hexdigest()[0:16]
        timestamp = int(udt_cmps[3])
        ts = '{:012x}'.format(timestamp)
        small_udt = "udt1_" + vdp + "_" + did + "_" + ts
        if len(udt_cmps) > 4:
            imid = udt_cmps[4]
            small_udt = small_udt + "_" + hashlib.sha256((imid).encode("utf-8")).hexdigest()[0:16]
        return small_udt
    else:
        return big_udt

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
            pfo = pyarrow_parquet.ParquetFile(parquet_uri)
            self.__parquet_files.append(pfo)
            #print(pfo.metadata.metadata)
            if pfo.metadata.metadata[b"data_type"] == b"CRAB_DATA_V1":
                self.__data_indicies.append(pfi)
            elif pfo.metadata.metadata[b"data_type"] == b"CRAB_ROI_V1":
                self.__roi_indicies.append(pfi)
            elif pfo.metadata.metadata[b"data_type"] == b"CRAB_ANNOTATION_V1":
                self.__annotation_indicies.append(pfi)
            else:
                raise RuntimeError("Unrecognised CRAB deposit file")

            pf_udts_str = pfo.metadata.metadata[b"contains_udts"]
            pf_udts = [pf_udts_str[i:i+21] for i in range(0, len(pf_udts_str), 21)]
            for pf_udt in pf_udts:
                if not pf_udt in self.__udt_map.keys():
                    self.__udt_map[pf_udt] = []
                self.__udt_map[pf_udt].append(pfi)

    def get_all_compact_udts(self):
        return self.__udt_map.keys()

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
                            numerical_format = pfo.metadata.metadata[b"numerical_format"]
                            dtype = numpy.uint8
                            if numerical_format == "uint16":
                                dtype = numpy.uint16
                            elif numerical_format == "uint32":
                                dtype = numpy.uint32
                            elif numerical_format == "uint64":
                                dtype = numpy.uint64
                            elif numerical_format == "float16":
                                dtype = numpy.float16
                            elif numerical_format == "float32":
                                dtype = numpy.float32
                            elif numerical_format == "float64":
                                dtype = numpy.float64
                            elif numerical_format == "float128":
                                dtype = numpy.float128
                            elif numerical_format == "complex64":
                                dtype = numpy.complext64
                            elif numerical_format == "complex128":
                                dtype = numpy.complex128
                            elif numerical_format == "complex256":
                                dtype = numpy.complex256
                            numpy_array = numpy.frombuffer(matched_record_def["data"], dtype=dtype)
                            numpy_array = numpy.reshape(numpy_array, shape=matched_record_def["extents"], order="C")
                            return DataRecord(matched_record_def["udt"], numpy_array, matched_record_def["last_modified"])

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


class DepositBuilder:
    def __init__(self):
        self.__data_provider = iter([])
        self.__roi_provider = iter([])
        self.__annotation_provider = iter([])
        self.__domain_types = []
        self.__dataset_compact_udts = None
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

    def add_udt(self, udt):
        if self.__dataset_compact_udts is None:
            self.__dataset_compact_udts = []
        self.__dataset_compact_udts.append(small_udt(binary_udt(udt)))
        return self

    def build(self):
        data_bit_depth = 8
        data_stored_bit_depth = 8
        data_size_approx_kb = 32
        data_batch_size = int(65536 / data_size_approx_kb) # Targeting batch size of ~ 64mb assuming a per-record size of about 32kb

        data_schema = pyarrow.schema([
            ("udt", pyarrow.string()),
            ("last_modified", pyarrow.timestamp('s', tz='UTC')),
            ("extents", pyarrow.list_(pyarrow.uint64())),
            ("udt_bin", pyarrow.binary()),
            ("data", pyarrow.binary())
        ])
        data_parquet_writer = pyarrow_parquet.ParquetWriter(self.__data_out_uri, data_schema)
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

        data_metadata = {
                "data_type": "CRAB_DATA_V1",
                "last_modified": struct.pack("<Q", int(datetime.utcnow().timestamp())),
                "domain_types": json.dumps(self.__domain_types),
                "bit_depth": struct.pack("<Q", data_bit_depth),
                "numerical_format": "uint64",
                "contains_udts": b"".join(self.__dataset_compact_udts),
            }
        data_parquet_writer.add_key_value_metadata(data_metadata)
        data_parquet_writer.close()

        return True
