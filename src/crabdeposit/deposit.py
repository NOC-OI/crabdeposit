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

class Deposit:
    def __init__(self):
        self.__parquet_uris = []
        self.__parquet_files = []
        self.__data_indicies = []
        self.__annotation_indicies = []
        self.__udt_map = {}

    def set_deposit_files(self, parquet_uris):
        self.__parquet_uris = parquet_uris
        for pfi, parquet_uri in enumerate(self.__parquet_uris):
            pfo = pyarrow.parquet.ParquetFile(parquet_uri)

            pf_udts_str = None
            #print(pfo.metadata.metadata)
            if pfo.metadata.metadata[b"data_type"] == b"CRAB_DATA_V1":
                pf_udts_str = pfo.metadata.metadata[b"contains_udts"]
                self.__data_indicies.append(pfi)
            elif pfo.metadata.metadata[b"data_type"] == b"CRAB_ANNOTATION_V1":
                pf_udts_str = pfo.metadata.metadata[b"references_udts"]
                self.__annotation_indicies.append(pfi)
            else:
                raise RuntimeError("Unrecognised CRAB deposit file")

            self.__parquet_files.append(pfo)
            #print(pfo.metadata.metadata[b"numerical_format"].decode("utf-8"))
            #self.__parquet_dtypes.append(numpy.dtype(pfo.metadata.metadata[b"numerical_format"].decode("utf-8")))

            if pf_udts_str is not None:
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
            return self.__udt_map[sbudt]
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
                                    value_domain = matched_record_def["value_domain"],
                                    numerical_format = numerical_format,
                                    bit_depth = matched_record_def["bit_depth"],
                                    extents = matched_record_def["extents"],
                                    sha256 = matched_record_def["sha256"]
                                )

    def get_annotation_records(self, udt, full_string_match=False):
        budt = binary_udt(udt)
        if full_string_match:
            if budt == udt:
                raise RuntimeError("Cannot use input binary UDT with full_string_match option")
        try_pfis = self.get_referencing_indicies(budt)
        annotation_records = []
        for pfi in try_pfis:
            if pfi in self.__annotation_indicies:
                pfo = self.__parquet_files[pfi]
                pfo_fields = []
                pfo_discard_fields = []
                for col_nm in pfo.schema.names:
                    if col_nm.startswith("field_"):
                        pfo_fields.append(col_nm[6:])
                    if col_nm.startswith("discard_field_"):
                        pfo_discard_fields.append(col_nm[14:])
                for row_group in range(pfo.num_row_groups):
                    rgt = pfo.read_row_group(row_group)
                    filtered_rgt = rgt.filter(pyarrow.compute.equal(rgt["udt_bin"], budt))
                    filtered_rgt = filtered_rgt.to_pylist(maps_as_pydicts="strict")
                    for matched_record_def in filtered_rgt:
                        if (not full_string_match) or (matched_record_def["udt"] == udt): # Optional check for full string match
                            field_dict = {}
                            discard_field_list = []
                            for field in pfo_fields:
                                field_dict[field] = matched_record_def["field_" + field]
                            for discard_field in pfo_discard_fields:
                                if matched_record_def["discard_field_" + discard_field]:
                                    discard_field_list.append(discard_field)
                            extents_it = iter(matched_record_def["extents"])
                            annotation_records.append(AnnotationRecord(
                                    udt = matched_record_def["udt"],
                                    last_modified = matched_record_def["last_modified"],
                                    extents = zip(extents_it, extents_it),
                                    origin_extents = matched_record_def["origin_extents"],
                                    sha256 = matched_record_def["sha256"],
                                    uuid = matched_record_def["uuid"],
                                    annotator = matched_record_def["annotator"],
                                    annotation_software = matched_record_def["annotation_software"],
                                    bin_udt = matched_record_def["udt_bin"],
                                    discard_in_favour = matched_record_def["discard_in_favour"],
                                    field_dict = field_dict,
                                    discard_field_list = discard_field_list
                                ))
        return annotation_records

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

    def find_annotation_record_matches(self, field_name, match_value):
        annotation_records = []
        full_field_name = "field_" + field_name
        for pfi in self.__annotation_indicies:
            pfo = self.__parquet_files[pfi]
            pfo_fields = []
            pfo_discard_fields = []
            if full_field_name in pfo.schema.names:
                for col_nm in pfo.schema.names:
                    if col_nm.startswith("field_"):
                        pfo_fields.append(col_nm[6:])
                    if col_nm.startswith("discard_field_"):
                        pfo_discard_fields.append(col_nm[14:])
                for row_group in range(pfo.num_row_groups):
                    rgt = pfo.read_row_group(row_group)
                    filtered_rgt = rgt.filter(pyarrow.compute.equal(rgt[full_field_name], match_value))
                    filtered_rgt = filtered_rgt.to_pylist(maps_as_pydicts="strict")
                    for matched_record_def in filtered_rgt:
                        field_dict = {}
                        discard_field_list = []
                        for field in pfo_fields:
                            field_dict[field] = matched_record_def["field_" + field]
                        for discard_field in pfo_discard_fields:
                            if matched_record_def["discard_field_" + discard_field]:
                                discard_field_list.append(discard_field)
                        extents_it = iter(matched_record_def["extents"])
                        annotation_records.append(AnnotationRecord(
                                udt = matched_record_def["udt"],
                                last_modified = matched_record_def["last_modified"],
                                extents = zip(extents_it, extents_it),
                                origin_extents = matched_record_def["origin_extents"],
                                sha256 = matched_record_def["sha256"],
                                uuid = matched_record_def["uuid"],
                                annotator = matched_record_def["annotator"],
                                annotation_software = matched_record_def["annotation_software"],
                                bin_udt = matched_record_def["udt_bin"],
                                discard_in_favour = matched_record_def["discard_in_favour"],
                                field_dict = field_dict,
                                discard_field_list = discard_field_list
                            ))
        return annotation_records

    def __get_coherence(self):
        return False

    #def get_entry(self, udt):


    coherent = property(
            fget = __get_coherence,
            doc = "Check if all properties referenced can be accessed. True if all data files avaliable, False if some missing."
        )
