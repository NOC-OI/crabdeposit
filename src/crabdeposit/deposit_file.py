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

import pyarrow
import pyarrow.parquet
import pyarrow.compute

class DepositFile:
    def __init__(self, input_file):
        self.__budt_list = []

        if isinstance(input_file, pyarrow.parquet.ParquetFile):
            self.parquet_file = input_file
        elif isinstance(input_file, str):
            self.parquet_file = pyarrow.parquet.ParquetFile(input_file)

        pf_udts_str = None
        if self.parquet_file.metadata.metadata[b"data_type"] == b"CRAB_DATA_V1":
            pf_udts_str = self.parquet_file.metadata.metadata[b"contains_udts"]
            self.__file_type = "DATA"
        elif self.parquet_file.metadata.metadata[b"data_type"] == b"CRAB_ANNOTATION_V1":
            pf_udts_str = self.parquet_file.metadata.metadata[b"references_udts"]
            self.__file_type = "ANNOTATION"
        else:
            raise RuntimeError("Unrecognised CRAB deposit file")

        self.__custom_metadata_dict = {}

        for key in self.parquet_file.metadata.metadata.keys():
            key_decoded = key.decode("utf-8")
            if key_decoded.startswith("x_"):
                self.__custom_metadata_dict[key_decoded[2:]] = self.parquet_file.metadata.metadata[key].decode("utf-8")

        if pf_udts_str is not None:
            self.__budt_list = [pf_udts_str[i:i+21] for i in range(0, len(pf_udts_str), 21)]
        else:
            self.__budt_list = []

    def get_type(self):
        return self.__file_type

    # Get Non Specific Entry (i.e. Without element id) Binary UDTs
    def get_nse_budts(self):
        return self.__budt_list

    def get_metadata_dict(self):
        return self.__custom_metadata_dict
