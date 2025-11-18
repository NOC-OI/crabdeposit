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
udt.py

Utilities for working with UDTs
'''

import struct
import hashlib

def string_udt(bin_udt):
    if isinstance(bin_udt, (bytes, bytearray)):
        if bin_udt[0] == 2 or bin_udt[0] == 3:
            vdp = bin_udt[1:7]
            did = bin_udt[7:15]
            ts = bin_udt[15:21]
            imid = None
            if bin_udt[0] == 3:
                imid = bin_udt[21:29]
            if imid is None:
                return "udt1_" + vdp.hex() + "_" + did.hex() + "_" + ts.hex()
            else:
                return "udt1_" + vdp.hex() + "_" + did.hex() + "_" + ts.hex() + "_" + imid.hex()
        else:
            raise RuntimeError("Unrecognised Binary UDT")
    else:
        return bin_udt

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
