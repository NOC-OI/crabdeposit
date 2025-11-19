from crabdeposit import DepositBuilder, DataRecord, Deposit
import json
from datetime import datetime
import pytz
import time

print("Test B")

ts1 = time.time()
deposit = Deposit()
deposit.set_deposit_files(["testout/D20140117T003426_IFCB014.parquet"])
print(deposit.get_all_compact_udts())
ts2 = time.time()
dr = deposit.get_data_record("udt1__usa_mc_lane_research_laboratories__imaging_flow_cytobot__ifcb014__1389918866__2176")
ts3 = time.time()
drs = deposit.get_data_records("udt1_2dc621accf3d_ffa788da37f2efa8_000052d87a92")
ts4 = time.time()
print(len(drs))

print(ts2-ts1, ts3-ts2, ts4-ts3)

import cv2
gray_image = cv2.cvtColor(dr.data, cv2.COLOR_GRAY2BGR)
cv2.imshow("From Parquet", gray_image)
cv2.waitKey(10000)
