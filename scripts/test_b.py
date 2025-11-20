from crabdeposit import DepositBuilder, DataRecord, Deposit
import json
from datetime import datetime
import pytz
import time

print("==================[ Test B ]==================")

deposit = Deposit()
deposit.set_deposit_files(["testout/D20250530.parquet"])
print(deposit.get_all_compact_udts())

drs = deposit.get_prefixed_udts("udt1_2dc621accf3d_c224a43f022373c2_00006838f56e")
print("Data records found:" + str(len(drs)))

import cv2
for i in range(0,25):
    gray_image = cv2.cvtColor(deposit.get_data_record(drs[i]).as_array(), cv2.COLOR_GRAY2BGR)
    cv2.imshow("Image " + str(i) + " from deposit", gray_image)
cv2.waitKey(10000)
