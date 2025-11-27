from crabdeposit import DepositBuilder, DataRecord, Deposit, DepositFile
import json
from datetime import datetime
import pytz
import time
import cv2

print("==================[ Test C ]==================")

data_deposit_file = DepositFile("testout/D20250530_data.parquet")
print(data_deposit_file.get_nse_udts())

full_deposit = Deposit()
full_deposit.set_deposit_files(["testout/D20250530_data.parquet", "testout/D20250530_annotation.parquet", "testout/D20250530_ecotaxa_annotations.parquet"])

records = full_deposit.find_annotation_record_matches("label", "diatom-pennate")
for record in records:
    print(record.udt)
    # Show me the diatom!
    gray_image = cv2.cvtColor(full_deposit.get_data_record(record.udt).as_array(), cv2.COLOR_GRAY2BGR)
    cv2.imshow("\"diatom-pennate\" from deposit, annotated by " + record.annotator, gray_image)
    cv2.waitKey(10000)

