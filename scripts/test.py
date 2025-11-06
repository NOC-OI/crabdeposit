from crabdeposit import DepositBuilder, DataRecord, Deposit
from libifcb import ROIReader
import json
from datetime import datetime
import pytz

def ifcb_id_to_udt(ifcb_id):
    id_split = ifcb_id.split("_")
    dt = datetime.strptime(id_split[0], "D%Y%m%dT%H%M%S").replace(tzinfo=pytz.UTC)
    res = int(dt.timestamp())
    udt = "udt1__usa_mc_lane_research_laboratories__imaging_flow_cytobot__" + id_split[1].lower() + "__" + str(res)
    if len(id_split) > 2:
        imid = int(id_split[2])
        udt = udt + "__" + str(imid)
    return udt

class IFCBDataProvider:
    def __init__(self, roi_readers, ifcb_ids):
        self.roi_readers = roi_readers
        self.ifcb_ids = ifcb_ids
        self.reader_index = 0
        self.index = 0

    def __iter__(self):
        return self

    def __next__(self):
        if self.index < len(self.roi_readers[self.reader_index].rois):
            roi = self.roi_readers[self.reader_index].rois[self.index]
            self.index += 1
            if self.index >= len(self.roi_readers[self.reader_index].rois):
                if self.reader_index < (len(self.roi_readers) - 1):
                    self.reader_index += 1
            dt = datetime.strptime(self.ifcb_ids[self.reader_index].split("_")[0], "D%Y%m%dT%H%M%S").replace(tzinfo=pytz.UTC)
            observation_id = self.ifcb_ids[self.reader_index] + "_" + str(roi.index).zfill(5)
            data_record = DataRecord(ifcb_id_to_udt(observation_id), roi.array, dt)
            return data_record
        raise StopIteration


roi_readers = [ROIReader("testdata/D20140117T003426_IFCB014.hdr", "testdata/D20140117T003426_IFCB014.adc", "testdata/D20140117T003426_IFCB014.roi")]
ifcb_ids = ["D20140117T003426_IFCB014"]
data_provider = IFCBDataProvider(roi_readers, ifcb_ids)

DepositBuilder().set_data_provider(data_provider).set_export_uri("testout/crabdep.parquet").build()

deposit = Deposit()
deposit.set_deposit_files(["testout/crabdep.parquet"])
deposit.get_all_compact_udts()
dr = deposit.get_data_record("udt1__usa_mc_lane_research_laboratories__imaging_flow_cytobot__ifcb014__1389918866__2176")

import cv2
im = cv2.imread("testdata/D20140117T003426_IFCB014_02177.jpeg")
cv2.imshow("From JPEG", im)

gray_image = cv2.cvtColor(dr.data, cv2.COLOR_GRAY2BGR)
cv2.imshow("From Parquet", gray_image)

cv2.waitKey(10000)
