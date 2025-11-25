from crabdeposit import DepositBuilder, DataRecord, AnnotationRecord, Deposit
from libifcb import ROIReader
import json
from datetime import datetime
import pytz
import hashlib
import uuid

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
                    self.index = 0
            dt = datetime.strptime(self.ifcb_ids[self.reader_index].split("_")[0], "D%Y%m%dT%H%M%S").replace(tzinfo=pytz.UTC)
            observation_id = self.ifcb_ids[self.reader_index] + "_" + str(roi.index).zfill(5)

            data_record = DataRecord(
                                udt = ifcb_id_to_udt(observation_id),
                                data = roi.array,
                                last_modified = dt,
                                domain_types = ["spatial 3.5714285714285716e-07 m", "spatial 3.5714285714285716e-07 m"],
                                bit_depth = 8
                            )

            #DataRecord(, , dt)

            return data_record
        raise StopIteration

class IFCBAnnotationProvider:
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
                    self.index = 0
            dt = datetime.strptime(self.ifcb_ids[self.reader_index].split("_")[0], "D%Y%m%dT%H%M%S").replace(tzinfo=pytz.UTC)
            observation_id = self.ifcb_ids[self.reader_index] + "_" + str(roi.index).zfill(5)
            extents = [(0, roi.array.shape[0]), (0, roi.array.shape[1])]
            origin_extents = roi.array.shape

            m = hashlib.sha256()
            m.update(roi.array.tobytes(order="C"))
            sha256 = m.digest()

            #print(roi.trigger.raw.keys())

            trigger_values = {}
            for key in roi.trigger.raw.keys():
                trigger_values[key] = float(roi.trigger.raw[key])

            annotation_record = AnnotationRecord(ifcb_id_to_udt(observation_id), dt, extents, origin_extents, sha256, uuid.uuid4(), annotator = None, annotation_software = "https://github.com/NOC-OI/ifcbproc", field_dict = trigger_values)
            return annotation_record
        raise StopIteration

print("==================[ Test A ]==================")

#import cv2
#im = cv2.imread("testdata/D20140117T003426_IFCB014_02177.jpeg")
#cv2.imshow("From JPEG", im)
#gray_image = cv2.cvtColor(dr.data, cv2.COLOR_GRAY2BGR)
#cv2.imshow("From Parquet", gray_image)
#cv2.waitKey(10000)

ifcb_bins = [
        "D20250530T000150_IFCB225",
        "D20250530T002259_IFCB225",
        "D20250530T004408_IFCB225",
        "D20250530T010516_IFCB225",
        "D20250530T061017_IFCB225"
    ]
roi_readers = []
for ifcb_bin in ifcb_bins:
    roi_readers.append(ROIReader("testdata/" + ifcb_bin + ".hdr", "testdata/" + ifcb_bin + ".adc", "testdata/" + ifcb_bin + ".roi"))

data_provider = IFCBDataProvider(roi_readers, ifcb_bins)
annotation_provider = IFCBAnnotationProvider(roi_readers, ifcb_bins)

deposit_builder = DepositBuilder()
deposit_builder.set_data_provider(data_provider)
deposit_builder.set_annotation_provider(annotation_provider)
deposit_builder.set_data_export_uri("testout/D20250530_data.parquet")
deposit_builder.set_annotation_export_uri("testout/D20250530_annotation.parquet")
deposit_builder.register_fields(['trigger_number', 'roi_x', 'roi_y', 'roi_width', 'roi_height', 'start_byte', 'comparator_out', 'start_point', 'signal_length', 'status'], int)
deposit_builder.register_fields(['adc_time', 'pmt_a', 'pmt_b', 'peak_a', 'peak_b', 'time_of_flight', 'grab_time_start', 'grab_time_end', 'run_time', 'inhibit_time'], float)
deposit_2025 = deposit_builder.build()

print(deposit_2025.get_all_compact_udts())
print("Generated data deposit file!")
