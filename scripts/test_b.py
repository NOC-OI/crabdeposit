from crabdeposit import DepositBuilder, DataRecord, Deposit, AnnotationRecord
import json
from datetime import datetime
import pytz
import time
import csv
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

print("==================[ Test B ]==================")

original_deposit = Deposit()
original_deposit.set_deposit_files(["testout/D20250530_data.parquet", "testout/D20250530_annotation.parquet"])

lookup_table = {
        "cyst<Dinophyceae": "https://www.gbif.org/species/9049014",
        "small-dinoflagellates": "https://www.gbif.org/species/9049014",
        "Polykrikos sp.": "https://www.gbif.org/species/7728817",
        "Prorocentrum sp.": "https://www.gbif.org/species/8069717",
        "diatom-pennate": "https://www.gbif.org/species/7947184",
        "scripsiella": "https://www.gbif.org/species/8426834",
        "Chaetoceros chain": "https://www.gbif.org/species/3192923",
        "Chaetoceros single": "https://www.gbif.org/species/3192923",
        "thalassiosira chain": "https://www.gbif.org/species/9623827",
        "thalassiosira single": "https://www.gbif.org/species/9623827",
        "ciliate-other": "https://www.gbif.org/species/3269382",
        "nauplii calanoida": "https://www.gbif.org/species/679",
        "Neoceratium fusus<Neoceratium": "https://www.gbif.org/species/7539507",
        "Amphidinium sp.<Amphidinium": "https://www.gbif.org/species/3207397",
        "round cells": "round cells",
        "dark<sphere": "dark<sphere",
        "Dinophyceae": "https://www.gbif.org/species/9049014",
        "light<fluffy": "light<fluffy",
        "Bacillariophyceae": "https://www.gbif.org/species/7947184",
        "Cylindrotheca": "https://www.gbif.org/species/3243149",
        "detritus": "detritus",
        "aggregate<detritus": "aggregate<detritus",
        "Leptocylindrus": "https://www.gbif.org/species/3193082",
        "frustule": "frustule",
        "Pleurosigma": "https://www.gbif.org/species/7650819",
        "multiple organisms": "multiple organisms",
        "Odontella sinensis<Odontella": "Odontella sinensis<Odontella",
        "Asterionellopsis": "https://www.gbif.org/species/3192607",
        "Guinardia": "https://www.gbif.org/species/3194104",
        "Navicula": "https://www.gbif.org/species/8868958",
        "fiber<detritus": "fiber<detritus",
        "faecal pellet": "faecal pellet",
        "blurry": "blurry",
        "Licmophora": "https://www.gbif.org/species/3195666",
        "Dictyocha": "https://www.gbif.org/species/5422131",
        "Gyrodinium": "https://www.gbif.org/species/3207317",
        "Rhizosolenia": "https://www.gbif.org/species/7850218",
        "Proboscia": "https://www.gbif.org/species/3194133",
        "Eucampia": "https://www.gbif.org/species/9569131",
        "artefact": "artefact",
        "Gyrodinium spirale": "https://www.gbif.org/species/8244445",
        "Neoceratium": "https://www.gbif.org/species/7506953",
        "veliger larvae": "veliger larvae",
        "Torodinium": "https://www.gbif.org/species/8100015",
        "Lauderia": "https://www.gbif.org/species/9504785",
        "Radiolaria": "https://www.gbif.org/species/4298634",
        "Pyramimonas": "https://www.gbif.org/species/9618316",
        "Thalassionema": "https://www.gbif.org/species/7858065",
        "Dinophysis": "https://www.gbif.org/species/8327141",
        "multiple species": "multiple species",
        "Cochlodinium": "https://www.gbif.org/species/7565797",
        "Eutintinnus": "https://www.gbif.org/species/3204748",
        "Tiarina fusus": "https://www.gbif.org/species/7883469",
        "Warnowia": "https://www.gbif.org/species/7947742",
        "Rotifera": "https://www.gbif.org/species/91",
        "Gymnodinium": "https://www.gbif.org/species/3207519",
        "Crustacea": "https://www.gbif.org/species/10996236",
        "Nematodinium": "https://www.gbif.org/species/7689919",
        "Neoceratium tripos": "https://www.gbif.org/species/7766417"
    }

def lookup_name(search_term):
    if search_term not in lookup_table.keys():
        return None
    return lookup_table[search_term]

annotations_to_export = []

# EcoTaxa places a UTF-8 BOM at the start of every line! This needs to be accounted for, and is very odd
with open("testdata/ecotaxa_export__TSV_18392_20251119_1039.tsv", encoding="utf-8-sig") as fh:
    #print(json.dumps(fh.readline()))
    tsv_reader = csv.DictReader(fh, delimiter='\t')
    for row in tsv_reader:
        if row["object_annotation_status"] == "validated":
            gbif_id = lookup_name(row["object_annotation_category"])
            annotation_records = original_deposit.get_annotation_records(ifcb_id_to_udt(row["object_id"]))
            if len(annotation_records) > 0:
                existing_annotation_record = annotation_records[0]


                annotation_datetime = datetime.strptime(row["object_annotation_date"] + "T" + row["object_annotation_time"], "%Y%m%dT%H%M%S").astimezone(pytz.utc).timestamp()
                annotator_match = row["object_annotation_person_email"]

                output_annotation_dict = {
                        "taxon": gbif_id,
                        "label": row["object_annotation_category"]
                    }

                output_annotation_record = AnnotationRecord(
                        existing_annotation_record.udt,
                        int(annotation_datetime),
                        existing_annotation_record.extents,
                        existing_annotation_record.origin_extents,
                        existing_annotation_record.sha256,
                        uuid.uuid4(),
                        annotator = annotator_match,
                        annotation_software = "https://github.com/ecotaxa/ecotaxa",
                        field_dict = output_annotation_dict
                    )
                annotations_to_export.append(output_annotation_record)
            else:
                pass
                #print("MISSING ROI RECORD FOR " + row["object_id"])


deposit_builder = DepositBuilder()
deposit_builder.set_annotation_provider(iter(annotations_to_export))
deposit_builder.set_annotation_export_uri("testout/D20250530_ecotaxa_annotations.parquet")
deposit_builder.register_fields(["taxon", "label"], str)
deposit_ecotaxa_annotations = deposit_builder.build()
