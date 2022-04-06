#!/usr/bin/env python3

import pandas as pd
from mongo_models import number as NumberModel
import config

inc = 0
for chunk in pd.read_csv('./files/' + config.csv_filename, chunksize=1000000):
    inc += 1
    insert = {}
    update = {}

    for index, row in chunk.iterrows():
        phone = row[0]
        domain = row[1]

        if (phone in insert):
            insert[phone]['quality'] += 1
        else:
            insert[phone] = { "phone": phone, "domain": domain, "quality": 1 }

    if (bool(insert)):
        NumberModel().insert_many(insert.values())
        print(f"chunk {inc} inserted")