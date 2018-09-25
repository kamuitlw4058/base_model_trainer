from datetime import datetime


feature = {}




values = ["car_4s_count","car_model_count"]


feature['name'] = "car_geo"
feature['keys'] = ["geo_city"]
feature['values'] = values
feature['csv'] = "/user/model/extend_data/car_geo.csv"

import json

j = json.dumps(feature, indent=4)
print(j)

with open("/Users/admin/PycharmProjects/zamplus_feature_engineer/libs/feature/inst/json/car_geo.json", "w",
          encoding='utf-8') as f:
    json.dump(feature, f, indent=4)