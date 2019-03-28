from datetime import datetime


feature = {}




values = ["car_4s_count","car_model_count"]
number_values = ["car_4s_count"]

feature['name'] = "car_geo_nubmer"
feature['keys'] = ["geo_city"]
feature['values'] = values
feature['number_values'] = number_values
feature['csv'] = "/user/model/extend_data/car_geo_number.csv"

import json

j = json.dumps(feature, indent=4)
print(j)

with open("/Users/admin/PycharmProjects/zamplus_feature_engineer/libs/imp/inst/json/car_geo_number.json", "w",
          encoding='utf-8') as f:
    json.dump(feature, f, indent=4)