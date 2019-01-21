
sql = """
SELECT 
    Zid as Id_Zid, 
    activity_time[1]  as ath_onehot_0,
	activity_time[2]  as ath_onehot_1,
	activity_time[3]  as ath_onehot_2,
	activity_time[4]  as ath_onehot_3,
	activity_time[5]  as ath_onehot_4,
	activity_time[6]  as ath_onehot_5,
	activity_time[7]  as ath_onehot_6,
	activity_time[8]  as ath_onehot_7,
	activity_time[9]  as ath_onehot_8,
	activity_time[10]  as ath_onehot_9,
	activity_time[11]  as ath_onehot_10,
	activity_time[12]  as ath_onehot_11,
	activity_time[13]  as ath_onehot_12,
	activity_time[14]  as ath_onehot_13,
	activity_time[15]  as ath_onehot_14,
	activity_time[16]  as ath_onehot_15,
	activity_time[17]  as ath_onehot_16,
	activity_time[18]  as ath_onehot_17,
	activity_time[19]  as ath_onehot_18,
	activity_time[20]  as ath_onehot_19,
	activity_time[21]  as ath_onehot_20,
	activity_time[22]  as ath_onehot_21,
	activity_time[23]  as ath_onehot_22,
	activity_time[24]  as ath_onehot_23,
	round(activity_time[1]/arraySum(activity_time),2)   as athr_onehot_0,
	round(activity_time[2]/arraySum(activity_time),2)   as athr_onehot_1,
	round(activity_time[3]/arraySum(activity_time),2)   as athr_onehot_2,
	round(activity_time[4]/arraySum(activity_time),2)   as athr_onehot_3,
	round(activity_time[5]/arraySum(activity_time),2)   as athr_onehot_4,
	round(activity_time[6]/arraySum(activity_time),2)   as athr_onehot_5,
	round(activity_time[7]/arraySum(activity_time),2)   as athr_onehot_6,
	round(activity_time[8]/arraySum(activity_time),2)   as athr_onehot_7,
	round(activity_time[9]/arraySum(activity_time),2)   as athr_onehot_8,
	round(activity_time[10]/arraySum(activity_time),2)  as athr_onehot_9,
	round(activity_time[11]/arraySum(activity_time),2)  as athr_onehot_10,
	round(activity_time[12]/arraySum(activity_time),2)  as athr_onehot_11,
	round(activity_time[13]/arraySum(activity_time),2)  as athr_onehot_12,
	round(activity_time[14]/arraySum(activity_time),2)  as athr_onehot_13,
	round(activity_time[15]/arraySum(activity_time),2)  as athr_onehot_14,
	round(activity_time[16]/arraySum(activity_time),2)  as athr_onehot_15,
	round(activity_time[17]/arraySum(activity_time),2)  as athr_onehot_16,
	round(activity_time[18]/arraySum(activity_time),2)  as athr_onehot_17,
	round(activity_time[19]/arraySum(activity_time),2)  as athr_onehot_18,
	round(activity_time[20]/arraySum(activity_time),2)  as athr_onehot_19,
	round(activity_time[21]/arraySum(activity_time),2)  as athr_onehot_20,
	round(activity_time[22]/arraySum(activity_time),2)  as athr_onehot_21,
	round(activity_time[23]/arraySum(activity_time),2)  as athr_onehot_22,
	round(activity_time[24]/arraySum(activity_time),2)  as athr_onehot_23
FROM audience.true_sight
where Id_Zid GLOBAL in (
select distinct toString(Id_Zid)
from zampda.rtb_all
where EventDate>today()-5
and Bid_CompanyId = 12
and Media_VendorId = 24
and notEmpty(Click.Timestamp)
and Device_Os='android'
)
"""

d = {}
d['sql'] = sql

values = [
    "ath_onehot_0",
    "ath_onehot_1",
    "ath_onehot_2",
    "ath_onehot_3",
    "ath_onehot_4",
    "ath_onehot_5",
    "ath_onehot_6",
    "ath_onehot_7",
    "ath_onehot_8",
    "ath_onehot_9",
    "ath_onehot_10",
    "ath_onehot_11",
    "ath_onehot_12",
    "ath_onehot_13",
    "ath_onehot_14",
    "ath_onehot_15",
    "ath_onehot_16",
    "ath_onehot_17",
    "ath_onehot_18",
    "ath_onehot_19",
    "ath_onehot_20",
    "ath_onehot_21",
    "ath_onehot_22",
    "ath_onehot_23",
    "athr_onehot_0",
    "athr_onehot_1",
    "athr_onehot_2",
    "athr_onehot_3",
    "athr_onehot_4",
    "athr_onehot_5",
    "athr_onehot_6",
    "athr_onehot_7",
    "athr_onehot_8",
    "athr_onehot_9",
    "athr_onehot_10",
    "athr_onehot_11",
    "athr_onehot_12",
    "athr_onehot_13",
    "athr_onehot_14",
    "athr_onehot_15",
    "athr_onehot_16",
    "athr_onehot_17",
    "athr_onehot_18",
    "athr_onehot_19",
    "athr_onehot_20",
    "athr_onehot_21",
    "athr_onehot_22",
    "athr_onehot_23",
]


d['name'] = "zid_site_inspection_onehot"
d['keys'] = ["Id_Zid"]
d['values'] = values
d['once_sql'] = True

import json

j = json.dumps(d, indent=4)
print(j)

with open("/Users/admin/PycharmProjects/zamplus_feature_engineer/libs/feature/inst/json/zid_site_inspection_onehot.json", "w",
          encoding='utf-8') as f:
    json.dump(d, f, indent=4)