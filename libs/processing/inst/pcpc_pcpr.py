test_sql = """
select
	Id_Zid,
	round(avg(toFloat32OrZero(ExtMap4.Value[2])),5) as avgPCTR,
	round(min(toFloat32OrZero(ExtMap4.Value[2])),5) as minPCTR,
	round(max(toFloat32OrZero(ExtMap4.Value[2])),5) as maxPCTR,
	groupArray(Geo_Longitude) as Geo_Longitude,
	groupArray(Geo_Latitude) as Geo_Latitude,
	groupArray(Media_Domain) as app,
	groupArray(Model_BidPrice) as bid_price,
	groupArray(Win_Price) as price,
	groupArray(ExtMap4.Value[1]) as pcpc,
	groupArray(ExtMap4.Value[2]) as pctr,
	groupArray(BidScore_Key) as bidKey,
	groupArray(BidScore_Value) as bidScore,
	groupArray(PvScore.Key) as pvKey,
	groupArray(ErrorCode.Adgroup) as errorAdGroup,
	groupArray(ErrorCode.Code) as errorCode,
	count(ExtMap4.Value[2])
from
	zampda.rtb_all
prewhere
	EventDate >= today() -30
where 
    Id_Zid = '09a5af5f83c8cdc88118610c1d316bb4'
group  by Id_Zid
FORMAT JSON


"""


"""
bc72e3b0fcabaad43444b3ffc7dea151
09a5af5f83c8cdc88118610c1d316bb4"""

bid_no_win ="""

select 
    arrayFilter(x-> x= 3 or x = 2 or x = 5 or x = 7,Event.Type) as type,
    arrayFilter(y,x-> x= 3 or x = 2 or x = 5 or x = 7,Event.GPrice,Event.Type) as price
from audience.user_event
where length (arrayFilter(x-> x= 7 ,Event.Type)) > 0
limit 1
FORMAT JSON
"""

"""



"""




sql ="""
select
	Id_Zid,
	round(avg(toFloat32OrZero(ExtMap4.Value[2])),2) as avgPCTR,
	round(min(toFloat32OrZero(ExtMap4.Value[2])),2) as minPCTR,
	round(max(toFloat32OrZero(ExtMap4.Value[2])),2) as maxPCTR,
	toDate('{target_day:%Y-%m-%d}') as EventDate
from
	zampda.rtb_all
prewhere
	EventDate >= toDate('{target_day:%Y-%m-%d}') -5
	and  EventDate < toDate('{target_day:%Y-%m-%d}')
	and TotalErrorCode=0
where
	Media_VendorId = 24
	and Bid_CompanyId = 12
	and notEmpty(Impression.Timestamp)
	and  Id_Zid GLOBAL in (
		select distinct Id_Zid
		from zampda.rtb_all
		prewhere
			EventDate >= toDate('{target_day:%Y-%m-%d}') -1
			and  EventDate < toDate('{target_day:%Y-%m-%d}')
			and TotalErrorCode=0
		where
			notEmpty(Impression.Timestamp)
			and Media_VendorId = 24
			and Bid_CompanyId  = 12
	)
	group by Id_Zid
"""


keys=["Id_Zid","EventDate"]

values = [
    "avgPCTR",
    "minPCTR",
    "maxPCTR"
]


d = {}
d['sql'] = sql
d['name'] = "pcpc_pctr"
d['keys'] = keys
d['values'] = values
d['data_date_col'] = 'target_day'
d["output_name"] = "a{account}_v{vendor}_t{target_day:%Y%m%d}"

import json

j = json.dumps(d, indent=4)
print(j)
with open("/Users/admin/PycharmProjects/zamplus_feature_engineer/libs/imp/inst/json/pcpc_pcpr.json", "w",
          encoding='utf-8') as f:
    json.dump(d, f, indent=4)