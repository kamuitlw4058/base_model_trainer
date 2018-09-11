from datetime import datetime

sql = """
select 
    Id_Zid,
    Bid_CompanyId,
    Media_VendorId,
    a{account}_v{vendor}_hour_last{interval}_clk,
    a{account}_v{vendor}_hour_last{interval}_imp,
    EventDate,
    Time_Hour,
    case
            when a{account}_v{vendor}_hour_last{interval}_imp < a{account}_v{vendor}_hour_last{interval}_clk then 1.0
            when a{account}_v{vendor}_hour_last{interval}_imp >= a{account}_v{vendor}_hour_last{interval}_clk then floor(a{account}_v{vendor}_hour_last{interval}_clk/a{account}_v{vendor}_hour_last{interval}_imp*1.0,1)
            else null
    end as a{account}_v{vendor}_hour_last{interval}_ctr
from
(
    select
        Id_Zid,
        sum(length(Click.Timestamp)) as a{account}_v{vendor}_hour_last{interval}_clk,
        sum(length(Impression.Timestamp)) as  a{account}_v{vendor}_hour_last{interval}_imp,
        if(Time_Hour == 23, toDate('{target_day:%Y-%m-%d}') +1,toDate('{target_day:%Y-%m-%d}')) as EventDate,
        if(Time_Hour == 23, 0, Time_Hour+1) as Time_Hour,
        Bid_CompanyId,
        Media_VendorId    
    from
        zampda.rtb_all
    prewhere
        EventDate =toDate('{target_day:%Y-%m-%d}') and TotalErrorCode=0
    where
        Bid_CompanyId = {account}
        and notEmpty(Impression.Timestamp)
        and Media_VendorId =  {vendor}
    group by Id_Zid,Bid_CompanyId,Media_VendorId,EventDate,Time_Hour
)

"""

values =["a{account}_v{vendor}_hour_last{interval}_clk","a{account}_v{vendor}_hour_last{interval}_imp","a{account}_v{vendor}_hour_last{interval}_ctr"]


feature={}


feature['feature_args'] = {"interval":1}
feature['name'] = "av_hour_ctr_last{interval}"
feature['keys'] = ["Id_Zid","Bid_CompanyId","Media_VendorId","EventDate","Time_Hour"]
feature['values']  = values
feature['sql'] = sql
feature['data_date_col'] = "target_day"
feature['output_name'] = "a{account}_v{vendor}_t{target_day:%Y%m%d}_hour_ctr"



import  json
j = json.dumps(feature,indent=4)
print(j)

with open("/Users/admin/PycharmProjects/zamplus_feature_engineer/libs/feature/inst/json/av_hour_ctr.json", "w", encoding='utf-8') as f:
    json.dump(feature,f,indent=4)
