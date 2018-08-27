
"""
select count(*) 
from 
(
select
    Id_Zid,
    sum(length(Click.Timestamp)) as a12_v24_hour_last1_clk,
    sum(length(Impression.Timestamp)) as  a12_v24_hour_last1_imp,
    if(Time_Hour == 0, toDate('2018-08-20') -1,toDate('2018-08-20')) as EventDate,
    if(Time_Hour == 0, 23, Time_Hour-1) as Time_Hour,
    Bid_CompanyId,
    Media_VendorId    
from
    zampda.rtb_all
prewhere
    EventDate =toDate('2018-08-20') and TotalErrorCode=0
where
    Bid_CompanyId = 12
    and notEmpty(Impression.Timestamp)
    and Media_VendorId =  24
group by Id_Zid,Bid_CompanyId,Media_VendorId,EventDate,Time_Hour

)
where a12_v24_hour_last1_clk > 1

"""

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
        if(Time_Hour == 0, toDate('{target_day:%Y-%m-%d}') -1,toDate('{target_day:%Y-%m-%d}')) as EventDate,
        if(Time_Hour == 0, 23, Time_Hour-1) as Time_Hour,
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


d={}
d['sql'] = sql

import  json
j = json.dumps(d)
print(j)

l = []

for i in range(24):
    l.append({'hour':i})


j = json.dumps(l)
print(j)

