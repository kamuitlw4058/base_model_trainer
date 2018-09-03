

import json
from datetime import  datetime

with open("/Users/admin/PycharmProjects/zamplus_feature_engineer/libs/feature/inst/json/test.json", "r", encoding='utf-8') as f:
    j =json.load(f)
    #print(j["sql"])

"""
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



sql="""
select 
    Id_Zid,
    {account} as Bid_CompanyId,
    {vendor} as Media_VendorId,
    if(TargetMinute == 143, toDate('{target_day:%Y-%m-%d}') +1,toDate('{target_day:%Y-%m-%d}')) as EventDate,
    if(TargetMinute == 143, 143, TargetMinute +1) as Time_Minute_10,
    CLK as    a{account}_v{vendor}_interval_min{interval}_clk,
    IMP as    a{account}_v{vendor}_interval_min{interval}_imp,
    case
            when IMP < CLK then 1.0
            when IMP >= CLK then floor(CLK/IMP*1.0,1)
            else null
    end a{account}_v{vendor}_interval_min{interval}_ctr
from
(
    select 
        Id_Zid,
        TargetMinute,
        sum(length(CLKS)) as CLK,
        sum(length(IMPS)) as  IMP
    from
    (
        select
            Id_Zid,
            EventDate,
           Time_Hour, 
           cast(Time_Timestamp as DateTime) as ts,
            toMinute(ts ) as Time_Minute,
            Time_Hour * 60 + Time_Minute as Date_Minute,
            floor(Date_Minute / {interval}) as TargetMinute,
            Click.Timestamp as CLKS,
            Impression.Timestamp IMPS
        from
            zampda.rtb_all
        prewhere
            EventDate =toDate('{target_day:%Y-%m-%d}') and TotalErrorCode=0
        where
            Bid_CompanyId = {account}
            and Media_VendorId =  {vendor}
            and notEmpty(Impression.Timestamp)
    )
    group by Id_Zid,TargetMinute
)
"""

#print(sql.format(target_day=datetime.now(),account=12,vendor=24,interval=10))

values_t = ["a{account}_v{vendor}_interval_min{interval}_clk","a{account}_v{vendor}_interval_min{interval}_imp","a{account}_v{vendor}_interval_min{interval}_ctr"]

d={}
d['sql'] = sql
d['values'] = values_t

import  json
j = json.dumps(d,indent=4)
print(j)