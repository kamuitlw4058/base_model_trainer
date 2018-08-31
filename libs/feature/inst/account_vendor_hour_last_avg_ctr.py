
from datetime import datetime

sql = """
select 
    Id_Zid,
    Bid_CompanyId,
    Media_VendorId,
    CLK as a{account}_v{vendor}_hour_last{interval}_clk,
    IMP as a{account}_v{vendor}_hour_last{interval}_imp,
    Time_Hour,
    case
            when CLK > IMP then 1.0
            when CLK <= IMP then floor(CLK/IMP*1.0,1)
            else null
    end as a{account}_v{vendor}_hour_last{interval}_ctr,
    toDate('{target_day:%Y-%m-%d}') as EventDate
from
(
    select
        Id_Zid,
        sum(length(Click.Timestamp)) as CLK,
        sum(length(Impression.Timestamp)) as  IMP,
        Bid_CompanyId,
        Media_VendorId,
        Time_Hour    
    from
        zampda.rtb_all
    prewhere
        EventDate >= toDate('{target_day:%Y-%m-%d}') -{interval} 
        and EventDate <= toDate('{target_day:%Y-%m-%d}') -1 
        and TotalErrorCode=0
    where
        Bid_CompanyId = {account}
        and notEmpty(Impression.Timestamp)
        and Media_VendorId =  {vendor}
    group by Id_Zid,Bid_CompanyId,Media_VendorId,Time_Hour
)

"""

values_t = ["a{account}_v{vendor}_hour_last{interval}_imp","a{account}_v{vendor}_hour_last{interval}_clk","a{account}_v{vendor}_hour_last{interval}_ctr"]

values=[]
for v in values_t:
    values.append(v.format(account=12,vendor=24,interval=30,target_day=datetime.now(),hour=18))

print(values)

print(sql.format(account=12,vendor=24,interval=30,target_day=datetime.now(),hour=18))

batch_cond = []
for i in range(24):
    batch_cond.append({"hour":i})

print(str(batch_cond).replace("'",'"'))

d={}
d['sql'] = sql
d['values'] = values_t

import  json
j = json.dumps(d,indent=4)
print(j)