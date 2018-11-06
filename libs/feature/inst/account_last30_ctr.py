


pre_sql ="""
select
    Id_Zid,
    sum(length(Click.Timestamp)) as CLK,
    sum(length(Impression.Timestamp)) as IMP,
    EventDate as TargetDate,
    Bid_CompanyId
    
from
    zampda.rtb_all
prewhere
    EventDate = toDate('{target_day:%Y-%m-%d}') and TotalErrorCode=0
where
    Bid_CompanyId = {account}
    and notEmpty(Impression.Timestamp)
group by Id_Zid,Bid_CompanyId,EventDate
"""

from datetime import datetime
print(pre_sql.format(target_day=datetime.now(),account=12,vendor=24))


sql = """
select 
    Id_Zid,
    Bid_CompanyId,
     a{account}_last{interval}_imp,
     a{account}_last{interval}_clk,
     case
            when a{account}_last{interval}_imp < a{account}_last{interval}_clk then 1.0
            when a{account}_last{interval}_imp >= a{account}_last{interval}_clk then format_number(a{account}_last{interval}_clk/a{account}_last{interval}_imp*1.0,1)
            else null
     end as a{account}_last{interval}_ctr
from 
(
    select
        Id_Zid,
        sum(IMP) as a{account}_last{interval}_imp,
        sum(CLK) as a{account}_last{interval}_clk,
        toDate('{target_day:%Y-%m-%d}') as EventDate,
        Bid_CompanyId
    from
        {temp_table}
    where 
        TargetDate >= date_sub(to_date('{target_day:%Y-%m-%d}'),30) and  TargetDate <= date_sub(to_date('{target_day:%Y-%m-%d}'),1)
    group by Id_Zid,Bid_CompanyId
)
"""

d={}
d['pre_sql'] = pre_sql
d['sql'] = sql

import  json
j = json.dumps(d)
print(j)