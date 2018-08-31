from datetime import datetime

test_sql ="""

select * from 
(
select 
    Id_Zid,
    Media_VendorId,
    count(*) as v{vendor}_clk
from 
    zampda.rtb_all
prewhere
    EventDate >= toDate('{target_day:%Y-%m-%d}') - 30 and   EventDate <= toDate('{target_day:%Y-%m-%d}') -1
where 
    Media_VendorId = {vendor}
    and notEmpty(Click.Timestamp)
group by
    Id_Zid,Media_VendorId
)
where 
    v{vendor}_clk > 10
order by   
    v{vendor}_clk desc

"""


test1_sql = """
select * from
(
    select 
            Id_Zid,
            count(*) as clk_day
    from
    ( 
        select 
            Id_Zid,
            EventDate,
            count(*) as a{account}_last{interval}_clk
        from 
            zampda.rtb_all
        prewhere
            EventDate >= toDate('{target_day:%Y-%m-%d}') - {interval} and   EventDate <= toDate('{target_day:%Y-%m-%d}') -1
        where 
            Bid_CompanyId = {account}
            and notEmpty(Click.Timestamp)
        group by
            Id_Zid,EventDate
        
    )
    group by 
        Id_Zid
)
where 
    clk_day > 2
limit 10
"""


#print(test1_sql.format(target_day=datetime.now(),account=12,interval=30))


sql ="""
select 
        Id_Zid,
        a{account}_last{interval}_clk,
        last{interval}_clk,
        floor(a{account}_last{interval}_clk/last{interval}_clk*1.0,1) as a{account}_last{interval}_clk_hold,
        toDate('{target_day:%Y-%m-%d}') as EventDate
from
( 
    select 
        Id_Zid,
        a{account}_last{interval}_clk,
        last{interval}_clk
    from 
    (
    select 
        Id_Zid,
        count(*) as last{interval}_clk
    from 
        zampda.rtb_all
    prewhere
        EventDate >= toDate('{target_day:%Y-%m-%d}') - {interval} and   EventDate <= toDate('{target_day:%Y-%m-%d}') -1
    where 
        notEmpty(Click.Timestamp)
    group by
        Id_Zid
    ) ANY left join 
    (
     select 
        Id_Zid,
        count(*) as a{account}_last{interval}_clk
    from 
        zampda.rtb_all
    prewhere
        EventDate >= toDate('{target_day:%Y-%m-%d}') - {interval} and   EventDate <= toDate('{target_day:%Y-%m-%d}') -1
    where 
        Bid_CompanyId = {account}
        and notEmpty(Click.Timestamp)
    group by
        Id_Zid,Bid_CompanyId
    ) USING Id_Zid
)

"""

"""
1233213
"""


test_sql ="""
select 
        Id_Zid,
        a{account}_last{interval}_clk,
        last{interval}_clk,
        case
            when isNaN(a{account}_last{interval}_clk) then null
            else floor(a{account}_last{interval}_clk/last{interval}_clk*1.0,1)
        end as a{account}_last{interval}_clk_hold,
        toDate('{target_day:%Y-%m-%d}') as EventDate
from
( 
    select 
        Id_Zid,
        a{account}_last{interval}_clk,
        last{interval}_clk
    from 
    (
    select 
        Id_Zid,
        count(*) as last{interval}_clk
    from 
        zampda.rtb_all
    prewhere
        EventDate >= toDate('{target_day:%Y-%m-%d}') - {interval} and   EventDate <= toDate('{target_day:%Y-%m-%d}') -1
    where 
        notEmpty(Click.Timestamp)
    group by
        Id_Zid
    ) ANY left join 
    (
     select 
        Id_Zid,
        count(*) as a{account}_last{interval}_clk
    from 
        zampda.rtb_all
    prewhere
        EventDate >= toDate('{target_day:%Y-%m-%d}') - {interval} and   EventDate <= toDate('{target_day:%Y-%m-%d}') -1
    where 
        Bid_CompanyId = {account}
        and notEmpty(Click.Timestamp)
    group by
        Id_Zid,Bid_CompanyId
    ) USING Id_Zid
)

"""

#print( test_sql.format(target_day=datetime.now(),account=12,interval=30))


d={}
d['sql'] = sql

import  json
j = json.dumps(d)
print(j)