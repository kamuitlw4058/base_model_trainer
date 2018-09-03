from datetime import  datetime


sql="""
select 
    Id_Zid,
    IP,
    topK(3)(IP_COUNT) as top3
from
(
    select 
        Id_Zid,
        IP,
        count(*) as IP_COUNT
    from 
    (
    
            select
                Id_Zid,
                IP
            from
                zampda.rtb_all
            ARRAY JOIN Click.Ip AS IP
            prewhere
                EventDate =toDate('{target_day:%Y-%m-%d}') and TotalErrorCode=0
            where
                Bid_CompanyId = {account}
                and Media_VendorId =  {vendor}
                and notEmpty(Click.Timestamp)
    )
    group by
                Id_Zid,
                IP
)
group by
    Id_Zid



limit 10
"""

print(sql.format(target_day=datetime.now(),account=12,vendor=24,interval=10))