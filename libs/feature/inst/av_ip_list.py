from datetime import  datetime

pre_sql = []

pre_sql1 = {}

pre_sql2 = {}

feature={}


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
        count(*) as IP_COUNT,
        row_number()
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

sql = """
select 
    RequestId,
    toInt64(Geo_Ip) as rtb_ip,
    
    
from
 zampda.rtb_all

left join 
(
    select 
        Id_Zid,
        groupArray(IP) as IPS
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
                    EventDate >= toDate('{target_day:%Y-%m-%d}') -7 and EventDate <= toDate('{target_day:%Y-%m-%d}') -1   and TotalErrorCode=0
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
) as b
on Id_Zid

prewhere
    EventDate = toDate('{target_day:%Y-%m-%d}')
where 
                        Bid_CompanyId = {account}
                    and Media_VendorId =  {vendor}

limit 10
"""

sql = """
select 
    RequestId,
    Id_Zid,
    toInt64(Geo_Ip) as rtb_ip,
     toInt64(Geo_Ip) = b.IPS ?  1 : 0  as clk_ip
    
from
 zampda.rtb_all
ANY left join
(
    select 
        Id_Zid,
        groupArray(IP)[1] as IPS
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
                    EventDate >= toDate('{target_day:%Y-%m-%d}') -7 and EventDate <= toDate('{target_day:%Y-%m-%d}') -1   and TotalErrorCode=0
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
)  as b
USING Id_Zid
prewhere
EventDate = toDate('{target_day:%Y-%m-%d}')    and TotalErrorCode=0
limit 10
"""


print(sql.format(target_day=datetime.now(),account=12,vendor=24,interval=10))

pre_sql1_sql = """
select 
        Id_Zid,
        groupArray(IP)[1] as IPS
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
                    EventDate >= toDate('{target_day:%Y-%m-%d}') -{interval} and EventDate <= toDate('{target_day:%Y-%m-%d}') -1   and TotalErrorCode=0
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
"""

pre_sql1['sql'] = pre_sql1_sql
pre_sql1['output_template'] = 'a{account}_v{vendor}_t{target_day:%Y%m%d}_clk_ips'
pre_sql1['table_name_template'] = 'a{account}_v{vendor}_clk_ips'
pre_sql1['table_name'] = 'av_clk_ips'

pre_sql.append(pre_sql1)


pre_sql2_sql = """
    select
        RequestId,
        Id_Zid
    from
        zampda.rtb_all
    prewhere
        EventDate = toDate('{target_day:%Y-%m-%d}')  and TotalErrorCode=0
    where
        Bid_CompanyId = {account}
        and Media_VendorId =  {vendor}
        and notEmpty(Impression.Timestamp)
"""
pre_sql2['sql'] = pre_sql2_sql
pre_sql2['output_template'] = 'a{account}_v{vendor}_t{target_day:%Y%m%d}_request'
pre_sql2['table_name_template'] = 'a{account}_v{vendor}_request'
pre_sql2['table_name'] = 'av_request'

pre_sql.append(pre_sql2)

values = ["is_a{account}_v{vendor}_ip_clk"]

sql = """


"""

feature['feature_args'] = {"interval":7}
feature['name'] = "av_clk_ip_last{interval}"
feature['keys'] = ["RequestId"]
feature['values']  = values
feature['pre_sql'] = pre_sql
feature['sql'] = sql
feature['data_date_col'] = "target_day"
feature['output_name'] = "a{account}_v{vendor}_t{target_day:%Y%m%d}_ip_clk"



import  json
j = json.dumps(feature,indent=4)
print(j)

with open("/Users/admin/PycharmProjects/zamplus_feature_engineer/libs/feature/inst/json/av_clk_ip.json", "w", encoding='utf-8') as f:
    json.dump(feature,f,indent=4)