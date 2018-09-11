from datetime import datetime


feature = {}


sql = """
select
            Id_Zid,
            {vendor} as Media_VendorId,
            {account} as Bid_CompanyId,
            sum(IMP) as a{account}_v{vendor}_last{interval}_imp,
            sum(CLK) as a{account}_v{vendor}_last{interval}_clk,
            case
                when a{account}_v{vendor}_last{interval}_imp < a{account}_v{vendor}_last{interval}_clk then 1.0
                when a{account}_v{vendor}_last{interval}_imp >= a{account}_v{vendor}_last{interval}_clk then floor(a{account}_v{vendor}_last{interval}_clk/a{account}_v{vendor}_last{interval}_imp*1.0,1)
                else null
            end as a{account}_v{vendor}_last{interval}_ctr,
            toDate('{target_day:%Y-%m-%d}') as EventDate
        from
            (
                select
                    Id_Zid,
                    sum(notEmpty(Click.Timestamp)) as CLK,
                    sum(notEmpty(Impression.Timestamp)) as IMP
                from
                    zampda.rtb_all
                prewhere
                    EventDate >= toDate('{target_day:%Y-%m-%d}')-{interval} and  EventDate <= toDate('{target_day:%Y-%m-%d}') -1 and TotalErrorCode=0
                where
                    Media_VendorId = {vendor}
                    and Bid_CompanyId = {account}
                    and notEmpty(Impression.Timestamp)
                group by Id_Zid
            )
        group by Id_Zid
"""


values = ["a{account}_v{vendor}_last{interval}_imp","a{account}_v{vendor}_last{interval}_clk","a{account}_v{vendor}_last{interval}_ctr"]



feature['feature_args'] = {"interval": 30}
feature['name'] = "av_ctr_day_last{interval}"
feature['keys'] = ["Id_Zid","Media_VendorId","Bid_CompanyId","EventDate"]
feature['values'] = values
feature['sql'] = sql
feature['data_date_col'] = "target_day"
feature['output_name'] = "a{account}_v{vendor}_t{target_day:%Y%m%d}_day_ctr"

import json

j = json.dumps(feature, indent=4)
print(j)

with open("/Users/admin/PycharmProjects/zamplus_feature_engineer/libs/feature/inst/json/av_ctr_day.json", "w",
          encoding='utf-8') as f:
    json.dump(feature, f, indent=4)