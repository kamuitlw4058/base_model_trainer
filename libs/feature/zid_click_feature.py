
from datetime import datetime



def get_ctr_feature(account, vendor, target_day):
    sql_temp ="""
            (select
                Id_Zid,
                {vendor} as Media_VendorId,
                {account} as Bid_CompanyId,
                sum(IMP) as a{account}_{vendor}_last30_imp,
                sum(CLK) as a{account}_{vendor}_last30_clk,
                case
                    when a{account}_{vendor}_last30_imp < a{account}_{vendor}_last30_clk then 1.0
                    when a{account}_{vendor}_last30_imp >= a{account}_{vendor}_last30_clk then floor(a{account}_{vendor}_last30_clk/a{account}_{vendor}_last30_imp*1.0,1)
                    else null
                end as a{account}_{vendor}_last30_ctr,
                (toDate('{target_day:%Y-%m-%d}')) as EventDate
            from
                (
                    select
                        Id_Zid,
                        sum(length(Click.Timestamp)) as CLK,
                        sum(length(Impression.Timestamp)) as IMP
                    from
                        zampda.rtb_all
                    prewhere
                        EventDate >= toDate('{target_day:%Y-%m-%d}')-30 and TotalErrorCode=0
                    where
                        Media_VendorId = {vendor}
                        and Bid_CompanyId = {account}
                        and notEmpty(Impression.Timestamp)
                    group by Id_Zid
                )
            where Id_Zid in (
                    select Id_Zid
                    from zampda.rtb_all
                    prewhere
                        EventDate >= toDate('{target_day:%Y-%m-%d}')-1 and TotalErrorCode=0
                    where
                        notEmpty(Impression.Timestamp)
                        and Media_VendorId = {vendor}
                        and Bid_CompanyId  = {account}
            )
            group by Id_Zid)

            """

    return sql_temp



def get_raw_sql():
    sql ="""
    (
    select Id_Zid,Media_VendorId,Bid_CompanyId,EventDate from zampda.rtb_all prewhere EventDate = '2018-08-01' and TotalErrorCode=0  where Media_VendorId = 24 and Bid_CompanyId =12   and notEmpty(Impression.Timestamp) limit 30
    )
    """
    return  sql