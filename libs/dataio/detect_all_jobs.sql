select
  Bid_CompanyId as account,
  Media_VendorId as vendor,
  Device_Os as os,
  has(Segment.Id, 100000 + Bid_CompanyId) as audience,
  count(*) as imp,
  sum(notEmpty(Click.Timestamp)) as clk
from
  rtb_all
prewhere
  EventDate>='{day_begin:%Y-%m-%d}'
  and EventDate<='{day_end:%Y-%m-%d}'
where
  TotalErrorCode = 0 and notEmpty(Impression.Timestamp)
group by
  account,
  vendor,
  os,
  audience
order by
  account,
  vendor,
  os,
  audience
