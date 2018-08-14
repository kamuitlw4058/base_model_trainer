#!/usr/bin/env python3
# -*- coding: utf-8 -*-

__author__ = 'mark'
__email__ = 'mark@zamplus.com'


from datetime import date, timedelta
from conf.conf import WINDOW

today = date.today()
day_begin = today - WINDOW
day_end = today

ad_ids = [
    #hotel 12560 12561
    #600100,600099,600064,600063,600028,600027,599984,599983

    #flight 12603
    # 600102,600101,600066,600065,600030,600029,599986,599985,601194,601195,601196,606153,606154,606155,606156,606157,606158

    # hotel
    600100,	600099, 600064,	600063, 600028, 600027,	599984,	599983, 612702, 612703, 612704,
    612705, 612706, 610987, 610988, 610989, 610990, 610991, 610992, 610993, 610994, 610995,
    610996, 610997, 610998, 610999, 611000, 611001, 612486, 612487, 612488, 612489, 612490,
    609343
]
ad_ids = ','.join([f'toLongId(toUInt64({x}))' for x in ad_ids])


jobs = [
    # 785 WeiJingYouLun
    {'meta': {'account': 785, 'vendor': 24, 'os': 'android', 'audience': 'ht'}, 'data': {'use_meta':False,'account':786,'day_begin': today - timedelta(days=90)}, },
    {'meta': {'account': 785, 'vendor': 70, 'os': 'android', 'audience': 'ht'}, 'data': {'use_meta':False,'account':786,'day_begin': today - timedelta(days=90)}, },
    {'meta': {'account': 785, 'vendor': 24, 'os': 'ios', 'audience': 'ht'}, 'data': {'use_meta':False,'account':786,'day_begin': today - timedelta(days=90)}, },
    {'meta': {'account': 785, 'vendor': 70, 'os': 'ios', 'audience': 'ht'}, 'data': {'use_meta':False,'account':786,'day_begin': today - timedelta(days=90)}, },

    # 9 XieCheng
    {'meta': {'account': 9, 'vendor': 21, 'os': 'android', 'audience': 'ht'}, 'data': {'filters': ['Bid_CampaignId in (toLongId(toUInt64(12333)),toLongId(toUInt64(12281)),toLongId(toUInt64(12103)),toLongId(toUInt64(12560)),toLongId(toUInt64(12561)),toLongId(toUInt64(12606)))'] }, },
    {'meta': {'account': 9, 'vendor': 75, 'os': 'android', 'audience': 'ht'}, 'data': {'filters': ['Bid_CampaignId in (toLongId(toUInt64(12560)),toLongId(toUInt64(12561)),toLongId(toUInt64(12606)))'] }, },
    {'meta': {'account': 9, 'vendor': 81, 'os': 'android', 'audience': 'ht'}, 'data': {'filters': ['Bid_CampaignId in (toLongId(toUInt64(12333)),toLongId(toUInt64(12281)),toLongId(toUInt64(12103)),toLongId(toUInt64(12560)),toLongId(toUInt64(12561)),toLongId(toUInt64(12606)))'] }, },
    {'meta': {'account': 9, 'vendor': 82, 'os': 'android', 'audience': 'ht'}, 'data': {'filters': ['Bid_CampaignId in (toLongId(toUInt64(12333)),toLongId(toUInt64(12281)),toLongId(toUInt64(12103)),toLongId(toUInt64(12560)),toLongId(toUInt64(12561)),toLongId(toUInt64(12606)))'] }, },

    {'meta': {'account': 9, 'vendor': 21, 'os': 'ios', 'audience': 'ht'}, 'data': {'filters': ['Bid_CampaignId in (toLongId(toUInt64(12333)),toLongId(toUInt64(12281)),toLongId(toUInt64(12103)),toLongId(toUInt64(12560)),toLongId(toUInt64(12561)),toLongId(toUInt64(12606)))'] }, },
    {'meta': {'account': 9, 'vendor': 75, 'os': 'ios', 'audience': 'ht'}, 'data': {'filters': ['Bid_CampaignId in (toLongId(toUInt64(12560)),toLongId(toUInt64(12561)),toLongId(toUInt64(12606)))'] }, },
    {'meta': {'account': 9, 'vendor': 81, 'os': 'ios', 'audience': 'ht'}, 'data': {'filters': ['Bid_CampaignId in (toLongId(toUInt64(12333)),toLongId(toUInt64(12281)),toLongId(toUInt64(12103)),toLongId(toUInt64(12560)),toLongId(toUInt64(12561)),toLongId(toUInt64(12606)))'] }, },
    {'meta': {'account': 9, 'vendor': 82, 'os': 'ios', 'audience': 'ht'}, 'data': {'filters': ['Bid_CampaignId in (toLongId(toUInt64(12333)),toLongId(toUInt64(12281)),toLongId(toUInt64(12103)),toLongId(toUInt64(12560)),toLongId(toUInt64(12561)),toLongId(toUInt64(12606)))'] }, },

    # xie cheng 2018-06-20 17:41
    {'meta': {'account': 9, 'vendor': 24, 'os': 'android', 'audience': 'ht'}, 'data': {'filters': [f'Bid_CampaignId in ({", ".join([f"toLongId(toUInt64({i}))" for i in [12602, 12700, 12848, 12850, 12018]])})']}},
    {'meta': {'account': 9, 'vendor': 24, 'os': 'ios', 'audience': 'ht'}, 'data': {'filters': [f'Bid_CampaignId in ({", ".join([f"toLongId(toUInt64({i}))" for i in [12602, 12700, 12848, 12850, 12018]])})']}},

    # add du jia 2018-05-25 14:30
    {'meta': {'account': 9, 'vendor': 78, 'os': 'ios', 'audience': 'ht'}, 'data': {'filters': [f'( Bid_AdId in ({ad_ids}) or Bid_CampaignId in (toLongId(toUInt64(12602))) )' ] }, },

    # yiche 2018-06-26
    {'meta': {'account': 12, 'vendor': 24, 'os': 'ios', 'audience': 'ht', 'id': '1999_24_2_1'}, 'data': {'filters': ['Bid_CampaignId=toLongId(toUInt64(12902))'] } },
    {'meta': {'account': 12, 'vendor': 24, 'os': 'android', 'audience': 'ht', 'id': '1999_24_1_1'}, 'data': {'filters': ['Bid_CampaignId=toLongId(toUInt64(12902))']}},

    {'meta': {'account': 12, 'vendor': 75, 'os': 'ios', 'audience': 'ht', 'id': '1999_75_2_1'}, 'data': {'filters': ['Bid_CampaignId=toLongId(toUInt64(12901))']}},
    {'meta': {'account': 12, 'vendor': 75, 'os': 'android', 'audience': 'ht', 'id': '1999_75_1_1'}, 'data': {'filters': ['Bid_CampaignId=toLongId(toUInt64(12901))']}},

    # 20 SuNing
    {'meta': {'account': 20, 'vendor': 78, 'os': 'android', 'audience': 'rmkt'}, 'data': {'day_begin': date.today()-timedelta(5) }, },

    # 604 YiHaiZuChe update 2018-05-28 11:50
    {'meta': {'account': 604, 'vendor': 70, 'os': 'android', 'audience': 'ht'}, 'data': {'day_begin': today - timedelta(days=30)}, },
    {'meta': {'account': 604, 'vendor': 70, 'os': 'ios', 'audience': 'ht'}, 'data': {'day_begin': today - timedelta(days=30)}, },

    {'meta': {'account': 604, 'vendor': 24, 'os': 'android', 'audience': 'ht'}, 'data': {'day_begin': today - timedelta(days=30)}, },
    {'meta': {'account': 604, 'vendor': 24, 'os': 'ios', 'audience': 'ht'}, 'data': {'day_begin': today - timedelta(days=30)}, },

    {'meta': {'account': 604, 'vendor': 75, 'os': 'android', 'audience': 'ht'}, 'data': {'day_begin': today - timedelta(days=30)}, },
    {'meta': {'account': 604, 'vendor': 75, 'os': 'ios', 'audience': 'ht'}, 'data': {'day_begin': today - timedelta(days=30)}, },

    # 12 YiChe on vendor 70 75
    # {'meta': {'account': 12,  'vendor': 70, 'os': 'android', 'audience': 'ht', 'day_begin': today - timedelta(days=14) }, },
    # {'meta': {'account': 12,  'vendor': 70, 'os': 'ios',     'audience': 'ht', 'day_begin': today - timedelta(days=14) }, },
    # {'meta': {'account': 12,  'vendor': 70, 'os': 'android', 'audience': 'rmkt', 'day_begin': today - timedelta(days=14) }, },
    # {'meta': {'account': 12,  'vendor': 70, 'os': 'ios',     'audience': 'rmkt', 'day_begin': today - timedelta(days=14) }, },
    {'meta': {'account': 12, 'vendor': 75, 'os': 'android', 'audience': 'ht'},   'data': {'day_begin': today - timedelta(days=21)}},
    {'meta': {'account': 12, 'vendor': 75, 'os': 'ios',     'audience': 'ht'},   'data': {'day_begin': today - timedelta(days=21)}},
    {'meta': {'account': 12, 'vendor': 75, 'os': 'android', 'audience': 'rmkt'}, 'data': {'day_begin': today - timedelta(days=21)}},
    {'meta': {'account': 12, 'vendor': 75, 'os': 'ios',     'audience': 'rmkt'}, 'data': {'day_begin': today - timedelta(days=21)}},
    {'meta': {'account': 12, 'vendor': 21, 'os': 'android', 'audience': 'ht'}},
    {'meta': {'account': 12, 'vendor': 21, 'os': 'ios',     'audience': 'ht'},   'data': {'account': 12, 'vendor': 76}},

    # vendor default model
    {'meta': {'vendor': 21, 'os': 'android'}, },
    {'meta': {'vendor': 21, 'os': 'ios'}, },

    {'meta': {'vendor': 24, 'os': 'android'}, },
    {'meta': {'vendor': 24, 'os': 'ios'}, },

    {'meta': {'vendor': 70, 'os': 'android'}, },
    {'meta': {'vendor': 70, 'os': 'ios'}, },

    {'meta': {'vendor': 75, 'os': 'android'}, },
    {'meta': {'vendor': 75, 'os': 'ios'}, },

    {'meta': {'vendor': 76, 'os': 'android'}, },
    {'meta': {'vendor': 76, 'os': 'ios'}, },

    {'meta': {'vendor': 77, 'os': 'android'}, },
    {'meta': {'vendor': 77, 'os': 'ios'}, },

    {'meta': {'vendor': 78, 'os': 'android'}, },
    {'meta': {'vendor': 78, 'os': 'ios'}, },

    {'meta': {'vendor': 80, 'os': 'android'}, },
    {'meta': {'vendor': 80, 'os': 'ios'}, },

    {'meta': {'vendor': 81, 'os': 'android'}, },
    {'meta': {'vendor': 81, 'os': 'ios'}, },

    {'meta': {'vendor': 82, 'os': 'android'}, },
    {'meta': {'vendor': 82, 'os': 'ios'}, },

    {'meta': {'vendor': 83, 'os': 'android'}, },
    {'meta': {'vendor': 83, 'os': 'ios'}, },

    {'meta': {'vendor': 85, 'os': 'android'}, },
    {'meta': {'vendor': 85, 'os': 'ios'}, },
]

#jobs = [
#     {'meta': {'account': 9, 'vendor': 24, 'os': 'android', 'audience': 'ht'},
#      'data': {'filters': [f'Bid_CampaignId in ({", ".join([f"toLongId(toUInt64({i}))" for i in [12602, 12700, 12848, 12850, 12018]])})']}
#      },
#     {'meta': {'account': 9, 'vendor': 24, 'os': 'ios', 'audience': 'ht'},
#      'data': {'filters': [
#          f'Bid_CampaignId in ({", ".join([f"toLongId(toUInt64({i}))" for i in [12602, 12700, 12848, 12850, 12018]])})']}
#      },
#         {'meta': {'account': 12, 'vendor': 75, 'os': 'android', 'audience': 'ht'}},
#     {'meta': {'account': 12, 'vendor': 24, 'os': 'android', 'audience': 'rmkt'}},
#     {'meta': {'account': 71, 'vendor': 85, 'os': 'android', 'audience': 'rmkt'}},
#{'meta': {'account': 785, 'vendor': 24, 'os': 'android', 'audience': 'ht'}, 'data': {'use_meta':False,'account':786,'day_begin': today - timedelta(days=90)}, },
#{'meta': {'account': 785, 'vendor': 70, 'os': 'android', 'audience': 'ht'}, 'data': {'use_meta':False,'account':786,'day_begin': today - timedelta(days=90)}, },
#{'meta': {'account': 785, 'vendor': 24, 'os': 'ios', 'audience': 'ht'}, 'data': {'use_meta':False,'account':786,'day_begin': today - timedelta(days=90)}, },
#{'meta': {'account': 785, 'vendor': 70, 'os': 'ios', 'audience': 'ht'}, 'data': {'use_meta':False,'account':786,'day_begin': today - timedelta(days=90)}, },
# ]

