#!/usr/bin/env python3
# -*- coding: utf-8 -*-

__author__ = "mark"
__email__ = "mark@zamplus.com"


user_feature = [
    'Device_OsVersion',
    'Device_Network',
    # 'Device_Brand',
    # 'Device_Model',
    'Age',
    'Gender',
    'Education',
    'Profession',
    'ConsumptionLevel',
    'Adb_Device_Platform',
    'Adb_Device_Type',
    'Adb_Device_Brand',
    'Adb_Device_Model',
    'Adb_Device_PriceLevel',
    # 'Work_Province',
    # 'Work_City',
    # 'Work_Area',
    # 'Work_District',
    # 'Work_Category',
    # 'Home_Province',
    # 'Home_City',
    # 'Home_Area',
    # 'Home_District',
    # 'Home_Category',
]

context_feature = [
    'Time_Hour',
    # 'Time_Minute',
    'weekday',
    'Slot_Id',
    'Slot_Type',
    'Media_Domain',
    'geo_county',
    'geo_city',
    'geo_province'
]

user_cap_feature = [
    'cap_PayAction',
    'cap_House',
    'cap_Car',
    'cap_CPI',
    # 'cap_EduInd',
    # 'cap_HouseInd',
    # 'cap_CarInd',
    # 'cap_TravelInd'
]


def get_bidding_feature(account, vendor):
    return [
        f'{account}_{vendor}_last30_imp',
        f'{account}_{vendor}_last30_clk',
        f'{account}_{vendor}_last30_ctr'
    ]


def get_raw_columns():
    cols = [
        'notEmpty(Click.Timestamp) as is_clk',
        'notEmpty(Impression.Timestamp) as is_win',
        'cast(Time_Timestamp as DateTime) as ts',
        # 'toString(toDayOfWeek(ts)) as Time_Weekday',
        'toString(Time_Hour) as Time_Hour',
        # 'toString(toMinute(ts)) as Time_Minute',
        'modulo(Geo_Code, 1000000) as geo_code',
        'toString(geo_code) as geo_county',
        'toString(intDiv(geo_code, 100) * 100) as geo_city',
        'toString(intDiv(geo_code, 10000) * 10000) as geo_province',
        'lower(Slot_Id) as Slot_Id',
        'Slot_Type',
'''lower(if(Media_Domain like 'app_bundle_id_47%',
    case Device_Os
		when 'ios' then 'com.tencent.info'
		else 'com.tencent.news'
	end,
	Media_Domain)) as Media_Domain''',
        'lower(Device_OsVersion) as Device_OsVersion',
        'toString(Device_Network) as Device_Network',
        'lower(Device_Brand) as Device_Brand',
        'lower(Device_Model) as Device_Model',
        'lower(Age) as Age',
        'lower(Gender) as Gender',
        'lower(Education) as Education',
        'lower(Profession) as Profession',
        'lower(ConsumptionLevel) as ConsumptionLevel',
        'lower(Adb_Device_Platform) as Adb_Device_Platform',
        'lower(Adb_Device_Type) as Adb_Device_Type',
        'lower(Adb_Device_Brand) as Adb_Device_Brand',
        'lower(Adb_Device_Model) as Adb_Device_Model',
        'lower(Adb_Device_PriceLevel) as Adb_Device_PriceLevel',
        # 'lower(Work_Province) as Work_Province',
        # 'lower(Work_City) as Work_City',
        # 'lower(Work_Area) as Work_Area',
        # 'lower(Work_District) as Work_District',
        # 'lower(Work_Category) as Work_Category',
        # 'lower(Home_Province) as Home_Province',
        # 'lower(Home_City) as Home_City',
        # 'lower(Home_Area) as Home_Area',
        # 'lower(Home_District) as Home_District',
        # 'lower(Home_Category) as Home_Category',
        "lower(arrayStringConcat(AppCategory.Key, ',')) as AppCategory",
        "arrayStringConcat(arrayMap(x -> toString(x), Segment.Id), ',') as segment",
        "arrayStringConcat(Ext.Key, ',') as ext_key",
        "lower(arrayStringConcat(Ext.Value, ',')) as ext_value"
    ]
    return [i for i in cols]

