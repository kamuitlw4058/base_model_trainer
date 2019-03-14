import time
from datetime import datetime, timedelta



def get_utc_sec(dt=datetime.now()):
    dt.timetuple()
    secondsFrom1970 = time.mktime(dt.timetuple())
    return int(secondsFrom1970)

def get_hour_sec(hour):
    return  hour * 60 * 60


def get_datetime_by_utc_sec(secs):
    timeArray = time.localtime(secs)  # 1970秒数
    otherStyleTime = time.strftime("%Y-%m-%d %H:%M:%S", timeArray)
    return datetime.strptime(otherStyleTime, "%Y-%m-%d %H:%M:%S")


def get_human_timestamp(dt=datetime.now()):
    return datetime.strftime(dt, "%Y%m%d%H%M%S")


def get_datetime_diff(diff,unit='day',dt=datetime.now()):
    if unit == 'day':
        return dt + timedelta(days=diff)
    elif unit == 'hour':
        return dt + timedelta(hours=diff)
    elif unit == 'min':
        return dt + timedelta(minutes=diff)
    elif unit == 'sec':
        return dt + timedelta(seconds=diff)
    elif unit == 'msec':
        return dt + timedelta(microseconds=diff)


#print(get_utc_sec())
#print(get_utc_sec() - get_hour_sec(1))

#print(get_datetime(1544175179))