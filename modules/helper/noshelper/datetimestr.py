# -*- coding: utf-8 -*-

from datetime import datetime, timedelta
import pytz

dateFormat     = '%Y-%m-%d'
datetimeFormat = '%Y-%m-%d %H:%M:%S'
 
dayStep         = timedelta(days=1)
dayStartOffset  = timedelta(hours=4)
secondDelta     = timedelta(seconds=1)


def date2str(dt):
    return dt.strftime(dateFormat)


def datetime2str(dt):
	return dt.strftime(datetimeFormat)


def str2datetime(dtStr):
	return datetime.strptime(dtStr, datetimeFormat)


def str2date(dtStr):
    return datetime.strptime(dtStr, dateFormat)


def add_timedelta_to_datestr(dtStr, timedelta):
	dt = str2datetime(dtStr)
	dt+= timedelta
	return datetime2str(dt)


def datetimestr_diff(dtStr1, dtStr2):
    """calculates difference in seconds between two dates represented as strings"""
    dt1 = datetime.strptime(dtStr1, datetimeFormat)
    dt2 = datetime.strptime(dtStr2, datetimeFormat)
    return int( (dt1 - dt2).total_seconds() )


def datetimestr2daystr(dt): return dt[:10]


def get_next_day(dayStr):
    """@param string dayStr day in %Y-%m-%d
       @return string next day in %Y-%m-%d"""
    dt = str2date(dayStr)
    dt = dt + dayStep
    return date2str(dt)


def get_prev_day(dayStr, daysToGoBack=1):
    """Goes back 1 day (default) or the number of days passed in the optional parameter
        @param string dayStr day in %Y-%m-%d
        @return string next day in %Y-%m-%d"""
    dt = str2date(dayStr)
    dt = dt - dayStep * daysToGoBack
    return date2str(dt)


def get_start_of_day(dayStr): 
    """@param string dayStr day in %Y-%m-%d
       @return string day start in %Y-%m-%d %H:%M:%S"""
    dt = str2date(dayStr)
    dt = dt + dayStartOffset
    return datetime2str(dt)


def get_end_of_day(dayStr): 
    """@param string dayStr day in %Y-%m-%d
       @return string day start in %Y-%m-%d %H:%M:%S"""
    dt = str2date(dayStr)
    dt = dt + dayStep + dayStartOffset - secondDelta
    return datetime2str(dt)


def get_start_of_current_day():
    now = datetime.now()
    nowStr = datetime2str(now)
    nowDayStr = nowStr[0:10]
    dayStartStr = get_start_of_day(nowDayStr)
    if (dayStartStr < nowStr): return str2datetime(dayStartStr)

    now = now - dayStep
    dayStartStr = get_start_of_day(date2str(now))
    return str2datetime(dayStartStr)


def get_next_day_start(dateStr):
    dayStr = dateStr[0:10]
    
    if int(dateStr[11:13]) < 4: 
        return get_start_of_day(dayStr)
    else: 
        return get_start_of_day(get_next_day(dayStr))


def get_prev_day_end(dateStr):
    dayStr = dateStr[0:10]
    
    if int(dateStr[11:13]) >= 4: 
        daysToGoBack = 1
    else: 
        daysToGoBack = 2

    return get_end_of_day(get_prev_day(dayStr, daysToGoBack))


#TOTEST
def get_timezone_offset(startDateStr):
    lisbonTimezone = pytz.timezone('Europe/Lisbon')
    return lisbonTimezone.utcoffset(str2datetime(startDateStr)) #returns timedelta

