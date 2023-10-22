import datetime

def get_hour(**timedelta):
    if timedelta:
        hours = timedelta.get('hours', 0)
        minutes = timedelta.get('minutes', 0)
        seconds = timedelta.get('seconds', 0)
        microseconds = timedelta.get('microseconds', 0)
        offset = datetime.timedelta(hours=hours, minutes=minutes, seconds=seconds, microseconds=microseconds)

        return (datetime.datetime.now() + offset).strftime('%H:%M:%S')

    return datetime.datetime.now().strftime('%H:%M:%S')
