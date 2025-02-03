""" Auxiliary functions to deal with time.
"""

from datetime import datetime, timedelta

def get_hour(**delta):
    """ Apply a timedelta to a datetime.

    Returns:
        str: DateTime with the delta applied.
    """
    if delta:
        hours = delta.get('hours', 0)
        minutes = delta.get('minutes', 0)
        seconds = delta.get('seconds', 0)
        microseconds = delta.get('microseconds', 0)
        offset = timedelta(hours=hours, minutes=minutes, seconds=seconds, microseconds=microseconds)

        return (datetime.now() + offset).strftime('%H:%M:%S')

    return datetime.now().strftime('%H:%M:%S')
