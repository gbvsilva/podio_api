""" Módulo de funções auxiliares de tempo.
"""

from datetime import datetime, timedelta

def get_hour(**delta):
    """ Função que retorna a hora atual com um offset de tempo.

    Returns:
        str: Hora atual com offset de tempo.
    """
    if delta:
        hours = delta.get('hours', 0)
        minutes = delta.get('minutes', 0)
        seconds = delta.get('seconds', 0)
        microseconds = delta.get('microseconds', 0)
        offset = timedelta(hours=hours, minutes=minutes, seconds=seconds, microseconds=microseconds)

        return (datetime.now() + offset).strftime('%H:%M:%S')

    return datetime.now().strftime('%H:%M:%S')
