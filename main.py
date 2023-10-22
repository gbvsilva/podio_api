from os import getenv
import sys
import time

from pypodio2 import api
from pypodio2.transport import TransportException

from get_time import get_hour
from podio_create_tables import create_tables
from podio_insert_items import insert_items
from podio_tools import handling_podio_error

from logging_tools import logger

if __name__ == '__main__':
    # Database update period in seconds
    timeOffset = int(getenv('TIMEOFFSET'))

    # Podio credentials
    client_id = getenv('PODIO_CLIENT_ID')
    client_secret = getenv('PODIO_CLIENT_SECRET')
    username = getenv('PODIO_USERNAME')
    password = getenv('PODIO_PASSWORD')
    # Apps IDs
    apps_ids = list(map(int, getenv('PODIO_APPS_IDS').split(',')))

    MESSAGE = "==== PODIO API PYTHON SCRIPT (PostgreSQL) ===="
    logger.debug(MESSAGE)

    # Podio authentication
    try:
        podio = api.OAuthClient(
            client_id,
            client_secret,
            username,
            password
        )
    # Caso haja erro, provavelmente o token de acesso a API expirou.
    except TransportException as err:
        handled = handling_podio_error(err)
        if handled == 'status_400':
            logger.info("Terminando o programa.")
        exit(1)
    else:

        CYCLE = 1
        while True:
            MESSAGE = f"==== Ciclo {CYCLE} ===="
            logger.info(MESSAGE)
            # send_to_bot(MESSAGE)

            CREATION = create_tables(podio, apps_ids)

            if CREATION == 0:

                INSERTION = insert_items(podio, apps_ids)

                if INSERTION == 1:
                    hour = get_hour(hours=1)
                    MESSAGE = f"Esperando a hora seguinte. Até às {hour}"
                    logger.info(MESSAGE)
                    time.sleep(3600)
                    try:
                        podio = api.OAuthClient(
                            client_id,
                            client_secret,
                            username,
                            password
                        )
                    except:
                        MESSAGE = 'Erro na obtenção do novo cliente Podio! Tentando novamente...'
                        logger.warning(MESSAGE)

                elif INSERTION == 0:
                    hours = get_hour(seconds=timeOffset)
                    MESSAGE = f"Esperando as próximas {timeOffset/3600}hs. Até às {hours}"
                    logger.info(MESSAGE)
                    time.sleep(timeOffset)
                    try:
                        podio = api.OAuthClient(
                            client_id,
                            client_secret,
                            username,
                            password
                        )
                    except:
                        MESSAGE = 'Erro na obtenção do novo cliente Podio! Tentando novamente...'
                        logger.warning(MESSAGE)

                else:
                    MESSAGE = "Tentando novamente..."
                    logger.info(MESSAGE)
                    try:
                        podio = api.OAuthClient(
                            client_id,
                            client_secret,
                            username,
                            password
                        )
                    except:
                        MESSAGE = 'Erro na obtenção do novo cliente Podio! Tentando novamente...'
                        logger.warning(MESSAGE)
                    # time.sleep(1)

            elif CREATION == 1:
                hour = get_hour(hours=1)
                MESSAGE = f"Esperando a hora seguinte às {hour}"
                logger.info(MESSAGE)
                time.sleep(3600)
                try:
                    podio = api.OAuthClient(
                        client_id,
                        client_secret,
                        username,
                        password
                    )
                except:
                    MESSAGE = 'Erro na obtenção do novo cliente Podio! Tentando novamente...'
                    logger.warning(MESSAGE)

            elif CREATION == 2:
                MESSAGE = "Tentando novamente..."
                logger.info(MESSAGE)
                try:
                    podio = api.OAuthClient(
                        client_id,
                        client_secret,
                        username,
                        password
                    )
                except:
                    MESSAGE = 'Erro na obtenção do novo cliente Podio! Tentando novamente...'
                    logger.warning(MESSAGE)
                # time.sleep(1)

            else:
                MESSAGE = "Erro inesperado na criação/atualização do BD. Terminando o programa."
                logger.error(MESSAGE)
                exit(code=1)
