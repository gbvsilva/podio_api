from os import getenv

import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

from get_time import get_hour

from logging_tools import logger

def get_db():
    try:
        mydb = psycopg2.connect(
            host=getenv('POSTGRES_HOST'),
            user=getenv('POSTGRES_USERNAME'),
            password=getenv('POSTGRES_PASSWORD'),
            dbname=getenv('POSTGRES_DATABASE'),
            port=getenv('POSTGRES_PORT')
        )
        mydb.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    except psycopg2.Error as err:
        # Inatividade do banco ou credenciais inválidas
        message = f"Erro inesperado no acesso inicial ao BD. Terminando o programa. Detalhes: {str(err)}"
        logger.error(message)
        exit(1)
    else:
        return mydb
