import sys
from os import getenv

import mysql.connector

from get_time import get_hour

from logging_tools import logger

def get_db():
    try:
        mydb = mysql.connector.connect(
            host=getenv('MYSQL_HOST'),
            user=getenv('MYSQL_USER'),
            password=getenv('MYSQL_PASSWORD'),
            port=getenv('MYSQL_PORT')
        )
    except mysql.connector.Error as err:
        # Inatividade do banco ou credenciais inválidas
        message = f"Erro inesperado no acesso inicial ao BD. Terminando o programa. {err}"
        logger.error(message)
        sys.exit()
    else:
        return mydb
