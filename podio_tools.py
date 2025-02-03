"""Auxiliary functions for Podio API"""
import json

from pypodio2.transport import TransportException
from logging_tools import logger


def handling_podio_error(err: TransportException):
    """ Management of Podio API errors.

    Args:
        err (TransportException): Podio transport error exception

    Returns:
        str: A string with the error message
    """
    if 'x-rate-limit-remaining' in err.status and err.status['x-rate-limit-remaining'] == '0':
        logger.warning("Quantidade de requisições chegou ao limite por hora.")
        return "rate_limit"

    if err.status['status'] == '401':
        logger.warning("Token expirado. Renovando...")
        return "token_expired"

    if err.status['status'] == '400':
        if json.loads(err.content)['error_detail'] == 'oauth.client.invalid_secret':
            message = "Secret inválido!"
        elif json.loads(err.content)['error_detail'] == 'user.invalid.username':
            message = "Usuário inválido!"
        elif json.loads(err.content)['error_detail'] == 'oauth.client.invalid_id':
            message = "ID do cliente inválido!"
        elif json.loads(err.content)['error_detail'] == 'user.invalid.password':
            message = "Senha do cliente inválida!"
        else:
            message = f"Parâmetro nulo na query! Detalhes: {err}"
        logger.warning(message)
        return "status_400"

    if err.status['status'] == '504':
        logger.warning("Servidor demorou muito para responder!")
        return "status_504"

    logger.warning("Erro inesperado no acesso a API! Detalhes: %s", str(err))
    return "not_known_yet"


def get_field_text_values(field: dict) -> str:
    """ Get Podio item field values as text.

    Constraints:
        1: Its return is named `values` because it can be multivalued.
        2: All multivalued fields are concatenated with a pipe '|'.
        3: `values` is wrapped by single quotes to insert in SQL query.
        4: All double quotes is replaced by single quotes.

    Args:
        field (dict): Podio item field.

    Returns:
        str: Values as text.
    """
    values = "'"
    if field['type'] == "contact":
        for elem in field['values']:
            values += elem['value']['name'].replace("\'", "") + "|"
        values = values[:-1]
    elif field['type'] == "category":
        values += field['values'][0]['value']['text'].replace("\'", "")
    elif field['type'] == "date" or (field['type'] == "calculation" and 'start' in \
            field['values'][0]):
        values += field['values'][0]['start']
    elif field['type'] == "money":
        values += field['values'][0]['currency'] + " " + field['values'][0]['value']
    elif field['type'] == "image":
        values += field['values'][0]['value']['link']
    elif field['type'] == "embed":
        values += field['values'][0]['embed']['url']
    elif field['type'] == "app":
        for val in field['values']:
            values += val['value']['title'].replace("\'", "") + "|"
        values = values[:-1]
    else:
        values += str(field['values'][0]['value']).replace("\'", "")

    values += "'"
    return values
