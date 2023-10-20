import json
from get_time import getHour

from logging_tools import logger

def handlingPodioError(err):
    hour = getHour()
    message = ""
    if 'x-rate-limit-remaining' in err.status and err.status['x-rate-limit-remaining'] == '0':
        message = f"Quantidade de requisições chegou ao limite por hora."
        logger.warning(message)
        return "rate_limit"
    if err.status['status'] == '401':
        # Token expirado. Re-autenticando
        message = f"Token expirado. Renovando..."
        logger.warning(message)
        return "token_expired"
    if err.status['status'] == '400':
        if json.loads(err.content)['error_detail'] == 'oauth.client.invalid_secret':
            message = f"Secret inválido."
        elif json.loads(err.content)['error_detail'] == 'user.invalid.username':
            message = f"Usuário inválido."
        elif json.loads(err.content)['error_detail'] == 'oauth.client.invalid_id':
            message = f"ID do cliente inválido."
        elif json.loads(err.content)['error_detail'] == 'user.invalid.password':
            message = f"Senha do cliente inválido."
        else:
            message = f"Parâmetro nulo na query. {err}"
            logger.warning(message)
            return "null_query"
        return "status_400"
    if err.status['status'] == '504':
        message = f"Servidor demorou muito para responder. {err}"
        logger.warning(message)
        return "status_504"
    message = f"Erro inesperado no acesso a API. {err}"
    logger.warning(message)
    return "not_known_yet"


def getFieldValues(field):
    values = "'"
    # De acordo com o tipo do campo há uma determinada forma de recuperar esse dado
    if field['type'] == "contact":
        # Nesse caso o campo é multivalorado, então concatena-se com um pipe '|'
        # Podem haver aspas duplas inseridas no valor do campo. Substituir com aspas simples
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
        # Nesse caso o campo é multivalorado, então concatena-se com um pipe '|'
        for val in field['values']:
            values += val['value']['title'].replace("\'", "") + "|"
        values = values[:-1]
    else:
        values += str(field['values'][0]['value']).replace("\'", "")
    values += "'"

    return values
