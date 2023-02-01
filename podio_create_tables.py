from mysql.connector import Error as dbError
from get_mydb import getDB


from pypodio2.transport import TransportException
from podio_tools import handlingPodioError

from logging_tools import logger

# Rotina para a criação inicial do banco de dados Postgres.
# Recebe a variável autenticada na API Podio.
def createTables(podio, apps_ids):
    # Acessando o BD
    mydb = getDB()
    cursor = mydb.cursor()
    cursor.execute("CREATE DATABASE IF NOT EXISTS podio")
    cursor.execute("USE podio")
    for app_id in apps_ids:
        # Criando as tabelas para cada database criado acima
        try:
            appInfo = podio.Application.find(app_id)
            spaceName = podio.Space.find(appInfo.get('space_id')).get('url_label').replace('-', '_')
            appName = appInfo.get('url_label').replace('-', '_')
            cursor.execute("SHOW TABLES")
            tables = cursor.fetchall()
            tableName = spaceName+"__"+appName
            if appInfo.get('status') == "active" and (tableName,) not in tables:
                query = ["CREATE TABLE " + tableName, "("]
                query.append("`id` VARCHAR(255) PRIMARY KEY NOT NULL")
                query.append(", `created_on` DATETIME")
                query.append(", `last_event_on` DATETIME")

                for field in appInfo.get('fields'):
                    if field['status'] == "active":
                        label = field['external_id']
                        # Alguns campos possuem nomes muito grandes
                        label = label[:40]
                        if "id" in label:
                            label += str("".join(query).lower().count(f"`id")+1)
                        query.append(f", `{label}` TEXT")
                query.append(")")

                message = f"Criando tabela `{tableName}`"
                cursor.execute(''.join(query))
                #mydb.commit()
                logger.info(message)
            # Caso tabela esteja inativa no Podio, excluí-la
            elif appInfo.get('status') != "active" and (tableName,) in tables:
                cursor.execute(f"DROP TABLE {tableName}")
                message = f"Tabela inativa `{tableName}` excluída."
                logger.info(message)
        except dbError as err:
            message = f"Erro no acesso ao BD. {err}"
            logger.error(message)
        except TransportException as err:
            handled = handlingPodioError(err)
            if handled == 'token_expired':
                return 3
            if handled == 'status_400' or handled == 'not_known_yet':
                continue
    mydb.close()
    return 0
