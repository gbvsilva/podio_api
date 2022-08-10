from get_time import getHour

from psycopg2 import Error as dbError
from get_mydb import getDB


from pypodio2.transport import TransportException
from podio_tools import handlingPodioError


# Rotina para a criação inicial do banco de dados Postgres.
# Recebe a variável autenticada na API Podio.
def createTables(podio, apps_ids):
    # Acessando o BD
    mydb = getDB()
    cursor = mydb.cursor()
    for app_id in apps_ids:
        # Criando as tabelas para cada database criado acima
        try:
            appInfo = podio.Application.find(app_id)
            spaceName = podio.Space.find(appInfo.get('space_id')).get('url_label').replace('-', '_')
            appName = appInfo.get('url_label').replace('-', '_')
            cursor.execute("SELECT table_name FROM information_schema.tables "\
                "WHERE table_schema = 'podio' ORDER BY table_name;")
            tables = cursor.fetchall()
            tableName = spaceName+"__"+appName
            if appInfo.get('status') == "active" and (tableName,) not in tables:
                query = [f"CREATE TABLE IF NOT EXISTS podio.{tableName}", "("]
                query.append("\"id\" TEXT PRIMARY KEY NOT NULL")
                query.append(", \"created_on\" TIMESTAMP")

                for field in appInfo.get('fields'):
                    if field['status'] == "active":
                        label = field['external_id']
                        # Alguns campos possuem nomes muito grandes
                        label = label[:40]
                        if "id" in label:	
                            label += str("".join(query).lower().count(f"\"id")+1)
                        query.append(f", \"{label}\" TEXT")
                query.append(")")

                cursor.execute("".join(query))
                hour = getHour()
                message = f"{hour} -> {''.join(query)}"
                mydb.commit()
                print(message)
            # Caso tabela esteja inativa no Podio, excluí-la
            elif appInfo.get('status') != "active" and (tableName,) in tables:
                cursor.execute(f"DROP TABLE podio.{tableName}")
                hour = getHour()
                message = f"{hour} -> Tabela inativa `{tableName}` excluída."
                print(message)
        except dbError as err:
            hour = getHour()
            message = f"{hour} -> Erro no acesso ao BD. {err}"
            print(message)
        except TransportException as err:
            handled = handlingPodioError(err)
            if handled == 'token_expired':
                return 3
            if handled == 'status_400' or handled == 'not_known_yet':
                continue
        mydb.close()
        return 0
    # Não parando o fluxo main
    return 3
