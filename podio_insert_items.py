from collections import OrderedDict

import datetime

from psycopg2 import Error as dbError
from get_mydb import getDB

from pypodio2.transport import TransportException
from podio_tools import handlingPodioError, getFieldValues

from logging_tools import logger

# Inserindo dados no Banco. Retorna 0 se nao ocorreram erros
# Retorna 1 caso precise refazer a estrutura do Banco, excluindo alguma(s) tabela(s).
# Retorna 2 caso seja atingido o limite de requisições por hora
def insertItems(podio, apps_ids):
    mydb = getDB()
    cursor = mydb.cursor()
    for app_id in apps_ids:
        try:
            appInfo = podio.Application.find(app_id)
            spaceName = podio.Space.find(appInfo.get('space_id')).get('url_label').replace('-', '_')
            appName = appInfo.get('url_label').replace('-', '_')
            cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'podio' ORDER BY table_name;")
            tables = cursor.fetchall()
            tableName = spaceName+"__"+appName

            if (tableName,) in tables:
                # Dados para preencher a tabela
                tableData = OrderedDict()
                for field in appInfo.get('fields'):
                    if field['status'] == 'active':
                        tableData[field['external_id'][:40]] = "''"

                # Fazendo requisicoes percorrendo todos os dados existentes
                # Para isso define-se o limite de cada consulta como 500 (o maximo) e o offset
                # Ou seja, a cada passo novo (offset) items são requisitados, com base na
                # quantidade de items obtidos na última iteração
                numberOfItems = podio.Application.get_items(appInfo.get('app_id'))['total']
                try:
                    for step in range(0, numberOfItems, 500):
                        # O valor padrão do offset é 0 de acordo com a documentação da API.
                        # Ordenando de forma crescente da data de criação para unificar a estruturação do BD.
                        filteredItems = podio.Item.filter(appInfo.get('app_id'),
                                        {"offset": step, "sort_by": "created_on", "sort_desc": False, "limit": 500})
                        items = filteredItems.get('items')
                        for item in items:
                             # Buscando a última atualização do Item no banco
                            cursor.execute(f"SELECT \"last_event_on\" FROM podio.{tableName} WHERE id='{item['item_id']}'")
                            last_event_on_podio = datetime.datetime.strptime(item['last_event_on'],
                                                    "%Y-%m-%d %H:%M:%S")
                            if cursor.rowcount > 0:
                                last_event_on_db = cursor.fetchone()[0]

                                if last_event_on_podio > last_event_on_db:
                                    message = f"Item com ID={item['item_id']} atualizado no Podio. Excluindo-o da tabela '{tableName}' e inserindo-o a seguir."
                                    logger.info(message)
                                    cursor.execute(f"DELETE FROM podio.{tableName} WHERE id='{item['item_id']}'")

                            if cursor.rowcount == 0 or last_event_on_podio > last_event_on_db:
                                query = [f"INSERT INTO podio.{tableName}", " VALUES", "("]
                                query.extend([f"'{str(item['item_id'])}','{item['created_on']}','{last_event_on_podio}',"])

                                # Atualizando os dados com o que é obtido do Podio
                                for field in item.get('fields'):
                                    # O item ainda pode trazer informações antigas não mais usadas. Daí a checagem.
                                    if field['external_id'][:40] in tableData:
                                        tableData.update({field['external_id'][:40]: getFieldValues(field)})

                                query.extend(','.join(tableData.values()))
                                query.append(")")
                                try:
                                    message = f"Inserindo item `{item['item_id']}` na tabela `{tableName}`"
                                    cursor.execute(''.join(query))
                                    logger.info(message)
                                    mydb.commit()
                                except dbError as err:
                                    message = f"Aplicativo alterado. Excluindo a tabela \"{tableName}\". {err}"
                                    logger.info(message)
                                    cursor.execute(f"DROP TABLE podio.{tableName}")
                                    return 1
                except TransportException as err:
                    handled = handlingPodioError(err)
                    if handled == 'status_504' or handled == 'null_query' or handled == 'status_400' or handled == 'token_expired':
                        return 1
                    if handled == 'rate_limit':
                        return 2

        except TransportException as err:
            handled = handlingPodioError(err)
            if handled == 'status_504' or handled == 'status_400' or handled == 'token_expired':
                return 1
            if handled == 'rate_limit':
                return 2
            return 1
    mydb.close()
    return 0
