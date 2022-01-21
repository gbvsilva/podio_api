from os import environ as env
# Usando a biblioteca de manipulação da API do Podio.
# Algumas alterações foram feitas para possibilitar a execução deste código
from pypodio2 import api

import time, datetime
import json

import psycopg2
from psycopg2 import sql
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

def handling_podio_error(err):	
    hour = datetime.datetime.now()
    message = ""
    if 'x-rate-limit-remaining' in err.status and err.status['x-rate-limit-remaining'] == '0':
        message = f"{hour.strftime('%H:%M:%S')} -> Quantidade de requisições chegou ao limite por hora."
        print(message)
        return "rate_limit"
    if err.status['status'] == '401':
        # Token expirado. Re-autenticando
        message = f"{hour.strftime('%H:%M:%S')} -> Token expirado. Renovando..."
        print(message)
        return "token_expired"
    if err.status['status'] == '400':
        if json.loads(err.content)['error_detail'] == 'oauth.client.invalid_secret':
            message = f"{hour.strftime('%H:%M:%S')} -> Secret inválido."
        elif json.loads(err.content)['error_detail'] == 'user.invalid.username':
            message = f"{hour.strftime('%H:%M:%S')} -> Usuário inválido."
        elif json.loads(err.content)['error_detail'] == 'oauth.client.invalid_id':
            message = f"{hour.strftime('%H:%M:%S')} -> ID do cliente inválido."
        elif json.loads(err.content)['error_detail'] == 'user.invalid.password':
            message = f"{hour.strftime('%H:%M:%S')} -> Senha do cliente inválido."
        else:
            message = f"{hour.strftime('%H:%M:%S')} -> Parâmetro nulo na query. {err}"
            print(message)
            return "null_query"
        return "status_400"
    if err.status['status'] == '504':
        message = f"{hour.strftime('%H:%M:%S')} -> Servidor demorou muito para responder. {err}"
        print(message)
        return "status_504"
    else:
        message = f"{hour.strftime('%H:%M:%S')} -> Erro inesperado no acesso a API. {err}"
    print(message)
    return "not_known_yet"

def get_all_workspaces(podio):
    # Obtendo informações de todas as organizações que o usuário tem acesso no Podio
    try:
        orgs = podio.Org.get_all()
        # Obtendo todas as workspaces que o usuário tem acesso
        hour = datetime.datetime.now()
        message = f"{hour.strftime('%H:%M:%S')} -> Sucesso na obtenção das orgs."
        print(message)
        return podio.Space.find_all_for_org(orgs[0]['org_id'])
    except api.transport.TransportException as err:
        return handling_podio_error(err)

# Rotina para a criação inicial do banco de dados MySQL.
# Recebe a variável autenticada na API Podio e o cursor do BD.
def create_tables(podio):
    workspaces = get_all_workspaces(podio)
    if workspaces == 'token_expired' or workspaces == 'null_query':
        return 3
    if workspaces == 'rate_limit':
        return 2
    if type(workspaces) is list:
        # Acessando o BD
        mydb = psycopg2.connect(host=env.get('POSTGRES_HOST'), user=env.get('POSTGRES_USERNAME'), password=env.get('POSTGRES_PASSWORD'), dbname=env.get('POSTGRES_DATABASE'), port=env.get('POSTGRES_PORT'))
        mydb.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = mydb.cursor()
        for w in workspaces:
            db_name = w.get('url_label').replace('-', '_')
            # Criando as tabelas para cada database criado acima
            try:
                apps = podio.Application.list_in_space(w.get('space_id'))
                #print(apps)
                cursor.execute(sql.SQL("SELECT table_name FROM information_schema.tables WHERE table_schema = 'podio' ORDER BY table_name;"))
                tables = cursor.fetchall()
                #print(db_name,tables)
                for app in apps:
                    #print(app)
                    table_name = app.get('url_label').replace('-', '_')
                    table_name = db_name+"__"+table_name
                    if app.get('status') == "active" and (table_name,) not in tables:
                        #print(table_name)
                        app_info = podio.Application.find(app.get('app_id'))
                        #print(app_info)
                        query = [f"CREATE TABLE IF NOT EXISTS podio.{table_name}", "("]
                        query.append("\"id\" INTEGER PRIMARY KEY NOT NULL")
                        query.append(", \"created_on_date\" DATE")
                        query.append(", \"created_on_time\" TIME")
                        #table_labels = []
                        for field in app_info.get('fields'):
                            if field['status'] == "active":
                                label = field['external_id']
                                # Alguns campos possuem nomes muito grandes
                                label = label[:40]
                                if "id" in label:	
                                    label += str("".join(query).lower().count(f"\"id")+1)
                                query.append(f", \"{label}\" TEXT")
                                #table_labels.append("\""+label+"\"")
                        query.append(")")

                        #print(table_name)
                        cursor.execute(sql.SQL("".join(query)))
                        hour = datetime.datetime.now()
                        message = f"{hour.strftime('%H:%M:%S')} -> {''.join(query)}"
                        mydb.commit()
                        print(message)
                    # Caso tabela esteja inativa no Podio, excluí-la
                    elif app.get('status') != "active" and (table_name,) in tables:
                        cursor.execute(sql.SQL(f"DROP TABLE podio.{table_name}"))
                        hour = datetime.datetime.now()
                        message = f"{hour.strftime('%H:%M:%S')} -> Tabela inativa `{table_name}` excluída."
                        print(message)
            except psycopg2.Error as err:
                hour = datetime.datetime.now()
                message = f"{hour.strftime('%H:%M:%S')} -> Erro no acesso ao BD. {err}"
                print(message)
            except api.transport.TransportException as err:
                handled = handling_podio_error(err)
                if handled == 'token_expired':
                    return 3
                if handled == 'status_400' or handled == 'not_known_yet':
                    continue
        mydb.close()
        return 0
    #return 1
    # Não parando o fluxo
    return 3

# Inserindo dados no Banco. Retorna 0 se nao ocorreram erros
# Retorna 1 caso precise refazer a estrutura do Banco, excluindo alguma(s) tabela(s).
# Retorna 2 caso seja atingido o limite de requisições por hora
def insert_items(podio):
    workspaces = get_all_workspaces(podio)
    if workspaces == 'token_expired' or workspaces == 'null_query':
        return 1
    if type(workspaces) is list:
        mydb = psycopg2.connect(host=env.get('POSTGRES_HOST'), user=env.get('POSTGRES_USERNAME'), password=env.get('POSTGRES_PASSWORD'), dbname=env.get('POSTGRES_DATABASE'))
        mydb.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = mydb.cursor()
        for w in workspaces:
            db_name = w.get('url_label').replace('-', '_')
            try:
                apps = podio.Application.list_in_space(w.get('space_id'))
                cursor.execute(sql.SQL("SELECT table_name FROM information_schema.tables WHERE table_schema = 'podio' ORDER BY table_name;"))
                tables = cursor.fetchall()
                for app in apps:
                    table_name = app.get('url_label').replace('-', '_')
                    table_name = db_name+"__"+table_name
                    #print(table_name)
                    if (table_name,) in tables:
                        app_info = podio.Application.find(app.get('app_id'))
                        cursor.execute(sql.SQL("SELECT COUNT(id) FROM podio."+table_name))
                        dbcount = cursor.fetchall()[0][0]
                        #print(dbcount)

                        table_labels = []
                        for field in app_info.get('fields'):
                            if field['status'] == "active":
                                label = field['external_id']
                                label = label[:40]
                                table_labels.append("\"" + label + "\"")

                        # Fazendo requisicoes percorrendo todos os dados existentes
                        # Para isso define-se o limite de cada consulta como 500 (o maximo) e o offset
                        # Ou seja, a cada passo novo (offset) items são requisitados, com base na
                        # quantidade de items obtidos na última iteração
                        number_of_items = podio.Application.get_items(app_info.get('app_id'))['total']
                        if dbcount < number_of_items:
                            hour = datetime.datetime.now()
                            message = f"{hour.strftime('%H:%M:%S')} -> {table_name} tem {dbcount} itens no BD e {number_of_items} no Podio."
                            print(message)
                            # Caso não seja possível inserir items em novas inspeções é necessário excluir a tabela
                            # recadastrando os dados no Banco
                            try:
                                for step in range(dbcount, number_of_items, 500):
                                    # O valor padrão do offset é 0 de acordo com a documentação da API.
                                    # Ordenando de forma crescente da data de criação para unificar a estruturação do BD.
                                    filtered_items = podio.Item.filter(app_info.get('app_id'), {"offset": step, "sort_by": "created_on", "sort_desc": False, "limit": 500})
                                    items = filtered_items.get('items')
                                    for item in items:
                                        query = [f"INSERT INTO podio.{table_name}", " VALUES", "("]
                                        query.extend([str(item['item_id']), ",", "\'" + str(item['created_on'].split()[0]) + "\'", \
                                                        ",\'" + str(item['created_on']).split()[1] + "\',"])
                                        fields = [x for x in item['fields'] if f"\"{x['external_id'][:40]}\"" in table_labels]
                                        # Fazendo a comparação entre os campos existentes e os preenchidos
                                        # Caso o campo esteja em branco no Podio, preencher com '?'
                                        j = 0
                                        for i in range(len(table_labels)):
                                            s = "\'"
                                            if j < len(fields) and str("\"" + fields[j]['external_id'][:40] + "\"") == table_labels[i]:
                                                # De acordo com o tipo do campo há uma determinada forma de recuperar esse dado
                                                if fields[j]['type'] == "contact":
                                                    # Nesse caso o campo é multivalorado, então concatena-se com um pipe '|'
                                                    # Podem haver aspas duplas inseridas no valor do campo. Substituir com aspas simples
                                                    for elem in fields[j]['values']:
                                                        s += elem['value']['name'].replace("\'", "") + "|"
                                                    s = s[:-1]
                                                elif fields[j]['type'] == "category":
                                                    s += fields[j]['values'][0]['value']['text'].replace("\'", "")
                                                elif fields[j]['type'] == "date" or (fields[j]['type'] == "calculation" and 'start' in \
                                                        fields[j]['values'][0]):
                                                    s += fields[j]['values'][0]['start']
                                                elif fields[j]['type'] == "money":
                                                    s += fields[j]['values'][0]['currency'] + " " + fields[j]['values'][0]['value']
                                                elif fields[j]['type'] == "image":
                                                    s += fields[j]['values'][0]['value']['link']
                                                elif fields[j]['type'] == "embed":
                                                    s += fields[j]['values'][0]['embed']['url']
                                                elif fields[j]['type'] == "app":
                                                    # Nesse caso o campo é multivalorado, então concatena-se com um pipe '|'
                                                    for val in fields[j]['values']:
                                                        s += val['value']['title'].replace("\'", "") + "|"
                                                    s = s[:-1]
                                                else:
                                                    value = str(fields[j]['values'][0]['value'])
                                                    s += value.replace("\'", "")
                                                s += "\'"
                                                j += 1
                                            else:
                                                s += "?\'"
                                            query.append(s)
                                            query.append(",")
                                        query.pop()
                                        query.append(")")
                                        try:
                                            cursor.execute(sql.SQL("".join(query)))
                                            hour = datetime.datetime.now()
                                            message = f"{hour.strftime('%H:%M:%S')} -> {''.join(query)}"
                                            print(message)
                                            mydb.commit()
                                        except psycopg2.Error as err:
                                            hour = datetime.datetime.now()
                                            message = f"{hour.strftime('%H:%M:%S')} -> Aplicativo alterado. Excluindo a tabela \"{table_name}\"."
                                            print(message)
                                            cursor.execute(sql.SQL(f"DROP TABLE podio.{table_name}"))
                                            return 1
                            except api.transport.TransportException as err:
                                handled = handling_podio_error(err)
                                if handled == 'status_504' or handled == 'null_query' or handled == 'status_400' or handled == 'token_expired':
                                    return 1
                                if handled == 'rate_limit':
                                    return 2
                        elif dbcount > number_of_items:
                            hour = datetime.datetime.now()
                            message = f"{hour.strftime('%H:%M:%S')} -> Itens excluídos do Podio. Excluindo a tabela \"{table_name}\" do BD e recriando-a."
                            print(message)
                            cursor.execute(sql.SQL(f"DROP TABLE podio.{table_name}"))
                            #return 1
                            continue

            except api.transport.TransportException as err:
                handled = handling_podio_error(err)	
                if handled == 'status_504' or handled == 'status_400' or handled == 'token_expired':
                    return 1
                if handled == 'rate_limit':
                    return 2
                return 1
        mydb.close()
        return 0
    return 1

if __name__ == '__main__':
    # Recuperando as variáveis de ambiente e guardando
    client_id = env.get('PODIO_CLIENT_ID')
    client_secret = env.get('PODIO_CLIENT_SECRET')
    username = env.get('PODIO_USERNAME')
    password = env.get('PODIO_PASSWORD')

    #print(client_id, client_secret, username, password)
    message = "==== PODIO API PYTHON SCRIPT ===="
    print(message)
    # Autenticando na plataforma do Podio com as credenciais recuperadas acima
    try:
        podio = api.OAuthClient(
            client_id,
            client_secret,
            username,
            password
        )
    # Caso haja erro, provavelmente o token de acesso a API expirou.
    except api.transport.TransportException as err:
        handled = handling_podio_error(err)
        if handled == 'status_400':
            print("Terminando o programa.")
        exit(1)
    else:
        #print(podio)
        #print(workspaces)
        cycle = 1
        while True:
            message = f"==== Ciclo {cycle} ===="
            print(message)
            res = create_tables(podio)
            if res == 0:
                result = insert_items(podio)
                # Caso o limite de requisições seja atingido, espera-se mais 1 hora até a seguinte iteração
                if result == 2:
                    hour = datetime.datetime.now() + datetime.timedelta(hours=1)
                    message = f"Esperando a hora seguinte. Até às {hour.strftime('%H:%M:%S')}"
                    print(message)
                    time.sleep(3600)
                    podio = api.OAuthClient(	
                        client_id,	
                        client_secret,	
                        username,	
                        password	
                    )
                elif result == 0:
                    # Nesse caso foi criado o primeiro snapshot do Podio no BD. Próxima iteração no dia seguinte
                    now = datetime.datetime.now()
                    hours = now + datetime.timedelta(hours=8)
                    message = f"Esperando as próximas 8hs. Até às {hours.strftime('%H:%M:%S')}"
                    print(message)
                    time.sleep(28800)
                    podio = api.OAuthClient(	
                        client_id,	
                        client_secret,	
                        username,	
                        password	
                    )
                else:
                    message = "Tentando novamente..."
                    print(message)
                    podio = api.OAuthClient(	
                        client_id,	
                        client_secret,	
                        username,	
                        password	
                    )
                    time.sleep(1)
            elif res == 2:
                hour = datetime.datetime.now() + datetime.timedelta(hours=1)
                message = f"Esperando a hora seguinte às {hour.strftime('%H:%M:%S')}"
                print(message)
                time.sleep(3600)
                podio = api.OAuthClient(	
                    client_id,	
                    client_secret,	
                    username,	
                    password	
                )
            elif res == 3:
                message = "Tentando novamente..."
                print(message)
                podio = api.OAuthClient(	
                    client_id,	
                    client_secret,	
                    username,	
                    password	
                )
                time.sleep(1)
            else:
                hour = datetime.datetime.now()
                message = f"{hour.strftime('%H:%M:%S')} -> Erro inesperado na criação/atualização do BD. Terminando o programa."
                print(message)
                exit(1)
            cycle += 1
