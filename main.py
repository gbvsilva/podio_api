from os import environ as env
# Usando a biblioteca de manipulação da API do Podio.
# Algumas alterações foram feitas para possibilitar a execução deste código
from pypodio2 import api

import time, datetime
import json

import psycopg2
from psycopg2 import sql
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

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
        hour = datetime.datetime.now()
        message = ""
        if err.status['status'] == '401':
            # Token expirado. Re-autenticando
            message = f"{hour.strftime('%H:%M:%S')} -> Token expirado. Renovando..."
            podio = api.OAuthClient(
                env.get('PODIO_CLIENT_ID'),
                env.get('PODIO_CLIENT_SECRET'),
                env.get('PODIO_USERNAME'),
                env.get('PODIO_PASSWORD')
            )
            return "token_expirado"
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
            message = f"{hour.strftime('%H:%M:%S')} -> Erro inesperado na obtenção das orgs. {err}"
        print(message)

# Rotina para a criação inicial do banco de dados MySQL.
# Recebe a variável autenticada na API Podio e o cursor do BD.
def create_tables(podio):
    workspaces = get_all_workspaces(podio)
    if workspaces == 'token_expirado':
        return 3
    if type(workspaces) is list:
        # Acessando o BD para armazenar os dados das workspaces nele
        # Verificando se as workspaces ja estão armazenadas no BD como databases. Se não, executar a criação
        mydb = psycopg2.connect(host="localhost", user="postgres", password=env.get('POSTGRES_PASSWORD'))
        mydb.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = mydb.cursor()
        cursor.execute(sql.SQL("SELECT * FROM pg_catalog.pg_database"))
        databases = cursor.fetchall()
        databases = [x[1] for x in databases]
        for w in workspaces:
            db_name = w.get('url_label').replace("-", "_")
            if db_name not in databases:
                try:
                    cursor.execute(sql.SQL(f"CREATE DATABASE \"{db_name}\""))
                    hour = datetime.datetime.now()
                    message = f"{hour.strftime('%H:%M:%S')} -> Banco \"{db_name}\" criado."
                    print(message)
                except psycopg2.Error as err:
                    hour = datetime.datetime.now()
                    message = f"{hour.strftime('%H:%M:%S')} -> Erro na criação do BD. {err}"
                    print(message)
                    return 1

            # Criando as tabelas para cada database criado acima
            cursor.execute(sql.SQL("SELECT * FROM pg_catalog.pg_database"))
            databases = cursor.fetchall()
            databases = [x[1] for x in databases]
            if db_name in databases:
                mydb = psycopg2.connect(host="localhost", user="postgres", password=env.get('POSTGRES_PASSWORD'), dbname=db_name)
                mydb.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
                cursor = mydb.cursor()
                try:
                    apps = podio.Application.list_in_space(w.get('space_id'))
                    # print(apps)
                    cursor.execute(sql.SQL("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' ORDER BY table_name;"))
                    tables = cursor.fetchall()
                    #print(db_name,tables)
                    for app in apps:
                        #print(app)
                        table_name = app.get('url_label').replace('-', '_')
                        if app.get('status') == "active" and (table_name,) not in tables:
                            #print(table_name)
                            app_info = podio.Application.find(app.get('app_id'))
                            # print(app_info)
                            query = ["CREATE TABLE " + table_name, "("]
                            query.append("\"id\" INTEGER PRIMARY KEY NOT NULL")
                            query.append(", \"created_on_date\" DATE")
                            query.append(", \"created_on_time\" TIME")
                            table_labels = []
                            for field in app_info.get('fields'):
                                if field['status'] == "active":
                                    label = field['label']
                                    # Alguns campos possuem nomes muito grandes
                                    label = label[:40].strip()
                                    if f"\"{label}\"".lower() in "".join(query).lower():
                                        label += str("".join(query).lower().count(f"\"{label}\"".lower())+1)
                                    query.append(f", \"{label}\" TEXT")
                                    table_labels.append("\""+label+"\"")
                            query.append(")")

                            #print(table_name)
                            cursor.execute(sql.SQL("".join(query)))
                            hour = datetime.datetime.now()
                            message = f"{hour.strftime('%H:%M:%S')} -> {''.join(query)}"
                            mydb.commit()
                            print(message)
                        # Caso tabela esteja inativa no Podio, excluí-la
                        elif app.get('status') != "active" and (table_name,) in tables:
                            cursor.execute(sql.SQL("DROP TABLE "+table_name))
                            hour = datetime.datetime.now()
                            message = f"{hour.strftime('%H:%M:%S')} -> Tabela inativa `{table_name}` excluída."
                            print(message)
                except psycopg2.Error as err:
                    hour = datetime.datetime.now()
                    message = f"{hour.strftime('%H:%M:%S')} -> Erro no acesso ao BD. {err}"
                    print(message)
                except api.transport.TransportException as err:
                    hour = datetime.datetime.now()
                    message = ""
                    if 'x-rate-limit-remaining' in err.status and err.status['x-rate-limit-remaining'] == '0':
                        message = f"{hour.strftime('%H:%M:%S')} -> Quantidade de requisições chegou ao limite por hora."
                        print(message)
                        return 2
                    if err.status['status'] == '401':
                        message = f"{hour.strftime('%H:%M:%S')} -> Token expirado. Renovando..."
                        podio = api.OAuthClient(
                            env.get('PODIO_CLIENT_ID'),
                            env.get('PODIO_CLIENT_SECRET'),
                            env.get('PODIO_USERNAME'),
                            env.get('PODIO_PASSWORD')
                        )
                        return 3
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
                        message = f"{hour.strftime('%H:%M:%S')} -> Erro inesperado na requisição para a API. {err}"
                    print(message)
                    return 1
        return 0
    return 1

# Inserindo dados no Banco. Retorna 0 se nao ocorreram erros
# Retorna 1 caso precise refazer a estrutura do Banco, excluindo alguma(s) tabela(s).
# Retorna 2 caso seja atingido o limite de requisições por hora
def insert_items(podio):
    workspaces = get_all_workspaces(podio)
    if workspaces == 'token_expirado':
        return 1
    if type(workspaces) is list:
        mydb = psycopg2.connect(host="localhost", user="postgres", password=env.get('POSTGRES_PASSWORD'))
        mydb.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = mydb.cursor()
        cursor.execute(sql.SQL("SELECT * FROM pg_catalog.pg_database"))
        databases = cursor.fetchall()
        databases = [x[1] for x in databases]
        for w in workspaces:
            db_name = w.get('url_label').replace("-", "_")
            if db_name in databases:
                #print(db_name)
                mydb = psycopg2.connect(host="localhost", user="postgres", password=env.get('POSTGRES_PASSWORD'), dbname=db_name)
                mydb.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
                cursor = mydb.cursor()
                try:
                    apps = podio.Application.list_in_space(w.get('space_id'))
                    cursor.execute(sql.SQL("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' ORDER BY table_name;"))
                    tables = cursor.fetchall()

                    for app in apps:
                        table_name = app.get('url_label').replace('-', '_')
                        #print(table_name)
                        if (table_name,) in tables:
                            app_info = podio.Application.find(app.get('app_id'))
                            cursor.execute(sql.SQL("SELECT COUNT(id) FROM "+table_name))
                            dbcount = cursor.fetchall()[0][0]
                            #print(dbcount)

                            table_labels = []
                            for field in app_info.get('fields'):
                                if field['status'] == "active":
                                    label = field['label']
                                    label = label[:40].strip()
                                    table_labels.append("\"" + label + "\"")

                            # Fazendo requisicoes percorrendo todos os dados existentes
                            # Para isso define-se o limite de cada consulta como 500 (o maximo) e o offset
                            # Ou seja, a cada passo novo (offset) items são requisitados, com base na
                            # quantidade de items obtidos na última iteração
                            number_of_items = podio.Application.get_items(app_info.get('app_id'))['total']
                            if dbcount < number_of_items:
                                hour = datetime.datetime.now()
                                message = f"{hour.strftime('%H:%M:%S')} -> {table_name} tem {str(dbcount)} itens no BD e {str(number_of_items)} no Podio."
                                print(message)
                                # Caso não seja possível inserir items em novas inspeções é necessário excluir a tabela
                                # recadastrando os dados no Banco
                                try:
                                    for step in range(dbcount, number_of_items, 500):
                                        # O valor padrão do offset é 0 de acordo com a documentação da API.
                                        # Ordenando de forma crescente da data de criação para unificar a estruturação do BD.
                                        items_filtered = podio.Item.filter(app_info.get('app_id'), {"offset": step, "sort_by": "created_on", "sort_desc": False, "limit": 500})
                                        items = items_filtered.get('items')
                                        for item in items:
                                            query = ["INSERT INTO " + table_name, " VALUES", "("]
                                            query.extend([str(item['item_id']), ",", "\'" + str(item['created_on'].split()[0]) + "\'", \
                                                          ",\'" + str(item['created_on']).split()[1] + "\',"])
                                            fields = item['fields']
                                            # Fazendo a comparação entre os campos existentes e os preenchidos
                                            # Caso o campo esteja em branco no Podio, preencher com '?'
                                            i = 0
                                            j = 0
                                            # print(table_labels)
                                            # print(fields)
                                            while i < len(table_labels):
                                                s = ""
                                                if j < len(fields) and str("\"" + fields[j]['label'][:40].strip() + "\"").lower() == table_labels[i].lower():
                                                    # print(str("`" + fields[j]['label'][:40] + "`").lower(), table_labels[i].lower())
                                                    # De acordo com o tipo do campo há uma determinada forma de recuperar esse dado
                                                    if fields[j]['type'] == "contact":
                                                        s += "\'"
                                                        # Nesse caso o campo é multivalorado, então concatena-se com um pipe '|'
                                                        # Podem haver aspas duplas inseridas no valor do campo. Substituir com aspas simples
                                                        for elem in fields[j]['values']:
                                                            s += elem['value']['name'].replace("\'", "") + "|"
                                                        s = s[:-1]
                                                    elif fields[j]['type'] == "category":
                                                        s += "\'" + fields[j]['values'][0]['value']['text'].replace("\'", "")
                                                    elif fields[j]['type'] == "date" or fields[j]['type'] == "calculation" and 'start' in \
                                                            fields[j]['values'][0]:
                                                        s += "\'" + fields[j]['values'][0]['start']
                                                    elif fields[j]['type'] == "money":
                                                        s += "\'" + fields[j]['values'][0]['currency'] + " " + fields[j]['values'][0]['value']
                                                    elif fields[j]['type'] == "image":
                                                        s += "\'" + fields[j]['values'][0]['value']['link']
                                                    elif fields[j]['type'] == "embed":
                                                        s += "\'" + fields[j]['values'][0]['embed']['url']
                                                    elif fields[j]['type'] == "app":
                                                        # Nesse caso o campo é multivalorado, então concatena-se com um pipe '|'
                                                        s += "\'"
                                                        for val in fields[j]['values']:
                                                            s += val['value']['title'].replace("\'", "") + "|"
                                                        s = s[:-1]
                                                    else:
                                                        value = str(fields[j]['values'][0]['value'])
                                                        if "\'" in value:
                                                            s += "\'" + value.replace("\'", "")
                                                        else:
                                                            s += "\'" + value
                                                    s += "\'"
                                                    j += 1
                                                else:
                                                    s += "\'?\'"
                                                i += 1
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
                                                cursor.execute(sql.SQL("DROP TABLE "+table_name))
                                                return 1
                                except api.transport.TransportException as err:
                                    hour = datetime.datetime.now()
                                    if err.status['status'] == '504':
                                        message = f"{hour.strftime('%H:%M:%S')} -> Servidor demorou muito para responder. {err}"
                                        print(message)
                                        return 1
                                    if err.status['status'] == '401':	
                                        message = f"{hour.strftime('%H:%M:%S')} -> Token expirado. Renovando..."	
                                        podio = api.OAuthClient(	
                                            env.get('PODIO_CLIENT_ID'),	
                                            env.get('PODIO_CLIENT_SECRET'),	
                                            env.get('PODIO_USERNAME'),	
                                            env.get('PODIO_PASSWORD')
                                        )	
                                        return 1
                                    if 'x-rate-limit-remaining' in err.status and err.status['x-rate-limit-remaining'] == '0':
                                        message = f"{hour.strftime('%H:%M:%S')} -> Quantidade de requisições chegou ao limite por hora."
                                        print(message)
                                        return 2
                            elif dbcount > number_of_items:
                                hour = datetime.datetime.now()
                                message = f"{hour.strftime('%H:%M:%S')} -> Itens excluídos do Podio. Excluindo a tabela \"{table_name}\" do BD e recriando-a."
                                print(message)
                                cursor.execute(sql.SQL("DROP TABLE " + table_name))
                                return 1

                except api.transport.TransportException as err:
                    hour = datetime.datetime.now()
                    if err.status['status'] == '504':
                        message = f"{hour.strftime('%H:%M:%S')} -> Servidor demorou muito para responder. {err}"
                        print(message)
                        return 1
                    if err.status['status'] == '401':
                        message = f"{hour.strftime('%H:%M:%S')} -> Token expirado. Renovando..."	
                        podio = api.OAuthClient(	
                            env.get('PODIO_CLIENT_ID'),	
                            env.get('PODIO_CLIENT_SECRET'),	
                            env.get('PODIO_USERNAME'),	
                            env.get('PODIO_PASSWORD')
                        )
                        return 1
                    if 'x-rate-limit-remaining' in err.status and err.status['x-rate-limit-remaining'] == '0':
                        message = f"{hour.strftime('%H:%M:%S')} -> Quantidade de requisições chegou ao limite por hora."
                        print(message)
                        return 2
                    message = f"{hour.strftime('%H:%M:%S')} -> Erro inesperado na requisição para a API. {err}"
                    print(message)
                    return 1
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
        hour = datetime.datetime.now()
        message = ""
        if err.status['status'] == '400':
            if json.loads(err.content)['error_detail'] == 'oauth.client.invalid_secret':
                message = f"{hour.strftime('%H:%M:%S')} -> Secret inválido. Terminando o programa."
            elif json.loads(err.content)['error_detail'] == 'user.invalid.username':
                message = f"{hour.strftime('%H:%M:%S')} -> Usuário inválido. Terminando o programa."
            elif json.loads(err.content)['error_detail'] == 'oauth.client.invalid_id':
                message = f"{hour.strftime('%H:%M:%S')} -> ID do cliente inválido. Terminando o programa."
            elif json.loads(err.content)['error_detail'] == 'user.invalid.password':
                message = f"{hour.strftime('%H:%M:%S')} -> Senha do cliente inválido. Terminando o programa."
        else:
            message = f"{hour.strftime('%H:%M:%S')} -> Terminando o programa. Erro no acesso a API. {err}"
        print(message)
        exit(1)
    else:
        #print(podio)
        #print(workspaces)
        cycle = 1
        while True:
            message = f"==== Ciclo {str(cycle)} ===="
            print(message)
            res = create_tables(podio)
            if res == 0:
                result = insert_items(podio)
                # Caso o limite de requisições seja atingido, espera-se mais 1 hora até a seguinte iteração
                if result == 2:
                    hour = datetime.datetime.now() + datetime.timedelta(hours=1)
                    message = f"Esperando a hora seguinte às {hour.strftime('%H:%M:%S')}"
                    print(message)
                    time.sleep(3600)
                elif result == 0:
                    # Nesse caso foi criado o primeiro snapshot do Podio no BD. Próxima iteração no dia seguinte
                    now = datetime.datetime.now()
                    hours = now + datetime.timedelta(hours=12)
                    message = f"Esperando as próximas 12hs às {hours.strftime('%H:%M:%S')}"
                    print(message)
                    time.sleep(43200)
                else:
                    message = "Tentando novamente..."
                    print(message)
                    time.sleep(1)
            elif res == 2:
                hour = datetime.datetime.now() + datetime.timedelta(hours=1)
                message = f"Esperando a hora seguinte às {hour.strftime('%H:%M:%S')}"
                print(message)
                time.sleep(3600)
            elif res == 3:	
                message = "Tentando novamente..."	
                print(message)	
                time.sleep(1)
            else:
                hour = datetime.datetime.now()
                message = f"{hour.strftime('%H:%M:%S')} -> Erro inesperado na criação/atualização do BD. Terminando o programa."
                print(message)
                exit(1)
            cycle += 1
