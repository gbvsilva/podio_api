"""Functions to create tables in the database based on Podio apps."""
from psycopg2 import Error as dbError

from pypodio2.client import Client
from pypodio2.transport import TransportException

from get_time import get_hour
from get_mydb import get_db
from podio_tools import handling_podio_error

from logging_tools import logger

def create_tables(podio: Client, apps_ids: list):
    """Create tables in the database for each Podio app.

    Args:
        podio (_type_): _description_
        apps_ids (_type_): _description_

    Returns:
        int: Code to handle the main loop. `0` if no errors,
        `1` if the Podio API limit is reached.
        `2` encountered another error with Podio,
    """
    # Waiting for DB connection
    mydb = None
    while not mydb:
        mydb = get_db()

    cursor = mydb.cursor()
    for app_id in apps_ids:

        # Creating database tables for each Podio app
        try:
            app_info = podio.Application.find(app_id)
            space_name = podio.Space.find(app_info.get('space_id')).get('url_label').replace('-', '_')
            app_name = app_info.get('url_label').replace('-', '_')
            cursor.execute("SELECT table_name FROM information_schema.tables "\
                "WHERE table_schema = 'podio' ORDER BY table_name;")
            tables = cursor.fetchall()
            table_name = space_name + "__" + app_name
            if app_info.get('status') == "active" and (table_name,) not in tables:
                query = [f"CREATE TABLE IF NOT EXISTS podio.{table_name}", "("]
                query.append("\"id\" TEXT PRIMARY KEY NOT NULL")
                query.append(", \"created_on\" TIMESTAMP")
                query.append(", \"last_event_on\" TIMESTAMP")

                for field in app_info.get('fields'):
                    if field['status'] == "active":
                        label = field['external_id']
                        # Some field names are too large
                        label = label[:40]
                        query.append(f", \"{label}\" TEXT")
                query.append(")")

                message = f"Criando a tabela `{table_name}`"
                cursor.execute(''.join(query))
                mydb.commit()
                logger.info(message)

            # If the app is inactive on Podio, delete its respescive table
            elif app_info.get('status') != "active" and (table_name,) in tables:
                cursor.execute(f"DROP TABLE podio.{table_name}")
                message = f"Tabela inativa `{table_name}` excluída."
                logger.info(message)

        except dbError as err:
            message = f"Erro no acesso ao BD. {err}"
            logger.error(message)
        except TransportException as err:
            handled = handling_podio_error(err)
            if handled == 'token_expired':
                return 1
            if handled == 'rate_limit':
                return 2
            if handled == 'status_400' or handled == 'not_known_yet':
                continue
    mydb.close()
    return 0
