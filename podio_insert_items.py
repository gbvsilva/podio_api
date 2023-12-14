"""Functions to insert items from Podio to the database."""
from collections import OrderedDict

import datetime

from pypodio2.client import Client
from pypodio2.transport import TransportException
from psycopg2 import Error as dbError

from get_time import get_hour
from get_mydb import get_db

from podio_tools import handling_podio_error, get_field_text_values

from logging_tools import logger


def insert_items(podio: Client, apps_ids: list):
    """Insert Podio items in the database.

    Args:
        podio (Client): Podio client
        apps_ids (list): List of Podio apps IDs

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
        try:
            app_info = podio.Application.find(app_id)
            space_name = podio.Space.find(app_info.get('space_id')).get('url_label').replace('-', '_')
            app_name = app_info.get('url_label').replace('-', '_')
            cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'podio_test' ORDER BY table_name;")
            tables = cursor.fetchall()
            table_name = space_name + "__" + app_name

            if (table_name,) in tables:
                # Data to fill in table
                table_data_model = OrderedDict()
                for field in app_info.get('fields'):
                    if field['status'] == 'active':
                        table_data_model[field['external_id'][:40]] = "''"

                # Making requests, iterating over all existing data.
                # For that, the limit of each query is set to 500 (the maximum) and the offset
                # That is, at each new step (offset) items are requested, based on
                # the number of items obtained in the last iteration.
                number_of_items = podio.Application.get_items(app_info.get('app_id'))['total']
                try:
                    for step in range(0, number_of_items, 500):
                        # The default value of the offset is 0 according to the API documentation.
                        # Sorting in ascending order of creation date to unify the DB structuring.
                        filter_response = podio.Item.filter(app_info.get('app_id'),
                                        {"offset": step, "sort_by": "created_on", "sort_desc": False, "limit": 500})
                        items = filter_response.get('items')
                        for item in items:
                            # New item being the copy of the model
                            new_item = table_data_model.copy()

                            last_event_on_podio = datetime.datetime.strptime(item['last_event_on'], "%Y-%m-%d %H:%M:%S")
                            result = cursor.execute(f"SELECT last_event_on FROM podio_test.{table_name} WHERE item_id='{item['item_id']}'")
                            if result:

                                last_event_on_db = result[0][0]

                                if last_event_on_podio > last_event_on_db:
                                    message = f"Item de ID={item['item_id']} e URL_ID={item['app_item_id']} atualizado no Podio. Excluindo-o da tabela '{table_name}' e inserindo-o a seguir."
                                    logger.info(message)
                                    cursor.execute(f"DELETE FROM podio_test.{table_name} WHERE item_id='{item['item_id']}'")

                            if not result or last_event_on_podio > last_event_on_db:
                                query = [f"INSERT INTO podio_test.{table_name}", " VALUES", "("]
                                query.extend([f"'{str(item['item_id'])}','{str(item['app_item_id'])}','{item['created_on']}','{last_event_on_podio}',"])

                                # Update new database item data with the item data from Podio
                                for field in item.get('fields'):
                                    # The item may still contain old information that is no longer used, hence the check.
                                    if field['external_id'][:40] in table_data_model:
                                        new_item.update({field['external_id'][:40]: get_field_text_values(field)})

                                query.extend(','.join(new_item.values()))
                                query.append(")")
                                try:
                                    message = f"Inserindo item de ID={item['item_id']} e URL_ID={item['app_item_id']} na tabela `{table_name}`"
                                    cursor.execute(''.join(query))
                                    logger.info(message)
                                    mydb.commit()
                                except dbError as err:
                                    message = f"Aplicativo alterado. Excluindo a tabela \"{table_name}\". {err}"
                                    logger.info(message)
                                    cursor.execute(f"DROP TABLE podio_test.{table_name}")
                                    raise dbError('Tabela excluída com sucesso!') from err

                except TransportException as err:
                    mydb.close()
                    handled = handling_podio_error(err)
                    if handled == 'rate_limit':
                        return 1
                    return 2

                except dbError as err:
                    continue

        except TransportException as err:
            mydb.close()
            handled = handling_podio_error(err)
            if handled == 'rate_limit':
                return 1
            return 2

    mydb.close()
    return 0
