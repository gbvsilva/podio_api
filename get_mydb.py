from os import environ as env
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

def getDB():
    mydb = psycopg2.connect(
                host=env.get('POSTGRES_HOST'), 
                user=env.get('POSTGRES_USERNAME'), 
                password=env.get('POSTGRES_PASSWORD'), 
                dbname=env.get('POSTGRES_DATABASE'), 
                port=env.get('POSTGRES_PORT'))
    mydb.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    return mydb
