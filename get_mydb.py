from os import environ as env
import mysql.connector

def getDB():
    mydb = mysql.connector.connect(
        host=env.get('MYSQL_HOST'),
        user=env.get('MYSQL_USER'),
        password=env.get('MYSQL_PASSWORD'),
        port=env.get('MYSQL_PORT')
    )
    return mydb
