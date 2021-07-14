# This is a sample Python script.

import pyodbc
import parametros as db
import query as q
from datetime import datetime
import mysql.connector
from mysql.connector import Error


def Consultar_as400(query):
    try:
        connection = pyodbc.connect(driver=db.driver, system=db.system, uid=db.uid, pwd=db.pwd)
        c1 = connection.cursor()
        c1.execute(query)
        row = c1.fetchall()
        valores = []
        for rows in row:
            valores.append(rows)
        insertar_mysql(valores)
        print("Se inserto:", valores)
        c1.close()
        connection.close()
    except pyodbc.Error as err:
        print(datetime.now())
        print("Error as400: ", err)
        print("Query: ", query)
        return None
    else:
        return None

def insertar_mysql(valores):
    try:
        conn = mysql.connector.connect(**db.mysql_args)
        if conn.is_connected():
            cur = conn.cursor()
            cur.execute(q.insert_horario_escenario1, valores)
            cur.close()
            conn.close()
    except mysql.connector.Error as err:
        print(datetime.now())
        print("Error mysql: ", err)
        return None
    else:
        return None

def main():
    Consultar_as400(q.query_horario)

if __name__ == '__main__':
    main()
