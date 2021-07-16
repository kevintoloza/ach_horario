# This is a sample Python script.

import pyodbc
import parametros as db
import query as q
from datetime import datetime
import mysql.connector
import time
import pytz

def local_time():
    tzone = pytz.timezone('America/El_Salvador')
    tz_dtime = tzone.localize(datetime.now())
    return tz_dtime

def Consultar_as400(query):
    try:
        conexion_as400 = pyodbc.connect(driver=db.driver, system=db.system, uid=db.uid, pwd=db.pwd)
        cursor = conexion_as400.cursor()
        cursor.execute(query)
        filas_obtenidas = cursor.fetchall()
        cursor.close()
        conexion_as400.close()
    except pyodbc.Error as err:
        print(datetime.now())
        print("Error as400: ", err)
        print("Query: ", query)
        return None
    else:
        return tuple(filas_obtenidas)

def insertar_mysql(valores):
    try:
        conexion_mysql = mysql.connector.connect(**db.mysql_args)
        if conexion_mysql.is_connected():
            cursor = conexion_mysql.cursor()
            cursor.executemany(q.insert_horario_escenario1, valores)
            cursor.close()
            conexion_mysql.close()
    except mysql.connector.Error as err:
        print(datetime.now())
        print("Error mysql: ", err)

        return None
    else:
        return None


def constructor_insert(datos):
    valores = []
    try:
        for val in datos:
            valores.append(val)
        return valores
    except Exception as error:
        print("Error sentencia sql;", error)


def main():

    valores = Consultar_as400(q.query_escenario1)
    recibido = constructor_insert(valores)
    insertar_mysql(recibido)

if __name__ == '__main__':
    main()

